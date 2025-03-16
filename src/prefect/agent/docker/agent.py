import multiprocessing
import ntpath
import posixpath
import re
import sys
from sys import platform
from typing import TYPE_CHECKING, Dict, Iterable, List, Tuple

from prefect import config, context
from prefect.agent import Agent
from prefect.run_configs import DockerRun
from prefect.serialization.run_config import RunConfigSchema
from prefect.utilities.agent import get_flow_image, get_flow_run_command
from prefect.utilities.docker_util import get_docker_ip
from prefect.utilities.graphql import GraphQLResult

if TYPE_CHECKING:
    import docker

def _stream_container_logs(base_url: str, container_id: str) -> None:
    """
    Stream container logs back to stdout

    Args:
        - base_url (str): URL for a Docker daemon server
        - container_id (str): ID of a container to stream logs
    """
    import docker
    client = docker.APIClient(base_url=base_url, version="auto")
    for log in client.logs(container=container_id, stream=True, follow=True):
        print(str(log, "utf-8").rstrip())

class DockerAgent(Agent):
    """
    Agent which deploys flow runs locally as Docker containers. Information on using the
    Docker Agent can be found at https://docs.prefect.io/orchestration/agents/docker.html

    Environment variables may be set on the agent to be provided to each flow run's container:
    ```
    prefect agent docker start --env MY_SECRET_KEY=secret --env OTHER_VAR=$OTHER_VAR
    ```

    The default Docker daemon may be overridden by providing a different `base_url`:
    ```
    prefect agent docker start --base-url "tcp://0.0.0.0:2375"
    ```

    Args:
        - agent_config_id (str, optional): An optional agent configuration ID that can be used to set
            configuration based on an agent from a backend API. If set all configuration values will be
            pulled from backend agent configuration.
        - name (str, optional): An optional name to give this agent. Can also be set through
            the environment variable `PREFECT__CLOUD__AGENT__NAME`. Defaults to "agent"
        - labels (List[str], optional): a list of labels, which are arbitrary string
            identifiers used by Prefect Agents when polling for work
        - env_vars (dict, optional): a dictionary of environment variables and values that will
            be set on each flow run that this agent submits for execution
        - max_polls (int, optional): maximum number of times the agent will poll Prefect Cloud
            for flow runs; defaults to infinite
        - agent_address (str, optional):  Address to serve internal api at. Currently this is
            just health checks for use by an orchestration layer. Leave blank for no api server
            (default).
        - no_cloud_logs (bool, optional): Disable logging to a Prefect backend for this agent
            and all deployed flow runs
        - base_url (str, optional): URL for a Docker daemon server. Defaults to
            `unix:///var/run/docker.sock` however other hosts such as
            `tcp://0.0.0.0:2375` can be provided
        - no_pull (bool, optional): Flag on whether or not to pull flow images.
            Defaults to `False` if not provided here or in context.
        - show_flow_logs (bool, optional): a boolean specifying whether the agent should
            re-route Flow run logs to stdout; defaults to `False`
        - volumes (List[str], optional): a list of Docker volume mounts to be attached to any
            and all created containers.
        - network (str, optional): Add containers to an existing docker network
        - docker_interface (bool, optional): Toggle whether or not a `docker0` interface is
            present on this machine.  Defaults to `True`. **Note**: This is mostly relevant for
            some Docker-in-Docker setups that users may be running their agent with.
        - reg_allow_list (List[str], optional): Limits Docker Agent to only pull images
            from the listed registries.
    """
    def __init__(
        self,
        agent_config_id: str = None,
        name: str = None,
        labels: Iterable[str] = None,
        env_vars: dict = None,
        max_polls: int = None,
        agent_address: str = None,
        no_cloud_logs: bool = False,
        base_url: str = None,
        no_pull: bool = None,
        volumes: List[str] = None,
        show_flow_logs: bool = False,
        network: str = None,
        docker_interface: bool = True,
        reg_allow_list: List[str] = None,
    ) -> None:
        super().__init__(
            agent_config_id=agent_config_id,
            name=name,
            labels=labels,
            env_vars=env_vars,
            max_polls=max_polls,
            agent_address=agent_address,
            no_cloud_logs=no_cloud_logs,
        )
        
        self.base_url = base_url or context.get("base_url", "npipe:////./pipe/docker_engine" if platform == "win32" else "unix://var/run/docker.sock")
        self.no_pull = no_pull or context.get("no_pull", False)
        self.network = network
        self.docker_interface = docker_interface
        self.show_flow_logs = show_flow_logs
        self.reg_allow_list = reg_allow_list
        self.processes = []
        
        # Parse volumes
        self.named_volumes, self.container_mount_paths, self.host_spec = self._parse_volume_spec(volumes or [])
        
        # Initialize Docker client and test connection
        self.failed_connections = 0
        self.docker_client = self._get_docker_client()
        self._test_docker_connection()

    def _get_docker_client(self) -> "docker.APIClient":
        import docker
        return docker.APIClient(base_url=self.base_url, version="auto")

    def _test_docker_connection(self) -> None:
        try:
            self.docker_client.ping()
        except Exception as exc:
            self.logger.exception("Issue connecting to the Docker daemon. Make sure it is running.")
            raise exc

    def _is_named_volume_unix(self, canditate_path: str) -> bool:
        if not canditate_path:
            return False
        return not canditate_path.startswith((".", "/", "~"))

    def _is_named_volume_win32(self, canditate_path: str) -> bool:
        result = self._is_named_volume_unix(canditate_path)
        return (
            result
            and not re.match(r"^[A-Za-z]\:\\.*", canditate_path)
            and not canditate_path.startswith("\\")
        )

    def _is_named_volume(self, path: str) -> bool:
        if platform == "win32":
            return self._is_named_volume_win32(path)
        return self._is_named_volume_unix(path)

    def _parse_volume_spec(self, volume_specs: List[str]) -> Tuple[Iterable[str], Iterable[str], Dict[str, Dict[str, str]]]:
        if platform == "win32":
            return self._parse_volume_spec_win32(volume_specs)
        return self._parse_volume_spec_unix(volume_specs)

    def _parse_volume_spec_unix(self, volume_specs: List[str]) -> Tuple[Iterable[str], Iterable[str], Dict[str, Dict[str, str]]]:
        named_volumes = []
        container_mount_paths = []
        host_spec = {}

        for volume_spec in volume_specs:
            fields = volume_spec.split(":")

            if len(fields) > 3:
                raise ValueError(f"Docker volume format is invalid: {volume_spec} (should be 'external:internal[:mode]')")

            if len(fields) == 1:
                external = None
                internal = posixpath.normpath(fields[0].strip())
            else:
                external = posixpath.normpath(fields[0].strip())
                internal = posixpath.normpath(fields[1].strip())

            mode = "rw"
            if len(fields) == 3:
                mode = fields[2]

            container_mount_paths.append(internal)

            if external and self._is_named_volume_unix(external):
                named_volumes.append(external)
                if mode != "rw":
                    raise ValueError(f"Named volumes can only have 'rw' mode, provided '{mode}'")
            else:
                host_spec[external or internal] = {
                    "bind": internal,
                    "mode": mode,
                }

        return named_volumes, container_mount_paths, host_spec

    def _parse_volume_spec_win32(self, volume_specs: List[str]) -> Tuple[Iterable[str], Iterable[str], Dict[str, Dict[str, str]]]:
        named_volumes = []
        container_mount_paths = []
        host_spec = {}

        for volume_spec in volume_specs:
            fields = volume_spec.split(":")

            if fields[-1] in ("ro", "rw"):
                mode = fields.pop()
            else:
                mode = "rw"

            if len(fields) == 3 and len(fields[0]) == 1:
                external = ntpath.normpath(":".join(fields[0:2]))
                internal = posixpath.normpath(fields[2])
            elif len(fields) == 2:
                combined_path = ":".join(fields)
                drive, path = ntpath.splitdrive(combined_path)
                if drive:
                    external = ntpath.normpath(combined_path)
                    path = str("/" + drive.lower().rstrip(":") + path).replace("\\", "/")
                    internal = posixpath.normpath(path)
                else:
                    external = ntpath.normpath(fields[0])
                    internal = posixpath.normpath(fields[1])
            elif len(fields) == 1:
                external = ntpath.normpath(fields[0])
                internal = external
            else:
                raise ValueError(f"Unable to parse volume specification '{volume_spec}'")

            container_mount_paths.append(internal)

            if external and self._is_named_volume_win32(external):
                named_volumes.append(external)
                if mode != "rw":
                    raise ValueError(f"Named volumes can only have 'rw' mode, provided '{mode}'")
            else:
                host_spec[external] = {
                    "bind": internal,
                    "mode": mode,
                }

        return named_volumes, container_mount_paths, host_spec

    def _normalize_path(self, path: str) -> str:
        return ntpath.normpath(path) if platform == "win32" else posixpath.normpath(path)

    def heartbeat(self) -> None:
        try:
            if not self.docker_client.ping():
                raise RuntimeError("Unexpected Docker ping result")
            if self.failed_connections > 0:
                self.logger.info("Reconnected to Docker daemon")
            self.failed_connections = 0
        except Exception as exc:
            self.logger.warning(f"Failed heartbeat: {repr(exc)}")
            self.failed_connections += 1

        if self.failed_connections >= 6:
            self.logger.error("Cannot reconnect to Docker daemon. Agent is shutting down.")
            raise SystemExit()

    def on_shutdown(self) -> None:
        for proc in self.processes:
            if proc.is_alive():
                proc.terminate()

    def deploy_flow(self, flow_run: GraphQLResult) -> str:
        self.logger.info(f"Deploying flow run {flow_run.id}")
        import docker

        # Validate run config
        if getattr(flow_run.flow, "run_config", None) is not None:
            run_config = RunConfigSchema().load(flow_run.flow.run_config)
            if not isinstance(run_config, DockerRun):
                raise TypeError(f"Unsupported RunConfig type: {type(run_config).__name__}")
        else:
            run_config = None

        image = get_flow_image(flow_run=flow_run)
        
        # Handle image pulling
        if not self.no_pull and "/" in image:
            registry = image.split("/")[0]
            if self.reg_allow_list and registry not in self.reg_allow_list:
                self.logger.error(
                    "Trying to pull image from a Docker registry '{}' which"
                    " is not in the reg_allow_list".format(registry)
                )
                raise ValueError(
                    "Trying to pull image from a Docker registry '{}' which"
                    " is not in the reg_allow_list".format(registry)
                )
            self._pull_image(image)

        # Create named volumes
        self._ensure_named_volumes()

        # Configure and create container
        container = self._create_container(image, flow_run, run_config)
        
        # Start container
        container_id = container.get("Id")
        self.docker_client.start(container=container_id)

        if self.show_flow_logs:
            self.stream_flow_logs(container_id)

        return f"Container ID: {container_id}"

    def _pull_image(self, image: str) -> None:
        self.logger.info(f"Pulling image {image}...")
        pull_output = self.docker_client.pull(image, stream=True, decode=True)
        for line in pull_output:
            self.logger.debug(line)
        self.logger.info(f"Successfully pulled image {image}")

    def _ensure_named_volumes(self) -> None:
        import docker
        for volume in self.named_volumes:
            try:
                self.docker_client.inspect_volume(name=volume)
            except docker.errors.APIError:
                self.logger.debug(f"Creating named volume {volume}")
                self.docker_client.create_volume(
                    name=volume,
                    driver="local",
                    labels={"prefect_created": "true"}
                )

    def _create_container(self, image: str, flow_run: GraphQLResult, run_config: DockerRun = None) -> dict:
        host_config = {"auto_remove": True}
        
        if self.container_mount_paths:
            host_config.update(binds=self.host_spec)

        if sys.platform.startswith("linux") and self.docker_interface:
            host_config.update(extra_hosts={"host.docker.internal": get_docker_ip()})

        networking_config = None
        if self.network:
            networking_config = self.docker_client.create_networking_config(
                {self.network: self.docker_client.create_endpoint_config()}
            )

        return self.docker_client.create_container(
            image,
            command=get_flow_run_command(flow_run),
            environment=self.populate_env_vars(flow_run, run_config),
            volumes=self.container_mount_paths,
            host_config=self.docker_client.create_host_config(**host_config),
            networking_config=networking_config,
        )

    def stream_flow_logs(self, container_id: str) -> None:
        proc = multiprocessing.Process(
            target=_stream_container_logs,
            kwargs={"base_url": self.base_url, "container_id": container_id},
        )
        proc.start()
        self.processes.append(proc)

    def populate_env_vars(self, flow_run: GraphQLResult, run_config: DockerRun = None) -> dict:
        api = "http://host.docker.internal:{}".format(config.server.port) if "localhost" in config.cloud.api else config.cloud.api
        
        env = {}
        if run_config is None:
            env["PREFECT__LOGGING__LEVEL"] = config.logging.level

        env.update(self.env_vars)

        if run_config is not None and run_config.env is not None:
            env.update(run_config.env)

        # Required environment variables
        env.update({
            "PREFECT__CLOUD__API": api,
            "PREFECT__CLOUD__AUTH_TOKEN": config.cloud.agent.auth_token,
            "PREFECT__CLOUD__AGENT__LABELS": str(self.labels),
            "PREFECT__CONTEXT__FLOW_RUN_ID": flow_run.id,
            "PREFECT__CONTEXT__FLOW_ID": flow_run.flow.id,
            "PREFECT__CLOUD__USE_LOCAL_SECRETS": "false",
            "PREFECT__LOGGING__LOG_TO_CLOUD": str(self.log_to_cloud).lower(),
            "PREFECT__ENGINE__FLOW_RUNNER__DEFAULT_CLASS": "prefect.engine.cloud.CloudFlowRunner",
            "PREFECT__ENGINE__TASK_RUNNER__DEFAULT_CLASS": "prefect.engine.cloud.CloudTaskRunner",
        })
        return env

if __name__ == "__main__":
    DockerAgent().start()