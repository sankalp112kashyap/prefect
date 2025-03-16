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
    Agent which deploys flow runs locally as Docker containers.
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

        self.base_url = base_url or context.get("base_url", self._get_default_docker_url())
        self.no_pull = no_pull or context.get("no_pull", False)
        self.network = network
        self.docker_interface = docker_interface
        self.show_flow_logs = show_flow_logs
        self.reg_allow_list = reg_allow_list
        self.processes = []
        self.failed_connections = 0

        self.named_volumes, self.container_mount_paths, self.host_spec = self._parse_volume_spec(volumes or [])
        self.docker_client = self._get_docker_client()

        self._ping_docker_daemon()

    def _get_default_docker_url(self) -> str:
        """Get the default Docker URL based on the platform."""
        return "npipe:////./pipe/docker_engine" if platform == "win32" else "unix://var/run/docker.sock"

    def _get_docker_client(self) -> "docker.APIClient":
        """Initialize and return a Docker client."""
        import docker
        return docker.APIClient(base_url=self.base_url, version="auto")

    def _ping_docker_daemon(self) -> None:
        """Ping the Docker daemon to ensure connectivity."""
        try:
            self.docker_client.ping()
        except Exception as exc:
            self.logger.exception("Issue connecting to the Docker daemon. Make sure it is running.")
            raise exc

    def heartbeat(self) -> None:
        """Check the Docker daemon connection."""
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
        """Cleanup child processes created for streaming logs."""
        for proc in self.processes:
            if proc.is_alive():
                proc.terminate()

    def _is_named_volume(self, candidate_path: str) -> bool:
        """Check if a path is a named volume."""
        if not candidate_path:
            return False
        if platform == "win32":
            return not (candidate_path.startswith((".", "/", "~")) and not re.match(r"^[A-Za-z]\:\\.*", candidate_path)
        return not candidate_path.startswith((".", "/", "~"))

    def _parse_volume_spec(self, volume_specs: List[str]) -> Tuple[List[str], List[str], Dict[str, Dict[str, str]]]:
        """Parse Docker volume specifications."""
        named_volumes = []
        container_mount_paths = []
        host_spec = {}

        for volume_spec in volume_specs:
            fields = volume_spec.split(":")
            mode = fields.pop() if fields[-1] in ("ro", "rw") else "rw"

            if platform == "win32":
                external, internal = self._parse_win32_volume(fields)
            else:
                external, internal = self._parse_unix_volume(fields)

            container_mount_paths.append(internal)

            if external and self._is_named_volume(external):
                named_volumes.append(external)
                if mode != "rw":
                    raise ValueError("Named volumes can only have 'rw' mode.")
            else:
                host_spec[external or internal] = {"bind": internal, "mode": mode}

        return named_volumes, container_mount_paths, host_spec

    def _parse_win32_volume(self, fields: List[str]) -> Tuple[str, str]:
        """Parse Windows-specific volume paths."""
        if len(fields) == 3 and len(fields[0]) == 1:
            external = ntpath.normpath(":".join(fields[0:2]))
            internal = posixpath.normpath(fields[2])
        elif len(fields) == 2:
            combined_path = ":".join(fields)
            drive, path = ntpath.splitdrive(combined_path)
            if drive:
                external = ntpath.normpath(combined_path)
                internal = posixpath.normpath(f"/{drive.lower().rstrip(':')}{path.replace('\\', '/')}")
            else:
                external = ntpath.normpath(fields[0])
                internal = posixpath.normpath(fields[1])
        elif len(fields) == 1:
            external = ntpath.normpath(fields[0])
            internal = external
        else:
            raise ValueError(f"Unable to parse volume specification: {':'.join(fields)}")
        return external, internal

    def _parse_unix_volume(self, fields: List[str]) -> Tuple[str, str]:
        """Parse Unix-specific volume paths."""
        if len(fields) > 3:
            raise ValueError(f"Docker volume format is invalid: {':'.join(fields)}")
        external = posixpath.normpath(fields[0].strip()) if len(fields) > 1 else None
        internal = posixpath.normpath(fields[1].strip()) if len(fields) > 1 else posixpath.normpath(fields[0].strip())
        return external, internal

    def deploy_flow(self, flow_run: GraphQLResult) -> str:
        """Deploy flow runs as Docker containers."""
        self.logger.info(f"Deploying flow run {flow_run.id}")

        import docker

        run_config = None
        if getattr(flow_run.flow, "run_config", None) is not None:
            run_config = RunConfigSchema().load(flow_run.flow.run_config)
            if not isinstance(run_config, DockerRun):
                self.logger.error(f"Flow run {flow_run.id} has an unsupported RunConfig type: {type(run_config).__name__}")
                raise TypeError(f"Unsupported RunConfig type: {type(run_config).__name__}")

        image = get_flow_image(flow_run=flow_run)
        env_vars = self.populate_env_vars(flow_run, run_config)

        if not self.no_pull and len(image.split("/")) > 1:
            self._pull_image(image)

        self._create_named_volumes()

        container = self._create_container(image, env_vars, flow_run)
        self.docker_client.start(container=container.get("Id"))

        if self.show_flow_logs:
            self.stream_flow_logs(container.get("Id"))

        return f"Container ID: {container.get('Id')}"

    def _pull_image(self, image: str) -> None:
        """Pull a Docker image if allowed."""
        registry = image.split("/")[0]
        if self.reg_allow_list and registry not in self.reg_allow_list:
            self.logger.error(f"Registry {registry} is not in the reg_allow_list")
            raise ValueError(f"Registry {registry} is not in the reg_allow_list")
        self.logger.info(f"Pulling image {image}...")
        for line in self.docker_client.pull(image, stream=True, decode=True):
            self.logger.debug(line)
        self.logger.info(f"Successfully pulled image {image}")

    def _create_named_volumes(self) -> None:
        """Create named volumes if they do not exist."""
        import docker
        for named_volume_name in self.named_volumes:
            try:
                self.docker_client.inspect_volume(name=named_volume_name)
            except docker.errors.APIError:
                self.logger.debug(f"Creating named volume {named_volume_name}")
                self.docker_client.create_volume(name=named_volume_name, driver="local", labels={"prefect_created": "true"})

    def _create_container(self, image: str, env_vars: dict, flow_run: GraphQLResult) -> dict:
        """Create and return a Docker container."""
        host_config = {"auto_remove": True}
        if self.container_mount_paths:
            host_config.update(binds=self.host_spec)

        if sys.platform.startswith("linux") and self.docker_interface:
            host_config.update(extra_hosts={"host.docker.internal": get_docker_ip()})

        networking_config = None
        if self.network:
            networking_config = self.docker_client.create_networking_config({self.network: self.docker_client.create_endpoint_config()})

        return self.docker_client.create_container(
            image,
            command=get_flow_run_command(flow_run),
            environment=env_vars,
            volumes=self.container_mount_paths,
            host_config=self.docker_client.create_host_config(**host_config),
            networking_config=networking_config,
        )

    def stream_flow_logs(self, container_id: str) -> None:
        """Stream container logs back to stdout."""
        proc = multiprocessing.Process(
            target=_stream_container_logs,
            kwargs={"base_url": self.base_url, "container_id": container_id},
        )
        proc.start()
        self.processes.append(proc)

    def populate_env_vars(self, flow_run: GraphQLResult, run_config: DockerRun = None) -> dict:
        """Populate environment variables for a flow run."""
        api = f"http://host.docker.internal:{config.server.port}" if "localhost" in config.cloud.api else config.cloud.api

        env = {}
        if run_config is None:
            env.update({"PREFECT__LOGGING__LEVEL": config.logging.level})
        env.update(self.env_vars)
        if run_config is not None and run_config.env is not None:
            env.update(run_config.env)
        env.update(
            {
                "PREFECT__CLOUD__API": api,
                "PREFECT__CLOUD__AUTH_TOKEN": config.cloud.agent.auth_token,
                "PREFECT__CLOUD__AGENT__LABELS": str(self.labels),
                "PREFECT__CONTEXT__FLOW_RUN_ID": flow_run.id,
                "PREFECT__CONTEXT__FLOW_ID": flow_run.flow.id,
                "PREFECT__CLOUD__USE_LOCAL_SECRETS": "false",
                "PREFECT__LOGGING__LOG_TO_CLOUD": str(self.log_to_cloud).lower(),
                "PREFECT__ENGINE__FLOW_RUNNER__DEFAULT_CLASS": "prefect.engine.cloud.CloudFlowRunner",
                "PREFECT__ENGINE__TASK_RUNNER__DEFAULT_CLASS": "prefect.engine.cloud.CloudTaskRunner",
            }
        )
        return env


if __name__ == "__main__":
    DockerAgent().start()