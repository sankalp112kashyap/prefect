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
    import docker
    client = docker.APIClient(base_url=base_url, version="auto")
    for log in client.logs(container=container_id, stream=True, follow=True):
        print(str(log, "utf-8").rstrip())


class DockerAgent(Agent):
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
        default_url = "npipe:////./pipe/docker_engine" if platform == "win32" else "unix://var/run/docker.sock"
        self.base_url = base_url or context.get("base_url", default_url)
        self.no_pull = no_pull or context.get("no_pull", False)
        self.named_volumes, self.container_mount_paths, self.host_spec = self._parse_volume_spec(volumes or [])
        self.network = network
        self.docker_interface = docker_interface
        self.failed_connections = 0
        self.docker_client = self._get_docker_client()
        self.show_flow_logs = show_flow_logs
        self.processes = []
        self.reg_allow_list = reg_allow_list

        try:
            self.docker_client.ping()
        except Exception as exc:
            self.logger.exception("Issue connecting to the Docker daemon. Make sure it is running.")
            raise exc

    def _get_docker_client(self) -> "docker.APIClient":
        import docker
        return docker.APIClient(base_url=self.base_url, version="auto")

    def heartbeat(self) -> None:
        try:
            if not self.docker_client.ping():
                raise RuntimeError("Unexpected Docker ping result")
            if self.failed_connections > 0:
                self.logger.info("Reconnected to Docker daemon")
            self.failed_connections = 0
        except Exception as exc:
            self.logger.warning("Failed heartbeat: {}".format(repr(exc)))
            self.failed_connections += 1

        if self.failed_connections >= 6:
            self.logger.error("Cannot reconnect to Docker daemon. Agent is shutting down.")
            raise SystemExit()

    def on_shutdown(self) -> None:
        for proc in self.processes:
            if proc.is_alive():
                proc.terminate()

    def _is_named_volume_unix(self, canditate_path: str) -> bool:
        return canditate_path and not canditate_path.startswith((".", "/", "~"))

    def _is_named_volume_win32(self, canditate_path: str) -> bool:
        return self._is_named_volume_unix(canditate_path) and not re.match(r"^[A-Za-z]\:\\.*", canditate_path) and not canditate_path.startswith("\\")

    def _parse_volume_spec(self, volume_specs: List[str]) -> Tuple[Iterable[str], Iterable[str], Dict[str, Dict[str, str]]]:
        return self._parse_volume_spec_win32(volume_specs) if platform == "win32" else self._parse_volume_spec_unix(volume_specs)

    def _parse_volume_spec_win32(self, volume_specs: List[str]) -> Tuple[Iterable[str], Iterable[str], Dict[str, Dict[str, str]]]:
        named_volumes, container_mount_paths, host_spec = [], [], {}
        for volume_spec in volume_specs:
            fields = volume_spec.split(":")
            mode = fields.pop() if fields[-1] in ("ro", "rw") else "rw"
            if len(fields) == 3 and len(fields[0]) == 1:
                external = ntpath.normpath(":".join(fields[0:2]))
                internal = posixpath.normpath(fields[2])
            elif len(fields) == 2:
                combined_path = ":".join(fields)
                (drive, path) = ntpath.splitdrive(combined_path)
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
                raise ValueError("Unable to parse volume specification '{}'".format(volume_spec))

            container_mount_paths.append(internal)
            if external and self._is_named_volume_win32(external):
                named_volumes.append(external)
                if mode != "rw":
                    raise ValueError("Named volumes can only have 'rw' mode, provided '{}'".format(mode))
            else:
                if not external:
                    external = internal
                host_spec[external] = {"bind": internal, "mode": mode}

        return named_volumes, container_mount_paths, host_spec

    def _parse_volume_spec_unix(self, volume_specs: List[str]) -> Tuple[Iterable[str], Iterable[str], Dict[str, Dict[str, str]]]:
        named_volumes, container_mount_paths, host_spec = [], [], {}
        for volume_spec in volume_specs:
            fields = volume_spec.split(":")
            if len(fields) > 3:
                raise ValueError(f"Docker volume format is invalid: {volume_spec} (should be 'external:internal[:mode]')")
            external = None if len(fields) == 1 else posixpath.normpath(fields[0].strip())
            internal = posixpath.normpath(fields[0].strip() if len(fields) == 1 else fields[1].strip())
            mode = "rw" if len(fields) < 3 else fields[2]
            container_mount_paths.append(internal)
            if external and self._is_named_volume_unix(external):
                named_volumes.append(external)
                if mode != "rw":
                    raise ValueError("Named volumes can only have 'rw' mode, provided '{}'".format(mode))
            else:
                if not external:
                    external = internal
                host_spec[external] = {"bind": internal, "mode": mode}
        return named_volumes, container_mount_paths, host_spec

    def deploy_flow(self, flow_run: GraphQLResult) -> str:
        self.logger.info("Deploying flow run {}".format(flow_run.id))
        import docker

        run_config = RunConfigSchema().load(flow_run.flow.run_config) if getattr(flow_run.flow, "run_config", None) else None
        if run_config and not isinstance(run_config, DockerRun):
            self.logger.error("Flow run %s has a `run_config` of type `%s`, only `DockerRun` is supported", flow_run.id, type(run_config).__name__)
            raise TypeError("Unsupported RunConfig type: %s" % type(run_config).__name__)

        image = get_flow_image(flow_run=flow_run)
        env_vars = self.populate_env_vars(flow_run, run_config=run_config)

        if not self.no_pull and len(image.split("/")) > 1:
            self.logger.info("Pulling image {}...".format(image))
            registry = image.split("/")[0]
            if self.reg_allow_list and registry not in self.reg_allow_list:
                self.logger.error("Trying to pull image from a Docker registry '{}' which is not in the reg_allow_list".format(registry))
                raise ValueError("Trying to pull image from a Docker registry '{}' which is not in the reg_allow_list".format(registry))
            else:
                pull_output = self.docker_client.pull(image, stream=True, decode=True)
                for line in pull_output:
                    self.logger.debug(line)
                self.logger.info("Successfully pulled image {}...".format(image))

        for named_volume_name in self.named_volumes:
            try:
                self.docker_client.inspect_volume(name=named_volume_name)
            except docker.errors.APIError:
                self.logger.debug("Creating named volume {}".format(named_volume_name))
                self.docker_client.create_volume(name=named_volume_name, driver="local", labels={"prefect_created": "true"})

        self.logger.debug("Creating Docker container {}".format(image))
        host_config = {"auto_remove": True}
        if self.container_mount_paths:
            host_config.update(binds=self.host_spec)
        if sys.platform.startswith("linux") and self.docker_interface:
            docker_internal_ip = get_docker_ip()
            host_config.update(extra_hosts={"host.docker.internal": docker_internal_ip})

        networking_config = self.docker_client.create_networking_config({self.network: self.docker_client.create_endpoint_config()}) if self.network else None

        container = self.docker_client.create_container(
            image,
            command=get_flow_run_command(flow_run),
            environment=env_vars,
            volumes=self.container_mount_paths,
            host_config=self.docker_client.create_host_config(**host_config),
            networking_config=networking_config,
        )

        self.logger.debug("Starting Docker container with ID {}".format(container.get("Id")))
        if self.network:
            self.logger.debug("Adding container to docker network: {}".format(self.network))
        self.docker_client.start(container=container.get("Id"))

        if self.show_flow_logs:
            self.stream_flow_logs(container.get("Id"))

        self.logger.debug("Docker container {} started".format(container.get("Id")))
        return "Container ID: {}".format(container.get("Id"))

    def stream_flow_logs(self, container_id: str) -> None:
        proc = multiprocessing.Process(target=_stream_container_logs, kwargs={"base_url": self.base_url, "container_id": container_id})
        proc.start()
        self.processes.append(proc)

    def populate_env_vars(self, flow_run: GraphQLResult, run_config: DockerRun = None) -> dict:
        api = "http://host.docker.internal:{}".format(config.server.port) if "localhost" in config.cloud.api else config.cloud.api
        env = {"PREFECT__LOGGING__LEVEL": config.logging.level} if run_config is None else {}
        env.update(self.env_vars)
        if run_config and run_config.env:
            env.update(run_config.env)
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