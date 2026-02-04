"""
Run Orchestrator Service

Manages the complete lifecycle of test runs including:
- Docker network creation
- Container orchestration
- Proxy configuration
- Agent execution
- Cleanup and resource management
"""

import asyncio
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import docker
from docker.errors import APIError, NotFound
from docker.models.containers import Container
from docker.models.networks import Network
from structlog import get_logger

from chaostrace.control_plane.config import Settings, get_settings
from chaostrace.control_plane.models.run import (
    RunRequest,
    RunResponse,
    RunState,
    RunStatus,
    Verdict,
)

logger = get_logger(__name__)


class OrchestratorError(Exception):
    """Base exception for orchestrator errors."""
    pass


class ContainerStartError(OrchestratorError):
    """Error starting a container."""
    pass


class NetworkError(OrchestratorError):
    """Error with Docker networking."""
    pass


class RunOrchestrator:
    """
    Orchestrates the complete lifecycle of ChaosTrace test runs.
    
    The orchestrator manages:
    1. Docker network creation for isolation
    2. PostgreSQL container with seed data
    3. DB Proxy container for SQL interception
    4. Agent runner container
    5. Cleanup on completion/failure/termination
    
    Each run gets a completely isolated environment with:
    - Dedicated Docker network (no internet by default)
    - Fresh PostgreSQL instance with fake data
    - Unique run_id for tracking
    
    Usage:
        orchestrator = RunOrchestrator()
        run_id = await orchestrator.create_run(request)
        status = await orchestrator.get_run_status(run_id)
        await orchestrator.terminate_run(run_id)
    """
    
    def __init__(self, settings: Settings | None = None):
        """
        Initialize the orchestrator.
        
        Args:
            settings: Application settings. If None, uses global settings.
        """
        self.settings = settings or get_settings()
        self._docker_client: docker.DockerClient | None = None
        self._runs: dict[UUID, RunState] = {}
        self._run_tasks: dict[UUID, asyncio.Task] = {}
        
        logger.info(
            "orchestrator_initialized",
            docker_socket=self.settings.docker_socket,
        )
    
    @property
    def docker(self) -> docker.DockerClient:
        """Get or create Docker client."""
        if self._docker_client is None:
            self._docker_client = docker.DockerClient(
                base_url=self.settings.docker_socket
            )
        return self._docker_client
    
    async def create_run(self, request: RunRequest) -> RunResponse:
        """
        Create and start a new test run.
        
        This method:
        1. Validates the request
        2. Creates a run state
        3. Schedules the run execution
        4. Returns immediately with run_id
        
        The actual container setup happens asynchronously.
        
        Args:
            request: The run request with agent and scenario config.
            
        Returns:
            RunResponse: Initial response with run_id and PENDING status.
        """
        run_id = uuid4()
        
        # Create run state
        state = RunState(
            run_id=run_id,
            request=request,
            status=RunStatus.PENDING,
            created_at=datetime.utcnow(),
        )
        self._runs[run_id] = state
        
        # Start the run execution in the background
        task = asyncio.create_task(self._execute_run(run_id))
        self._run_tasks[run_id] = task
        
        logger.info(
            "run_created",
            run_id=str(run_id),
            agent_type=request.agent_type.value,
            scenario=request.scenario,
            policy=request.policy_profile,
        )
        
        return RunResponse(
            run_id=run_id,
            status=RunStatus.PENDING,
            created_at=state.created_at,
            message="Run created and queued for execution",
        )
    
    async def _execute_run(self, run_id: UUID) -> None:
        """
        Execute a run through its complete lifecycle.
        
        This is the main run loop that:
        1. Sets up the isolated environment
        2. Starts the agent
        3. Monitors for completion/timeout
        4. Cleans up resources
        """
        state = self._runs[run_id]
        
        try:
            # Phase 1: Initialize containers
            await self._initialize_run(run_id)
            
            # Phase 2: Wait for agent completion
            await self._monitor_run(run_id)
            
            # Phase 3: Analyze results
            await self._finalize_run(run_id)
            
        except Exception as e:
            logger.exception(
                "run_execution_failed",
                run_id=str(run_id),
                error=str(e),
            )
            state.status = RunStatus.FAILED
            state.error_message = str(e)
            state.ended_at = datetime.utcnow()
        
        finally:
            # Always clean up
            if self.settings.docker_cleanup_on_exit:
                await self._cleanup_run(run_id)
    
    async def _initialize_run(self, run_id: UUID) -> None:
        """Initialize all containers for the run."""
        state = self._runs[run_id]
        state.status = RunStatus.INITIALIZING
        
        logger.info("initializing_run", run_id=str(run_id))
        
        # Create isolated network
        network = await self._create_network(run_id)
        state.network_id = network.id
        
        # Start PostgreSQL
        postgres = await self._start_postgres(run_id, network)
        state.postgres_container_id = postgres.id
        
        # Wait for PostgreSQL to be ready
        await self._wait_for_postgres(postgres)
        
        # Start DB Proxy
        proxy = await self._start_proxy(run_id, network, postgres)
        state.proxy_container_id = proxy.id
        
        # Start Agent Runner
        agent = await self._start_agent(run_id, network, state.request)
        state.agent_container_id = agent.id
        
        state.status = RunStatus.RUNNING
        state.started_at = datetime.utcnow()
        
        logger.info(
            "run_initialized",
            run_id=str(run_id),
            network=network.name,
        )
    
    async def _create_network(self, run_id: UUID) -> Network:
        """Create an isolated Docker network for this run."""
        network_name = f"{self.settings.docker_network_prefix}_{run_id.hex[:12]}"
        
        try:
            network = self.docker.networks.create(
                name=network_name,
                driver="bridge",
                internal=True,  # No internet access
                labels={
                    "chaostrace.run_id": str(run_id),
                    "chaostrace.component": "network",
                },
            )
            
            logger.debug(
                "network_created",
                run_id=str(run_id),
                network_name=network_name,
                network_id=network.id,
            )
            
            return network
            
        except APIError as e:
            raise NetworkError(f"Failed to create network: {e}")
    
    async def _start_postgres(
        self,
        run_id: UUID,
        network: Network,
    ) -> Container:
        """Start PostgreSQL container with seed data."""
        container_name = f"chaostrace_postgres_{run_id.hex[:12]}"
        
        # Prepare init script path
        init_sql = self.settings.sandbox_dir / "init.sql"
        
        volumes = {}
        if init_sql.exists():
            volumes[str(init_sql)] = {
                "bind": "/docker-entrypoint-initdb.d/init.sql",
                "mode": "ro",
            }
        
        try:
            container = self.docker.containers.run(
                image=self.settings.postgres_image,
                name=container_name,
                detach=True,
                network=network.name,
                environment={
                    "POSTGRES_USER": self.settings.postgres_user,
                    "POSTGRES_PASSWORD": self.settings.postgres_password,
                    "POSTGRES_DB": self.settings.postgres_db,
                },
                volumes=volumes,
                labels={
                    "chaostrace.run_id": str(run_id),
                    "chaostrace.component": "postgres",
                },
                # Resource limits
                mem_limit="512m",
                cpu_period=100000,
                cpu_quota=50000,  # 50% of one CPU
            )
            
            logger.debug(
                "postgres_started",
                run_id=str(run_id),
                container_id=container.id,
            )
            
            return container
            
        except APIError as e:
            raise ContainerStartError(f"Failed to start PostgreSQL: {e}")
    
    async def _wait_for_postgres(
        self,
        container: Container,
        timeout: int = 30,
    ) -> None:
        """Wait for PostgreSQL to be ready to accept connections."""
        for _ in range(timeout):
            try:
                result = container.exec_run(
                    "pg_isready -U sandbox",
                    demux=True,
                )
                if result.exit_code == 0:
                    logger.debug("postgres_ready", container_id=container.id)
                    return
            except Exception:
                pass
            
            await asyncio.sleep(1)
        
        raise ContainerStartError("PostgreSQL did not become ready in time")
    
    async def _start_proxy(
        self,
        run_id: UUID,
        network: Network,
        postgres: Container,
    ) -> Container:
        """Start the DB proxy container."""
        container_name = f"chaostrace_proxy_{run_id.hex[:12]}"
        
        # For now, we'll run the proxy in the same network
        # In production, this would be a separate container
        # with the proxy code mounted
        
        try:
            # Mount the project code
            project_root = self.settings.base_dir
            volumes = {
                str(project_root): {
                    "bind": "/app",
                    "mode": "ro",
                }
            }

            container = self.docker.containers.run(
                image="python:3.11-slim",
                name=container_name,
                detach=True,
                network=network.name,
                working_dir="/app",
                command="python -m chaostrace.db_proxy.proxy_server",  # Actual proxy command
                environment={
                    "CHAOSTRACE_RUN_ID": str(run_id),
                    "POSTGRES_HOST": f"chaostrace_postgres_{run_id.hex[:12]}",
                    "POSTGRES_PORT": "5432",
                    "PROXY_LISTEN_PORT": str(self.settings.proxy_listen_port),
                    "PYTHONPATH": "/app",
                },
                volumes=volumes,
                labels={
                    "chaostrace.run_id": str(run_id),
                    "chaostrace.component": "proxy",
                },
                mem_limit="256m",
            )
            
            # Install dependencies if needed (MVP hack)
            # In production, use a pre-built image
            container.exec_run("pip install structlog pydantic sqlglot pyyaml")
            
            logger.debug(
                "proxy_started",
                run_id=str(run_id),
                container_id=container.id,
            )
            
            return container
            
        except APIError as e:
            raise ContainerStartError(f"Failed to start proxy: {e}")
    
    async def _start_agent(
        self,
        run_id: UUID,
        network: Network,
        request: RunRequest,
    ) -> Container:
        """Start the agent runner container."""
        container_name = f"chaostrace_agent_{run_id.hex[:12]}"
        
        # Resolve agent path
        agent_path = Path(request.agent_entry)
        if not agent_path.is_absolute():
            agent_path = self.settings.base_dir / agent_path
        
        if not agent_path.exists():
            raise ContainerStartError(f"Agent file not found: {agent_path}")
            
        # Mount the entire project into the container to ensure dependencies import correctly
        # In a real scenario, we might want to be more selective or build a custom image
        project_root = self.settings.base_dir
        
        volumes = {
            str(project_root): {
                "bind": "/app",
                "mode": "rw",  # Allow writing logs/reports
            }
        }
        
        # Determine command based on agent type
        if request.agent_type == "python":
            # Run relative to project root in container
            rel_path = agent_path.relative_to(project_root)
            cmd = f"python {rel_path}"
        else:
            cmd = f"python {agent_path.name}"  # Fallback

        try:
            container = self.docker.containers.run(
                image="python:3.11-slim",
                name=container_name,
                detach=True,
                network=network.name,
                working_dir="/app",
                command=cmd,
                environment={
                    "CHAOSTRACE_RUN_ID": str(run_id),
                    "DB_HOST": f"chaostrace_proxy_{run_id.hex[:12]}",
                    "DB_PORT": str(self.settings.proxy_listen_port),
                    "PYTHONPATH": "/app",  # Add project root to python path
                    **request.environment,
                },
                volumes=volumes,
                labels={
                    "chaostrace.run_id": str(run_id),
                    "chaostrace.component": "agent",
                },
                mem_limit="512m",
            )
            
            # Install dependencies if requirements.txt exists
            # This is a bit of a hack for the MVP - ideally we'd build an image
            container.exec_run("pip install psycopg structlog requests pydantic")
            
            logger.debug(
                "agent_started",
                run_id=str(run_id),
                container_id=container.id,
                cmd=cmd,
            )
            
            return container
            
        except APIError as e:
            raise ContainerStartError(f"Failed to start agent: {e}")
    
    async def _monitor_run(self, run_id: UUID) -> None:
        """Monitor the run until completion or timeout."""
        state = self._runs[run_id]
        timeout = state.request.timeout_seconds
        start_time = datetime.utcnow()
        
        while True:
            elapsed = (datetime.utcnow() - start_time).total_seconds()
            
            if elapsed >= timeout:
                logger.warning(
                    "run_timeout",
                    run_id=str(run_id),
                    elapsed=elapsed,
                    timeout=timeout,
                )
                state.status = RunStatus.TERMINATED
                state.error_message = "Run timed out"
                break
            
            # Check if agent container is still running
            if state.agent_container_id:
                try:
                    container = self.docker.containers.get(
                        state.agent_container_id
                    )
                    if container.status != "running":
                        # Agent has exited
                        exit_code = container.attrs.get(
                            "State", {}
                        ).get("ExitCode", -1)
                        
                        if exit_code == 0:
                            state.status = RunStatus.COMPLETED
                        else:
                            state.status = RunStatus.FAILED
                            state.error_message = f"Agent exited with code {exit_code}"
                        break
                        
                except NotFound:
                    state.status = RunStatus.FAILED
                    state.error_message = "Agent container not found"
                    break
            
            await asyncio.sleep(1)
        
        state.ended_at = datetime.utcnow()
    
    async def _finalize_run(self, run_id: UUID) -> None:
        """Finalize the run and determine verdict."""
        state = self._runs[run_id]
        
        # Determine verdict based on status and violations
        if state.status == RunStatus.COMPLETED:
            if state.violations:
                state.verdict = Verdict.FAIL
            elif state.blocked_events > 0:
                state.verdict = Verdict.WARN
            else:
                state.verdict = Verdict.PASS
        elif state.status == RunStatus.FAILED:
            state.verdict = Verdict.FAIL
        else:
            state.verdict = Verdict.INCOMPLETE
        
        logger.info(
            "run_finalized",
            run_id=str(run_id),
            status=state.status.value,
            verdict=state.verdict.value if state.verdict else None,
            total_sql_events=state.total_sql_events,
            blocked_events=state.blocked_events,
        )
    
    async def _cleanup_run(self, run_id: UUID) -> None:
        """Clean up all Docker resources for a run."""
        state = self._runs.get(run_id)
        if not state:
            return
        
        if state and state.run_id:
             # Capture logs before cleanup
            try:
                log_dir = self.settings.logs_dir / str(state.run_id)
                log_dir.mkdir(parents=True, exist_ok=True)
                
                for name, cid in [
                    ("agent", state.agent_container_id),
                    ("proxy", state.proxy_container_id),
                    ("postgres", state.postgres_container_id)
                ]:
                    if cid:
                        try:
                            container = self.docker.containers.get(cid)
                            logs = container.logs().decode('utf-8', errors='replace')
                            (log_dir / f"{name}.log").write_text(logs)
                        except Exception as e:
                            logger.warning("failed_to_capture_logs", container=name, error=str(e))
            except Exception as e:
                logger.error("log_capture_failed", error=str(e))

        logger.info("cleaning_up_run", run_id=str(run_id))
        
        # Stop and remove containers
        for container_id in [
            state.agent_container_id,
            state.proxy_container_id,
            state.postgres_container_id,
        ]:
            if container_id:
                await self._remove_container(container_id)
        
        # Remove network
        if state.network_id:
            await self._remove_network(state.network_id)
        
        logger.info("run_cleaned_up", run_id=str(run_id))
    
    async def _remove_container(self, container_id: str) -> None:
        """Stop and remove a container."""
        try:
            container = self.docker.containers.get(container_id)
            container.stop(timeout=5)
            container.remove()
            logger.debug("container_removed", container_id=container_id)
        except NotFound:
            pass
        except APIError as e:
            logger.warning(
                "container_removal_failed",
                container_id=container_id,
                error=str(e),
            )
    
    async def _remove_network(self, network_id: str) -> None:
        """Remove a Docker network."""
        try:
            network = self.docker.networks.get(network_id)
            network.remove()
            logger.debug("network_removed", network_id=network_id)
        except NotFound:
            pass
        except APIError as e:
            logger.warning(
                "network_removal_failed",
                network_id=network_id,
                error=str(e),
            )
    
    async def get_run_status(self, run_id: UUID) -> RunState | None:
        """Get the current status of a run."""
        return self._runs.get(run_id)
    
    async def terminate_run(self, run_id: UUID) -> bool:
        """
        Terminate a running test.
        
        Returns True if the run was terminated, False if not found/already done.
        """
        state = self._runs.get(run_id)
        if not state:
            return False
        
        if state.status not in (RunStatus.PENDING, RunStatus.INITIALIZING, RunStatus.RUNNING):
            return False
        
        # Cancel the run task
        task = self._run_tasks.get(run_id)
        if task and not task.done():
            task.cancel()
        
        state.status = RunStatus.TERMINATED
        state.ended_at = datetime.utcnow()
        state.verdict = Verdict.INCOMPLETE
        
        # Cleanup
        await self._cleanup_run(run_id)
        
        logger.info("run_terminated", run_id=str(run_id))
        return True
    
    async def list_runs(
        self,
        page: int = 1,
        page_size: int = 20,
    ) -> tuple[list[RunState], int]:
        """List all runs with pagination."""
        runs = list(self._runs.values())
        runs.sort(key=lambda r: r.created_at, reverse=True)
        
        start = (page - 1) * page_size
        end = start + page_size
        
        return runs[start:end], len(runs)
    
    def close(self) -> None:
        """Close the Docker client."""
        if self._docker_client:
            self._docker_client.close()
            self._docker_client = None
