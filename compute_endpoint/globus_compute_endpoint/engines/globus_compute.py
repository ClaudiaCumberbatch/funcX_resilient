import datetime
import inspect
import logging
import os
import queue
import shlex
import sys
import threading
import typing as t
import uuid
from concurrent.futures import Future
from getpass import getuser
from globus_compute_common.messagepack.message_types import (
    EPStatusReport,
    TaskTransition,
)
from globus_compute_endpoint.engines.base import (
    GlobusComputeEngineBase,
    ReportingThread,
)
from globus_compute_endpoint.strategies import SimpleStrategy
from parsl.app.futures import DataFuture
from parsl.dataflow.futures import AppFuture
from parsl.dataflow.rundirs import make_rundir
from parsl.dataflow.states import States, FINAL_STATES, FINAL_FAILURE_STATES
from parsl.dataflow.taskrecord import TaskRecord
from parsl.executors.high_throughput.executor import HighThroughputExecutor
from parsl.monitoring.message_type import MessageType
from parsl.utils import get_version, get_std_fname_mode
from socket import gethostname
from typing import Optional
from uuid import uuid4

logger = logging.getLogger(__name__)
DOCKER_CMD_TEMPLATE = "docker run {options} -v {rundir}:{rundir} -t {image} {command}"
APPTAINER_CMD_TEMPLATE = "apptainer run {options} {image} {command}"
SINGULARITY_CMD_TEMPLATE = "singularity run {options} {image} {command}"
VALID_CONTAINER_TYPES = ("docker", "singularity", "apptainer", "custom", None)


class GlobusComputeEngine(GlobusComputeEngineBase):
    """GlobusComputeEngine is a wrapper over Parsl's HighThroughputExecutor"""

    def __init__(
        self,
        *args,
        label: str = "GlobusComputeEngine",
        max_retries_on_system_failure: int = 0,
        strategy: t.Optional[SimpleStrategy] = SimpleStrategy(),
        executor: t.Optional[HighThroughputExecutor] = None,
        container_type: t.Literal[VALID_CONTAINER_TYPES] = None,  # type: ignore
        container_uri: t.Optional[str] = None,
        container_cmd_options: t.Optional[str] = None,
        # monitoring
        monitoring = None,
        **kwargs,
    ):
        """The ``GlobusComputeEngine`` is a shim over `Parsl's HighThroughputExecutor
        <parslhtex_>`_, almost all of arguments are passed along, unfettered.
        Consequently, please reference `Parsl's HighThroughputExecutor <parslhtex_>`_
        documentation for a complete list of arguments; we list below only the
        arguments specific to the ``GlobusComputeEngine``.

        .. _parslhtex: https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.HighThroughputExecutor.html

        Parameters
        ----------

        label: str
           Label used to name engine log directories and batch jobs
           default: "GlobusComputeEngine"

        max_retries_on_system_failure: int
           Set the number of retries for functions that fail due to
           system failures such as node failure/loss. Since functions
           can fail after partial runs, consider additional cleanup
           logic before enabling this functionality
           default: 0

        strategy: Stategy object
           Specify scaling strategy.
           default: SimpleStrategy

        """  # noqa: E501
        self.run_dir = os.getcwd()
        self.label = label
        self._status_report_thread = ReportingThread(target=self.report_status, args=[])
        super().__init__(
            *args, max_retries_on_system_failure=max_retries_on_system_failure, **kwargs
        )
        self.strategy = strategy
        self.max_workers_per_node = 1

        self.container_type = container_type
        assert (
            self.container_type in VALID_CONTAINER_TYPES
        ), f"{self.container_type} is not a valid container_type"
        self.container_uri = container_uri
        self.container_cmd_options = container_cmd_options

        if executor is None:
            executor = HighThroughputExecutor(  # type: ignore
                *args,
                label=label,
                **kwargs,
            )
        self.executor = executor

        self.run_dir = make_rundir('runinfo')
        self.task_state_counts = {state: 0 for state in States}
        self.run_id = str(uuid4())
        self.monitoring = monitoring

         # hub address and port for interchange to connect
        self.hub_address = None  # type: Optional[str]
        self.hub_interchange_port = None  # type: Optional[int]
        if self.monitoring:
            if self.monitoring.logdir is None:
                self.monitoring.logdir = self.run_dir
            self.hub_address = self.monitoring.hub_address
            self.hub_interchange_port = self.monitoring.start(self.run_id, self.run_dir)

        self.time_began = datetime.datetime.now()
        self.time_completed: Optional[datetime.datetime] = None

        self.workflow_name = None
        if self.monitoring is not None and self.monitoring.workflow_name is not None:
            self.workflow_name = self.monitoring.workflow_name
        else:
            for frame in inspect.stack():
                logger.debug("Considering candidate for workflow name: {}".format(frame.filename))
                fname = os.path.basename(str(frame.filename))
                parsl_file_names = ['dflow.py', 'typeguard.py', '__init__.py']
                # Find first file name not considered a parsl file
                if fname not in parsl_file_names:
                    self.workflow_name = fname
                    logger.debug("Using {} as workflow name".format(fname))
                    break
            else:
                logger.debug("Could not choose a name automatically")
                self.workflow_name = "unnamed"

        self.workflow_version = str(self.time_began.replace(microsecond=0))
        if self.monitoring is not None and self.monitoring.workflow_version is not None:
            self.workflow_version = self.monitoring.workflow_version

        workflow_info = {
                'python_version': "{}.{}.{}".format(sys.version_info.major,
                                                    sys.version_info.minor,
                                                    sys.version_info.micro),
                'parsl_version': get_version(),
                "time_began": self.time_began,
                'time_completed': None,
                'run_id': self.run_id,
                'workflow_name': self.workflow_name,
                'workflow_version': self.workflow_version,
                'rundir': self.run_dir,
                'tasks_completed_count': self.task_state_counts[States.exec_done],
                'tasks_failed_count': self.task_state_counts[States.failed],
                'user': getuser(),
                'host': gethostname(),
        }

        if self.monitoring:
            self.monitoring.send(MessageType.WORKFLOW_INFO,
                                 workflow_info)

    def containerized_launch_cmd(self) -> str:
        """Recompose executor's launch_cmd to launch with containers

        Returns
        -------
        str launch_cmd
        """
        launch_cmd = self.executor.launch_cmd
        # Adding assert here since mypy can't figure out launch_cmd's type
        assert launch_cmd
        if self.container_type == "docker":
            launch_cmd = DOCKER_CMD_TEMPLATE.format(
                image=self.container_uri,
                rundir=self.run_dir,
                command=launch_cmd,
                options=self.container_cmd_options or "",
            )
        elif self.container_type == "apptainer":
            launch_cmd = APPTAINER_CMD_TEMPLATE.format(
                image=self.container_uri,
                command=launch_cmd,
                options=self.container_cmd_options or "",
            )
        elif self.container_type == "singularity":
            launch_cmd = SINGULARITY_CMD_TEMPLATE.format(
                image=self.container_uri,
                command=launch_cmd,
                options=self.container_cmd_options or "",
            )
        elif self.container_type == "custom":
            assert (
                self.container_cmd_options
            ), "GCE.container_cmd_options is required for GCE.container_type=custom"
            template = self.container_cmd_options.replace(
                "{EXECUTOR_RUNDIR}", str(self.run_dir)
            )
            launch_cmd = template.replace("{EXECUTOR_LAUNCH_CMD}", launch_cmd)

        return launch_cmd

    def start(
        self,
        *args,
        endpoint_id: t.Optional[uuid.UUID] = None,
        run_dir: t.Optional[str] = None,
        results_passthrough: t.Optional[queue.Queue] = None,
        **kwargs,
    ):
        assert endpoint_id, "GCExecutor requires kwarg:endpoint_id at start"
        assert run_dir, "GCExecutor requires kwarg:run_dir at start"

        self.endpoint_id = endpoint_id
        self.run_dir = run_dir
        self.executor.run_dir = self.run_dir
        script_dir = os.path.join(self.run_dir, "submit_scripts")
        self.executor.provider.script_dir = script_dir
        if self.container_type:
            containerized_launch_cmd = self.containerized_launch_cmd()
            self.executor.launch_cmd = shlex.join(shlex.split(containerized_launch_cmd))
            logger.info(
                f"Containerized launch cmd template: {self.executor.launch_cmd}"
            )

        if (
            self.executor.provider.channel
            and not self.executor.provider.channel.script_dir
        ):
            self.executor.provider.channel.script_dir = script_dir

        os.makedirs(self.executor.provider.script_dir, exist_ok=True)
        if results_passthrough:
            # Only update the default queue in GCExecutorBase if
            # a queue is passed in
            self.results_passthrough = results_passthrough
        self.executor.start()
        if self.strategy:
            self.strategy.start(self)
        self._status_report_thread.start()

    def _submit(
        self,
        func: t.Callable,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> Future:
        task_record = self._create_task_record(func, str(uuid4()), *args, **kwargs)
        task_record['args'] = args
        task_record['kwargs'] = kwargs
        self._send_task_log_info(task_record)
        return self.executor.submit(func, {}, *args, **kwargs)
    
    def _create_task_record(self, func, task_id, *args, **kwargs):
        resource_specification = kwargs.get('parsl_resource_specification', {})
        task_record: TaskRecord
        task_record = {'depends': [],
                       'dfk': self,
                       'executor': self.executor.label,
                       'func_name': func.__name__,
                       'memoize': None, #cache,
                       'hashsum': None,
                       'exec_fu': None,
                       'fail_count': 0,
                       'fail_cost': 0,
                       'fail_history': [],
                       'from_memo': None,
                       'ignore_for_cache': None, #ignore_for_cache,
                       'join': None, #join,
                       'joins': None,
                       'try_id': 0,
                       'id': task_id,
                       'task_launch_lock': threading.Lock(),
                       'time_invoked': datetime.datetime.now(),
                       'time_returned': None,
                       'try_time_launched': None,
                       'try_time_returned': None,
                       'resource_specification': resource_specification}
        return task_record
    
    def _send_task_log_info(self, task_record: TaskRecord) -> None:
        if self.monitoring:
            task_log_info = self._create_task_log_info(task_record)
            self.monitoring.send(MessageType.TASK_INFO, task_log_info)

    def _create_task_log_info(self, task_record):
        """
        Create the dictionary that will be included in the log.
        """
        # info_to_monitor = ['func_name', 'memoize', 'hashsum', 'fail_count', 'fail_cost', 'status',
        info_to_monitor = ['func_name', 'memoize', 'hashsum', 'fail_count', 'fail_cost', 
                           'id', 'time_invoked', 'try_time_launched', 'time_returned', 'try_time_returned']#, 'executor']

        task_log_info = {"task_" + k: task_record[k] for k in info_to_monitor}
        task_log_info['run_id'] = self.run_id
        task_log_info['try_id'] = task_record['try_id']
        task_log_info['timestamp'] = datetime.datetime.now()
        # task_log_info['task_status_name'] = task_record['status'].name
        task_log_info['tasks_failed_count'] = self.task_state_counts[States.failed]
        task_log_info['tasks_completed_count'] = self.task_state_counts[States.exec_done]
        task_log_info['tasks_memo_completed_count'] = self.task_state_counts[States.memo_done]
        task_log_info['from_memo'] = task_record['from_memo']
        task_log_info['task_inputs'] = str(task_record['kwargs'].get('inputs', None))
        task_log_info['task_outputs'] = str(task_record['kwargs'].get('outputs', None))
        task_log_info['task_stdin'] = task_record['kwargs'].get('stdin', None)
        stdout_spec = task_record['kwargs'].get('stdout', None)
        stderr_spec = task_record['kwargs'].get('stderr', None)
        try:
            stdout_name, _ = get_std_fname_mode('stdout', stdout_spec)
        except Exception as e:
            logger.warning("Incorrect stdout format {} for Task {}".format(stdout_spec, task_record['id']))
            stdout_name = str(e)
        try:
            stderr_name, _ = get_std_fname_mode('stderr', stderr_spec)
        except Exception as e:
            logger.warning("Incorrect stderr format {} for Task {}".format(stderr_spec, task_record['id']))
            stderr_name = str(e)
        task_log_info['task_stdout'] = stdout_name
        task_log_info['task_stderr'] = stderr_name
        task_log_info['task_fail_history'] = ",".join(task_record['fail_history'])
        task_log_info['task_depends'] = None
        if task_record['depends'] is not None:
            task_log_info['task_depends'] = ",".join([str(t.tid) for t in task_record['depends']
                                                      if isinstance(t, AppFuture) or isinstance(t, DataFuture)])
        task_log_info['task_joins'] = None

        if isinstance(task_record['joins'], list):
            task_log_info['task_joins'] = ",".join([str(t.tid) for t in task_record['joins']
                                                    if isinstance(t, AppFuture) or isinstance(t, DataFuture)])
        elif isinstance(task_record['joins'], Future):
            task_log_info['task_joins'] = ",".join([str(t.tid) for t in [task_record['joins']]
                                                    if isinstance(t, AppFuture) or isinstance(t, DataFuture)])

        return task_log_info

    @property
    def provider(self):
        return self.executor.provider

    def get_connected_managers(self) -> t.List[t.Dict[str, t.Any]]:
        """
        Returns
        -------
        List of dicts containing info for all connected managers
        """
        return self.executor.connected_managers()

    def get_total_managers(self, managers: t.List[t.Dict[str, t.Any]]) -> int:
        """
        Parameters
        ----------
        managers: list[dict[str, Any]]
            List of dicts containing info for all connected managers

        Returns
        -------
        Total number of managers
        """
        return len(managers)

    def get_total_active_managers(self, managers: t.List[t.Dict[str, t.Any]]) -> int:
        """
        Parameters
        ----------
        managers: list[dict[str, Any]]
            List of dicts containing info for all connected managers

        Returns
        -------
        Number of managers that have capacity for new tasks
        """
        return sum(1 for m in managers if m["active"])

    def get_outstanding_breakdown(
        self, managers: t.Optional[t.List[t.Dict[str, t.Any]]] = None
    ) -> t.List[t.Tuple[str, int, bool]]:
        """
        Parameters
        ----------
        managers: list[dict[str, Any]] | None
            List of dicts containing info for all connected managers

        Returns
        -------
        List of tuples of the form (component, # of tasks on component, active?)
        """
        if managers is None:
            managers = self.get_connected_managers()
        total_task_count = self.executor.outstanding
        breakdown = [(m["manager"], m["tasks"], m["active"]) for m in managers]
        total_count_managers = sum([m["tasks"] for m in managers])
        task_count_interchange = total_task_count - total_count_managers
        breakdown = [("interchange", task_count_interchange, True)] + breakdown
        return breakdown

    def get_total_tasks_outstanding(self) -> dict:
        """
        Returns
        -------
        Dict of type {str_task_type: count_tasks}
        """
        return {"RAW": self.executor.outstanding}

    def get_total_tasks_pending(self, managers: t.List[t.Dict[str, t.Any]]) -> int:
        """
        Parameters
        ----------
        managers: list[dict[str, Any]]
            List of dicts containing info for all connected managers

        Returns
        -------
        Total number of pending tasks
        """
        outstanding = self.get_outstanding_breakdown(managers=managers)
        return outstanding[0][1]  # Queued in interchange

    def provider_status(self):
        status = []
        if self.provider:
            # ex.locks is a dict of block_id:job_id mappings
            job_ids = self.executor.blocks.values()
            status = self.provider.status(job_ids=job_ids)
        return status

    def get_total_live_workers(
        self, managers: t.Optional[t.List[t.Dict[str, t.Any]]] = None
    ) -> int:
        """
        Parameters
        ----------
        managers: list[dict[str, Any]]
            List of dicts containing info for all connected managers

        Returns
        -------
        Total number of live workers
        """
        if managers is None:
            managers = self.get_connected_managers()
        return sum([mgr["worker_count"] for mgr in managers])

    def get_total_idle_workers(self, managers: t.List[t.Dict[str, t.Any]]) -> int:
        """
        Parameters
        ----------
        managers: list[dict[str, Any]]
            List of dicts containing info for all connected managers

        Returns
        -------
        Total number of workers that are not actively running tasks
        """
        idle_workers = 0
        for mgr in managers:
            workers = mgr["worker_count"]
            tasks = mgr["tasks"]
            idle_workers += max(0, workers - tasks)
        return idle_workers

    def scale_out(self, blocks: int):
        logger.info(f"Scaling out {blocks} blocks")
        return self.executor.scale_out(blocks=blocks)

    def scale_in(self, blocks: int):
        logger.info(f"Scaling in {blocks} blocks")
        return self.executor.scale_in(blocks=blocks)

    def _handle_task_exception(
        self,
        task_id: str,
        execution_begin: TaskTransition,
        exception: BaseException,
    ) -> bytes:
        result_bytes = b""
        retry_info = self._retry_table[task_id]
        if retry_info["retry_count"] < self.max_retries_on_system_failure:
            retry_info["retry_count"] += 1
            retry_info["exception_history"].append(exception)
            self.submit(task_id, retry_info["packed_task"])
        else:
            # This is a terminal state
            result_bytes = super()._handle_task_exception(
                task_id=task_id, execution_begin=execution_begin, exception=exception
            )

        return result_bytes

    @property
    def scaling_enabled(self) -> bool:
        """Indicates whether scaling is possible"""
        max_blocks = self.executor.provider.max_blocks
        return max_blocks > 0

    def get_status_report(self) -> EPStatusReport:
        """
        Returns
        -------
        Object containing info on the current status of the endpoint
        """
        managers = self.get_connected_managers()
        executor_status: t.Dict[str, t.Any] = {
            "task_id": -2,  # Deprecated
            "info": {
                "total_cores": 0,  # TODO
                "total_mem": 0,  # TODO
                "new_core_hrs": 0,  # TODO
                "total_core_hrs": 0,  # TODO
                "managers": self.get_total_managers(managers=managers),
                "active_managers": self.get_total_active_managers(managers=managers),
                "total_workers": self.get_total_live_workers(managers=managers),
                "idle_workers": self.get_total_idle_workers(managers=managers),
                "pending_tasks": self.get_total_tasks_pending(managers=managers),
                "outstanding_tasks": self.get_total_tasks_outstanding()["RAW"],
                "worker_mode": 0,  # Deprecated
                "scheduler_mode": 0,  # Deprecated
                "scaling_enabled": self.scaling_enabled,
                "mem_per_worker": self.executor.mem_per_worker,
                "cores_per_worker": self.executor.cores_per_worker,
                "prefetch_capacity": self.executor.prefetch_capacity,
                "max_blocks": self.executor.provider.max_blocks,
                "min_blocks": self.executor.provider.min_blocks,
                "max_workers_per_node": self.executor.max_workers,
                "nodes_per_block": self.executor.provider.nodes_per_block,
                "heartbeat_period": self.executor.heartbeat_period,
            },
        }
        task_status_deltas: t.Dict[str, t.List[TaskTransition]] = {}  # TODO
        return EPStatusReport(
            endpoint_id=self.endpoint_id,
            global_state=executor_status,
            task_statuses=task_status_deltas,
        )

    def shutdown(self, /, **kwargs) -> None:
        self._status_report_thread.stop()
        if self.strategy:
            self.strategy.close()
        self.executor.shutdown()
