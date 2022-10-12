"""SWF activity task management."""

import datetime
import warnings
import dataclasses
import typing as t

from . import _common

if t.TYPE_CHECKING:
    import botocore.client
    from . import _activities
    from . import _executions


class Cancelled(Exception):
    """Current activity task has been cancelled."""


@dataclasses.dataclass
class TaskConfiguration(_common.Deserialisable):
    """Activity task configuration."""

    task_list: str
    """Task task-list."""

    runtime_timeout: t.Union[datetime.timedelta, None]
    """Finish timeout."""

    schedule_timeout: t.Union[datetime.timedelta, None]
    """Start timeout."""

    total_timeout: t.Union[datetime.timedelta, None]
    """Total finish timeout."""

    heartbeat_timeout: t.Union[datetime.timedelta, None] = _common.unset
    """Heartbeat timeout."""

    priority: int = None
    """Task priority."""

    @classmethod
    def from_api(cls, data) -> "TaskConfiguration":
        return cls(
            task_list=data["taskList"]["name"],
            runtime_timeout=_common.parse_timeout(data["taskStartToCloseTimeout"]),
            schedule_timeout=_common.parse_timeout(data["taskScheduleToStartTimeout"]),
            total_timeout=_common.parse_timeout(data["taskScheduleToCloseTimeout"]),
            heartbeat_timeout=_common.parse_timeout(data["taskHeartbeatTimeout"]),
            priority=int(data["taskPriority"]),
        )


@dataclasses.dataclass
class PartialTaskConfiguration(TaskConfiguration, _common.SerialisableToArguments):
    """Partial activity task configuration."""

    task_list: str = None
    runtime_timeout: t.Union[datetime.timedelta, None] = _common.unset
    schedule_timeout: t.Union[datetime.timedelta, None] = _common.unset
    total_timeout: t.Union[datetime.timedelta, None] = _common.unset

    @classmethod
    def from_api(cls, data) -> "PartialTaskConfiguration":
        return cls(
            task_list=data.get("taskList") and data["taskList"]["name"],
            runtime_timeout=(
                data.get("taskStartToCloseTimeout") and
                _common.parse_timeout(data["taskStartToCloseTimeout"])
            ),
            schedule_timeout=(
                data.get("taskScheduleToStartTimeout") and
                _common.parse_timeout(data["taskScheduleToStartTimeout"])
            ),
            total_timeout=(
                data.get("taskScheduleToCloseTimeout") and
                _common.parse_timeout(data["taskScheduleToCloseTimeout"])
            ),
            heartbeat_timeout=(
                data.get("taskHeartbeatTimeout") and
                _common.parse_timeout(data["taskHeartbeatTimeout"])
            ),
            priority=data.get("taskPriority") and int(data["taskPriority"]),
        )

    def get_api_args(self):
        data = {}

        if self.task_list or self.task_list == "":
            data["taskList"] = {"name": self.task_list}

        for timeout, name in [
            (self.runtime_timeout, "StartToClose"),
            (self.schedule_timeout, "ScheduleToStart"),
            (self.total_timeout, "ScheduleToClose"),
            (self.heartbeat_timeout, "Heartbeat"),
        ]:
            if timeout or timeout == datetime.timedelta(0):
                data[f"task{name}Timeout"] = str(int(timeout.total_seconds()))
            elif timeout is None:
                data[f"task{name}Timeout"] = "NONE"

        if self.priority or self.priority == 0:
            data["taskPriority"] = str(self.priority)

        return data


@dataclasses.dataclass
class WorkerTask(_common.Deserialisable):
    """Activity worker activity task."""

    token: str
    """Task token, provided by SWF."""

    id: str
    """Task ID."""

    activity: "_activities.ActivityId"
    """Task activity."""

    execution: "_executions.ExecutionId"
    """Task execution."""

    task_started_execution_history_event_id: int
    """History event ID for task start."""

    input: str = None
    """Task input."""

    @classmethod
    def from_api(cls, data) -> "WorkerTask":
        from . import _activities
        from . import _executions

        return cls(
            token=data["taskToken"],
            id=data["activityId"],
            activity=_activities.ActivityId.from_api(data["activityType"]),
            execution=_executions.ExecutionId.from_api(data["workflowExecution"]),
            task_started_execution_history_event_id=data["startedEventId"],
            input=data.get("input"),
        )


def get_number_of_pending_tasks(
    task_list: str,
    domain: str,
    client: "botocore.client.BaseClient" = None,
) -> int:
    """Get the number of pending activity tasks.

    Warns if the number of pending tasks is greater than what's returned.

    Args:
        task_list: activity task-list
        domain: domain of task-list
        client: SWF client

    Returns:
        number of pending tasks
    """

    client = _common.ensure_client(client)
    response = client.count_pending_activity_tasks(
        domain=domain, taskList=dict(name=task_list)
    )
    if response["truncated"]:
        warnings.warn("Actual task count greater than returned amount")
    return response["count"]


def request_task(
    task_list: str,
    domain: str,
    worker_identity: str = None,
    no_tasks_callback: t.Callable[[], None] = None,
    client: "botocore.client.BaseClient" = None,
) -> WorkerTask:
    """Request (poll for) an activity task; blocks until task is received.

    Args:
        task_list: activity task-list to request from
        domain: domain of task-list
        worker_identity: activity worker identity, recorded in execution
            history
        no_tasks_callback: called after no tasks were provided by SWF
        client: SWF client

    Returns:
        activity task
    """

    client = _common.ensure_client(client)
    kw = {}
    if worker_identity or worker_identity == "":
        kw["identity"] = worker_identity
    with _common.polling_socket_timeout():
        while True:
            response = client.poll_for_activity_task(
                domain=domain, taskList=dict(name=task_list), **kw
            )
            if response["taskToken"]:
                break
            no_tasks_callback()
    return WorkerTask.from_api(response)


def send_heartbeat(
    token: str,
    details: str = None,
    client: "botocore.client.BaseClient" = None,
) -> None:
    """Send a heartbeat to SWF for the current activity task.

    Args:
        token: activity task token
        details: activity task progress message
        client: SWF client

    Raises:
        Cancelled: if activity task was cancelled by decider
    """

    client = _common.ensure_client(client)
    kw = {}
    if details or details == "":
        kw["details"] = details
    response = client.record_activity_task_heartbeat(taskToken=token, **kw)
    if response["cancelRequested"]:
        raise Cancelled(f"SWF activity task {token!r} cancelled")


def cancel_task(
    token: str,
    details: str = None,
    client: "botocore.client.BaseClient" = None,
) -> None:
    """Cancel the current activity task.

    Only valid if the activity task is open and has a cancellation request.

    Args:
        token: activity task token
        details: extra information, usually for explanation
        client: SWF client
    """

    client = _common.ensure_client(client)
    kw = {}
    if details or details == "":
        kw["details"] = details
    client.respond_activity_task_canceled(taskToken=token, **kw)


def complete_task(
    token: str,
    result: str = None,
    client: "botocore.client.BaseClient" = None,
) -> None:
    """Complete the current activity task.

    Only valid if the activity task is open.

    Args:
        token: activity task token
        result: task result
        client: SWF client
    """

    client = _common.ensure_client(client)
    kw = {}
    if result or result == "":
        kw["result"] = result
    client.respond_activity_task_completed(taskToken=token, **kw)


def fail_task(
    token: str,
    reason: str = None,
    details: str = None,
    client: "botocore.client.BaseClient" = None,
) -> None:
    """Fail the current activity task.

    Only valid if the activity task is open.

    Args:
        token: activity task token
        reason: failure reason, usually for classification
        details: failure details, usually for explanation
        client: SWF client
    """

    client = _common.ensure_client(client)
    kw = {}
    if reason or reason == "":
        kw["reason"] = reason
    if details or details == "":
        kw["details"] = details
    client.respond_activity_task_failed(taskToken=token, **kw)
