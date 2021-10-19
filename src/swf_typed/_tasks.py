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
    pass


@dataclasses.dataclass
class TaskConfiguration:
    task_list: str
    timeout: t.Union[datetime.timedelta, None]
    schedule_timeout: t.Union[datetime.timedelta, None]
    total_timeout: t.Union[datetime.timedelta, None]
    heartbeat_timeout: t.Union[datetime.timedelta, None] = _common.unset
    priority: int = None

    @classmethod
    def from_api(cls, data: t.Dict[str, t.Any]) -> "TaskConfiguration":
        return cls(
            task_list=data["taskList"]["name"],
            timeout=_common.parse_timeout(data["taskStartToCloseTimeout"]),
            schedule_timeout=_common.parse_timeout(data["taskScheduleToStartTimeout"]),
            total_timeout=_common.parse_timeout(data["taskScheduleToCloseTimeout"]),
            heartbeat_timeout=_common.parse_timeout(data["taskHeartbeatTimeout"]),
            priority=int(data["taskPriority"]),
        )


@dataclasses.dataclass
class PartialTaskConfiguration(TaskConfiguration):
    task_list: str = None
    timeout: t.Union[datetime.timedelta, None] = _common.unset
    schedule_timeout: t.Union[datetime.timedelta, None] = _common.unset
    total_timeout: t.Union[datetime.timedelta, None] = _common.unset

    @classmethod
    def from_api(cls, data: t.Dict[str, t.Any]) -> "PartialTaskConfiguration":
        return cls(
            task_list=data.get("taskList") and data["taskList"]["name"],
            timeout=(
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

    def get_api_args(self) -> t.Dict[str, t.Any]:
        data = {}

        if self.task_list or self.task_list == "":
            data["taskList"] = {"name": self.task_list}

        for timeout, name in [
            (self.timeout, "StartToClose"),
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
class Task:
    token: str
    id: str
    activity: "_activities.ActivityId"
    execution: "_executions.ExecutionId"
    task_started_execution_history_event_id: int
    input: str = None

    @classmethod
    def from_api(cls, data: t.Dict[str, t.Any]) -> "Task":
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
    client = _common.ensure_client(client)
    response = client.count_pending_activity_tasks(
        domain=domain,
        task_list=dict(name=task_list),
    )
    if response["truncated"]:
        warnings.warn("Actual task count greater than returned amount")
    return response["count"]


def poll_for_task(
    task_list: str,
    domain: str,
    worker_identity: str = None,
    client: "botocore.client.BaseClient" = None,
) -> Task:
    client = _common.ensure_client(client)
    kw = {}
    if worker_identity or worker_identity == "":
        kw["identity"] = worker_identity
    with _common.polling_socket_timeout():
        response = {"taskToken": ""}
        while not response["taskToken"]:
            response = client.poll_for_activity_task(
                domain=domain, task_list=dict(name=task_list), **kw
            )
    return Task.from_api(response)


def record_task_heartbeat(
    token: str,
    details: str = None,
    client: "botocore.client.BaseClient" = None,
) -> None:
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
    client = _common.ensure_client(client)
    kw = {}
    if reason or reason == "":
        kw["reason"] = reason
    if details or details == "":
        kw["details"] = details
    client.respond_activity_task_failed(taskToken=token, **kw)
