"""SWF workflow execution management."""

import abc
import enum
import datetime
import warnings
import functools
import dataclasses
import typing as t

from . import _common

if t.TYPE_CHECKING:
    import botocore.client
    from . import _workflows


@dataclasses.dataclass
class CurrentExecutionId:
    id: str

    @classmethod
    def from_api(cls, data: t.Dict[str, t.Any]) -> "CurrentExecutionId":
        return cls(id=data["workflowId"])

    def to_api(self) -> t.Dict[str, str]:
        return {"workflowId": self.id}


@dataclasses.dataclass
class ExecutionId(CurrentExecutionId):
    run_id: str

    @classmethod
    def from_api(cls, data) -> "ExecutionId":
        return cls(id=data["workflowId"], run_id=data["runId"])

    def to_api(self):
        data = super().to_api()
        data["runId"] = self.run_id
        return data


class ExecutionStatus(str, enum.Enum):
    open = "OPEN"
    started = "OPEN"
    completed = "COMPLETED"
    failed = "FAILED"
    cancelled = "CANCELED"
    terminated = "TERMINATED"
    continued_as_new = "CONTINUED_AS_NEW"
    timed_out = "TIMED_OUT"


@dataclasses.dataclass
class ExecutionInfo:
    execution: ExecutionId
    workflow: "_workflows.WorkflowId"
    started: datetime.datetime
    status: ExecutionStatus
    cancel_requested: bool
    closed: datetime.datetime = None
    parent: ExecutionId = None
    tags: t.List[str] = None

    @classmethod
    def from_api(cls, data: t.Dict[str, t.Any]) -> "ExecutionInfo":
        from . import _workflows

        status_data = data["executionStatus"]
        if status_data == "CLOSED":
            status_data = data["closeStatus"]
        return cls(
            execution=ExecutionId.from_api(data["execution"]),
            workflow=_workflows.WorkflowId.from_api(data["workflowType"]),
            started=data["startTimestamp"],
            status=ExecutionStatus(status_data),
            cancel_requested=data["cancelRequested"],
            closed=data.get("closeTimestamp"),
            parent=data.get("parent") and ExecutionId.from_api(data["parent"]),
            tags=data.get("tagList"),
        )


class ChildExecutionTerminationPolicy(str, enum.Enum):
    terminate = "TERMINATE"
    request_cancel = "REQUEST_CANCEL"
    abandon = "ABANDON"


@dataclasses.dataclass
class ExecutionConfiguration:
    timeout: t.Union[datetime.timedelta, None]
    decision_task_timeout: t.Union[datetime.timedelta, None]
    decision_task_list: str
    decision_task_priority: int
    child_execution_policy_on_termination: ChildExecutionTerminationPolicy
    lambda_iam_role_arn: str = None

    @classmethod
    def from_api(cls, data: t.Dict[str, t.Any]) -> "ExecutionConfiguration":
        child_policy = ChildExecutionTerminationPolicy(data["childPolicy"])
        decision_task_timeout = _common.parse_timeout(data["taskStartToCloseTimeout"])
        return cls(
            timeout=_common.parse_timeout(data["executionStartToCloseTimeout"]),
            decision_task_timeout=decision_task_timeout,
            decision_task_list=data["taskList"]["name"],
            decision_task_priority=int(data["taskPriority"]),
            child_execution_policy_on_termination=child_policy,
            lambda_iam_role_arn=data.get("lambdaRole"),
        )


@dataclasses.dataclass
class PartialExecutionConfiguration(ExecutionConfiguration):
    timeout: t.Union[datetime.timedelta, None] = _common.unset
    decision_task_timeout: t.Union[datetime.timedelta, None] = _common.unset
    decision_task_list: str = None
    decision_task_priority: int = None
    child_execution_policy_on_termination: ChildExecutionTerminationPolicy = None

    @classmethod
    def from_api(cls, data: t.Dict[str, t.Any]) -> "PartialExecutionConfiguration":
        return cls(
            timeout=(
                data.get("executionStartToCloseTimeout") and
                _common.parse_timeout(data["executionStartToCloseTimeout"])
            ),
            decision_task_timeout=(
                data.get("taskStartToCloseTimeout") and
                _common.parse_timeout(data["taskStartToCloseTimeout"])
            ),
            decision_task_list=data.get("taskList") and data["taskList"]["name"],
            decision_task_priority=(
                data.get("taskPriority") and int(data["taskPriority"])
            ),
            child_execution_policy_on_termination=(
                data.get("childPolicy") and
                ChildExecutionTerminationPolicy(data["childPolicy"])
            ),
            lambda_iam_role_arn=data.get("lambdaRole"),
        )

    def get_api_args(self) -> t.Dict[str, t.Any]:
        data = {}

        if self.timeout or self.timeout == datetime.timedelta(0):
            data["executionStartToCloseTimeout"] = str(
                int(self.timeout.total_seconds())
            )
        elif self.timeout is None:
            data["executionStartToCloseTimeout"] = "NONE"

        decision_task_timeout = self.decision_task_timeout
        if decision_task_timeout or decision_task_timeout == datetime.timedelta(0):
            data["taskStartToCloseTimeout"] = str(
                int(decision_task_timeout.total_seconds())
            )
        elif decision_task_timeout is None:
            data["taskStartToCloseTimeout"] = "NONE"

        if self.decision_task_list or self.decision_task_list == "":
            data["taskList"] = {"name": self.decision_task_list}

        if self.decision_task_priority or self.decision_task_priority == 0:
            data["taskPriority"] = str(self.decision_task_priority)

        if self.child_execution_policy_on_termination:
            data["childPolicy"] = self.child_execution_policy_on_termination.value

        if self.lambda_iam_role_arn or self.lambda_iam_role_arn == "":
            data["lambdaRole"] = self.lambda_iam_role_arn

        return data


@dataclasses.dataclass
class ExecutionOpenCounts:
    activity_tasks: int = None
    decision_tasks: int = None
    timers: int = None
    child_executions: int = None
    lambda_tasks: int = None

    @classmethod
    def from_api(cls, data: t.Dict[str, t.Any]) -> "ExecutionOpenCounts":
        return cls(
            data["openActivityTasks"],
            data["openDecisionTasks"],
            data["openTimers"],
            data["openChildWorkflowExecutions"],
            data["openLambdaFunctions"],
        )


@dataclasses.dataclass
class ExecutionDetails:
    info: ExecutionInfo
    configuration: ExecutionConfiguration = None
    n_open: ExecutionOpenCounts = None
    latest_activity_task_scheduled: datetime.datetime = None
    latest_context: str = None

    @classmethod
    def from_api(cls, data: t.Dict[str, t.Any]) -> "ExecutionDetails":
        config = ExecutionConfiguration.from_api(data["executionConfiguration"])
        return cls(
            info=ExecutionInfo.from_api(data["executionInfo"]),
            configuration=config,
            n_open=ExecutionOpenCounts.from_api(data["openCounts"]),
            latest_activity_task_scheduled=data["latestActivityTaskTimestamp"],
            latest_context=data.get("latestExecutionContext"),
        )


@dataclasses.dataclass
class ExecutionFilter(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_api_args(self) -> t.Dict[str, t.Any]:
        pass


@dataclasses.dataclass
class DateTimeFilter:
    oldest: datetime.datetime
    latest: datetime.datetime = None


@dataclasses.dataclass
class StartTimeExecutionFilter(DateTimeFilter, ExecutionFilter):
    def get_api_args(self):
        data = {"startTimeFilter": {"oldestDate": self.oldest}}
        if self.latest:
            data["startTimeFilter"]["latestDate"] = self.latest
        return data


@dataclasses.dataclass
class CloseTimeExecutionFilter(DateTimeFilter, ExecutionFilter):
    def get_api_args(self):
        data = {"closeTimeFilter": {"oldestDate": self.oldest}}
        if self.latest:
            data["closeTimeFilter"]["latestDate"] = self.latest
        return data


@dataclasses.dataclass
class IdExecutionFilter(ExecutionFilter, metaclass=abc.ABCMeta):
    execution: CurrentExecutionId

    def get_api_args(self):
        return {"executionFilter": self.execution.to_api()}


@dataclasses.dataclass
class WorkflowTypeExecutionFilter(ExecutionFilter, metaclass=abc.ABCMeta):
    workflow: t.Union["_workflows.WorkflowId", "_workflows.WorkflowIdFilter"]

    def get_api_args(self):
        return {"typeFilter": self.workflow.to_api()}


@dataclasses.dataclass
class TagExecutionFilter(ExecutionFilter):
    tag: str

    def get_api_args(self):
        return {"tagFilter": {"tag": self.tag}}


@dataclasses.dataclass
class CloseStatusExecutionFilter(ExecutionFilter):
    status: str

    def get_api_args(self):
        return {"closeStatusFilter": {"status": self.status}}


def _get_number_of_executions(
    time_filter: t.Union[StartTimeExecutionFilter, CloseTimeExecutionFilter],
    domain: str,
    property_filter: t.Union[
        IdExecutionFilter,
        WorkflowTypeExecutionFilter,
        TagExecutionFilter,
        CloseStatusExecutionFilter,
        None,
    ],
    client_method: t.Callable[..., t.Dict[str, t.Any]],
) -> int:
    kw = time_filter.get_api_args()
    if property_filter:
        kw.update(property_filter.get_api_args())
    response = client_method(domain=domain, **kw)
    if response["truncated"]:
        warnings.warn("Actual execution count greater than returned amount")
    return response["count"]


def get_number_of_closed_executions(
    time_filter: t.Union[StartTimeExecutionFilter, CloseTimeExecutionFilter],
    domain: str,
    property_filter: t.Union[
        IdExecutionFilter,
        WorkflowTypeExecutionFilter,
        TagExecutionFilter,
        CloseStatusExecutionFilter,
    ] = None,
    client: "botocore.client.BaseClient" = None,
) -> int:
    client = _common.ensure_client(client)
    return _get_number_of_executions(
        time_filter, domain, property_filter, client.count_closed_workflow_executions
    )


def get_number_of_open_executions(
    started_filter: StartTimeExecutionFilter,
    domain: str,
    property_filter: t.Union[
        IdExecutionFilter,
        WorkflowTypeExecutionFilter,
        TagExecutionFilter,
    ] = None,
    client: "botocore.client.BaseClient" = None,
) -> int:
    client = _common.ensure_client(client)
    return _get_number_of_executions(
        started_filter, domain, property_filter, client.count_open_workflow_executions
    )


def describe_execution(
    execution: ExecutionId,
    domain: str,
    client: "botocore.client.BaseClient" = None,
) -> ExecutionDetails:
    client = _common.ensure_client(client)
    response = client.describe_workflow_execution(
        domain=domain, execution=execution.to_api()
    )
    return ExecutionDetails.from_api(response)


def list_closed_executions(
    time_filter: t.Union[StartTimeExecutionFilter, CloseTimeExecutionFilter],
    domain: str,
    property_filter: t.Union[
        IdExecutionFilter,
        WorkflowTypeExecutionFilter,
        TagExecutionFilter,
        CloseStatusExecutionFilter,
    ] = None,
    reverse: bool = False,
    client: "botocore.client.BaseClient" = None,
) -> t.Generator[ExecutionInfo, None, None]:
    client = _common.ensure_client(client)
    kw = time_filter.get_api_args()
    if property_filter:
        kw.update(property_filter.get_api_args())
    call = functools.partial(
        client.list_closed_workflow_executions,
        domain=domain,
        reverseOrder=reverse,
        **kw,
    )
    return _common.iter_paged(call, ExecutionInfo.from_api, "executionInfos")


def list_open_executions(
    started_filter: StartTimeExecutionFilter,
    domain: str,
    property_filter: t.Union[
        IdExecutionFilter,
        WorkflowTypeExecutionFilter,
        TagExecutionFilter,
    ] = None,
    reverse: bool = False,
    client: "botocore.client.BaseClient" = None,
) -> t.Generator[ExecutionInfo, None, None]:
    client = _common.ensure_client(client)
    kw = started_filter.get_api_args()
    if property_filter:
        kw.update(property_filter.get_api_args())
    call = functools.partial(
        client.list_open_workflow_executions, domain=domain, reverseOrder=reverse, **kw
    )
    return _common.iter_paged(call, ExecutionInfo.from_api, "executionInfos")


def request_cancel_execution(
    execution: t.Union[CurrentExecutionId, ExecutionId],
    domain: str,
    client: "botocore.client.BaseClient" = None,
) -> None:
    client = _common.ensure_client(client)
    kw = {}
    if isinstance(execution, ExecutionId):
        kw["runId"] = execution.run_id
    client.request_cancel_workflow_execution(
        domain=domain, workflowId=execution.id, **kw
    )


def signal_execution(
    execution: t.Union[CurrentExecutionId, ExecutionId],
    signal: str,
    domain: str,
    input_: str = None,
    client: "botocore.client.BaseClient" = None,
) -> None:
    client = _common.ensure_client(client)
    kw = {}
    if isinstance(execution, ExecutionId):
        kw["runId"] = execution.run_id
    if input_ or input_ == "":
        kw["input"] = input_
    client.request_cancel_workflow_execution(
        domain=domain,
        workflowId=execution.id,
        signalName=signal,
        **kw,
    )


def start_execution(
    workflow: "_workflows.WorkflowId",
    execution: CurrentExecutionId,
    domain: str,
    configuration: PartialExecutionConfiguration = None,
    tags: t.List[str] = None,
    client: "botocore.client.BaseClient" = None,
) -> ExecutionId:
    client = _common.ensure_client(client)
    configuration = configuration or PartialExecutionConfiguration()
    kw = configuration.get_api_args()
    if tags or tags == []:
        kw["tagList"] = tags
    response = client.start_workflow_execution(
        domain=domain,
        workflowId=execution.id,
        workflowType=workflow.to_api(),
        **kw,
    )
    return ExecutionId(id=execution.id, run_id=response["runId"])


def terminate_execution(
    execution: t.Union[CurrentExecutionId, ExecutionId],
    domain: str,
    reason: str = None,
    details: str = None,
    child_execution_policy: ChildExecutionTerminationPolicy = None,
    client: "botocore.client.BaseClient" = None,
) -> None:
    client = _common.ensure_client(client)
    kw = {}
    if isinstance(execution, ExecutionId):
        kw["runId"] = execution.run_id
    if reason or reason == "":
        kw["reason"] = reason
    if details or details == "":
        kw["details"] = details
    if child_execution_policy:
        kw["childPolicy"] = child_execution_policy.value
    client.terminate_workflow_execution(
        domain=domain,
        workflowId=execution.id,
        **kw,
    )
