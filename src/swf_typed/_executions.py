"""SWF workflow execution management."""

import abc
import enum
import typing as t
import datetime
import warnings
import functools
import dataclasses

from . import _common

if t.TYPE_CHECKING:
    import botocore.client

    from . import _workflows

default_executions_list_time_range = datetime.timedelta(days=90)


@dataclasses.dataclass
class CurrentWorkflowExecutionReference(_common.Deserialisable, _common.Serialisable):
    """Current open workflow execution specifier."""

    id: str
    """Execution workflow-ID."""

    @classmethod
    def from_api(cls, data) -> "CurrentWorkflowExecutionReference":
        return cls(id=data["workflowId"])

    def to_api(self):
        return {"workflowId": self.id}


@dataclasses.dataclass
class WorkflowExecutionReference(CurrentWorkflowExecutionReference):
    """Workflow execution identifier."""

    run_id: str
    """Execution run-ID."""

    @classmethod
    def from_api(cls, data) -> "WorkflowExecutionReference":
        return cls(id=data["workflowId"], run_id=data["runId"])

    def to_api(self):
        data = super().to_api()
        data["runId"] = self.run_id
        return data


class WorkflowExecutionStatus(enum.Enum):
    """Workflow execution status."""

    open = "OPEN"
    """Execution is in-progress."""

    started = "OPEN"
    """Execution is in-progress."""

    completed = "COMPLETED"
    """Execution has finished successfully."""

    failed = "FAILED"
    """Execution has failed."""

    cancelled = "CANCELED"
    """Execution has been cancelled."""

    terminated = "TERMINATED"
    """Execution has been terminated."""

    continued_as_new = "CONTINUED_AS_NEW"
    """Execution has been continued as a new execution."""

    timed_out = "TIMED_OUT"
    """Execution has timed out."""


@dataclasses.dataclass
class WorkflowExecutionInfo(_common.Deserialisable):
    """Workflow execution details."""

    execution: WorkflowExecutionReference
    """Execution reference."""

    workflow_type: "_workflows.WorkflowTypeReference"
    """Execution workflow."""

    started: datetime.datetime
    """Execution start-date."""

    status: WorkflowExecutionStatus
    """Execution status."""

    cancel_requested: bool
    """Execution cancellation has been requested."""

    closed: t.Union[datetime.datetime, None] = None
    """Execution end-date."""

    parent: t.Union[WorkflowExecutionReference, None] = None
    """Parent execution reference."""

    tags: t.Union[t.List[str], None] = None
    """Execution tags."""

    @classmethod
    def from_api(cls, data) -> "WorkflowExecutionInfo":
        from . import _workflows

        status_data = data["executionStatus"]
        if status_data == "CLOSED":
            status_data = data["closeStatus"]
        return cls(
            execution=WorkflowExecutionReference.from_api(data["execution"]),
            workflow_type=_workflows.WorkflowTypeReference.from_api(
                data["workflowType"],
            ),
            started=data["startTimestamp"],
            status=WorkflowExecutionStatus(status_data),
            cancel_requested=data["cancelRequested"],
            closed=data.get("closeTimestamp"),
            parent=(
                data.get("parent")
                and WorkflowExecutionReference.from_api(data["parent"])
            ),
            tags=data.get("tagList"),
        )


class ChildWorkflowExecutionTerminationPolicy(str, enum.Enum):
    """Child workflow execution ending policy on parent termination."""

    terminate = "TERMINATE"
    """Terminate child executions."""

    request_cancel = "REQUEST_CANCEL"
    """Request for child execution cancellation."""

    abandon = "ABANDON"
    """Abandon child executions."""


@dataclasses.dataclass
class WorkflowExecutionConfiguration(_common.Deserialisable):
    """Workflow execution configuration."""

    timeout: t.Union[datetime.timedelta, None]
    """Execution run-time timeout."""

    decision_task_timeout: t.Union[datetime.timedelta, None]
    """Decision task timeout."""

    decision_task_list: str
    """Decision task task-list."""

    child_execution_policy_on_termination: ChildWorkflowExecutionTerminationPolicy
    """Child workflow execution ending policy on termination."""

    decision_task_priority: t.Union[int, None] = None
    """Decision task priority."""

    lambda_iam_role_arn: t.Union[str, None] = None
    """Execution IAM role ARN for Lambda invocations."""

    @classmethod
    def from_api(cls, data) -> "WorkflowExecutionConfiguration":
        child_policy = ChildWorkflowExecutionTerminationPolicy(data["childPolicy"])
        decision_task_timeout = _common.parse_timeout(data["taskStartToCloseTimeout"])
        return cls(
            timeout=_common.parse_timeout(data["executionStartToCloseTimeout"]),
            decision_task_timeout=decision_task_timeout,
            decision_task_list=data["taskList"]["name"],
            decision_task_priority=(
                data.get("taskPriority") and int(data["taskPriority"])
            ),
            child_execution_policy_on_termination=child_policy,
            lambda_iam_role_arn=data.get("lambdaRole"),
        )


@dataclasses.dataclass
class PartialWorkflowExecutionConfiguration(
    WorkflowExecutionConfiguration, _common.SerialisableToArguments
):
    """Partial workflow execution configuration."""

    timeout: t.Union[datetime.timedelta, None] = _common.unset
    decision_task_timeout: t.Union[datetime.timedelta, None] = _common.unset
    decision_task_list: t.Union[str, None] = None
    decision_task_priority: t.Union[int, None] = None
    child_execution_policy_on_termination: t.Union[
        ChildWorkflowExecutionTerminationPolicy,
        None,
    ] = None

    @classmethod
    def from_api(cls, data) -> "PartialWorkflowExecutionConfiguration":
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
                data.get("childPolicy")
                and ChildWorkflowExecutionTerminationPolicy(data["childPolicy"])
            ),
            lambda_iam_role_arn=data.get("lambdaRole"),
        )

    def get_api_args(self):
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
class WorkflowExecutionOpenCounts:
    """Counts of workflow executions' open tasks/timers/children."""

    activity_tasks: int
    """Number of scheduled/started activity tasks."""

    decision_tasks: int
    """Number of scheduled/started decision tasks."""

    timers: int
    """Number of started timers."""

    child_executions: int
    """Number of started child executions."""

    lambda_tasks: t.Union[int, None] = None
    """Number of scheduled/started Lambda invocations."""

    @classmethod
    def from_api(cls, data) -> "WorkflowExecutionOpenCounts":
        return cls(
            data["openActivityTasks"],
            data["openDecisionTasks"],
            data["openTimers"],
            data["openChildWorkflowExecutions"],
            lambda_tasks=data.get("openLambdaFunctions"),
        )


@dataclasses.dataclass
class WorkflowExecutionDetails:
    """Workflow execution details, configuration, open-counts and snapshot."""

    info: WorkflowExecutionInfo
    """Execution details."""

    configuration: t.Union[WorkflowExecutionConfiguration, None] = None
    """Execution configuration."""

    n_open: t.Union[WorkflowExecutionOpenCounts, None] = None
    """Counts of open tasks/timers/children in execution."""

    latest_activity_task_scheduled: t.Union[datetime.datetime, None] = None
    """Most recent activity task's scheduled's date."""

    latest_context: t.Union[str, None] = None
    """Most recent decision's execution context."""

    @classmethod
    def from_api(cls, data) -> "WorkflowExecutionDetails":
        config = WorkflowExecutionConfiguration.from_api(data["executionConfiguration"])
        return cls(
            info=WorkflowExecutionInfo.from_api(data["executionInfo"]),
            configuration=config,
            n_open=WorkflowExecutionOpenCounts.from_api(data["openCounts"]),
            latest_activity_task_scheduled=data.get("latestActivityTaskTimestamp"),
            latest_context=data.get("latestExecutionContext"),
        )


@dataclasses.dataclass
class WorkflowExecutionFilter(_common.SerialisableToArguments, metaclass=abc.ABCMeta):
    """Workflow execution filter."""

    @abc.abstractmethod
    def get_api_args(self):
        pass


@dataclasses.dataclass
class DateTimeFilter(_common.Serialisable):
    """Date-time property filter mix-in."""

    earliest: datetime.datetime
    """Earliest date."""

    latest: t.Union[datetime.datetime, None] = None
    """Latest date."""

    def to_api(self):
        data = {"oldestDate": self.earliest}
        if self.latest:
            data["latestDate"] = self.latest
        return data


@dataclasses.dataclass
class StartTimeWorkflowExecutionFilter(DateTimeFilter, WorkflowExecutionFilter):
    """Workflow execution filter on start-time."""

    def get_api_args(self):
        return {"startTimeFilter": self.to_api()}


@dataclasses.dataclass
class CloseTimeWorkflowExecutionFilter(DateTimeFilter, WorkflowExecutionFilter):
    """Workflow execution filter on close-time."""

    def get_api_args(self):
        return {"closeTimeFilter": self.to_api()}


@dataclasses.dataclass
class IdWorkflowExecutionFilter(WorkflowExecutionFilter):
    """Workflow execution filter on execution workflow-ID."""

    execution: CurrentWorkflowExecutionReference
    """Execution reference."""

    def get_api_args(self):
        return {"executionFilter": self.execution.to_api()}


@dataclasses.dataclass
class WorkflowTypeWorkflowExecutionFilter(WorkflowExecutionFilter):
    """Workflow execution filter on execution workflow-type."""

    workflow_type: t.Union[
        "_workflows.WorkflowTypeReference",
        "_workflows.WorkflowTypeFilter",
    ]
    """Execution workflow."""

    def get_api_args(self):
        return {"typeFilter": self.workflow_type.to_api()}


@dataclasses.dataclass
class TagWorkflowExecutionFilter(WorkflowExecutionFilter):
    """Workflow execution filter on execution tags."""

    tag: str
    """Execution tag."""

    def get_api_args(self):
        return {"tagFilter": {"tag": self.tag}}


@dataclasses.dataclass
class CloseStatusWorkflowExecutionFilter(WorkflowExecutionFilter):
    """Workflow execution filter on execution close-status."""

    status: str
    """Execution status."""

    def get_api_args(self):
        return {"closeStatusFilter": {"status": self.status}}


TimeFilter = t.TypeVar(
    "TimeFilter", StartTimeWorkflowExecutionFilter, CloseTimeWorkflowExecutionFilter
)


def _default_time_filter(cls: t.Type[TimeFilter]) -> TimeFilter:
    """Construct a default execution time-filter."""
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    return cls(earliest=now - default_executions_list_time_range)


def _get_number_of_executions(
    time_filter: t.Union[
        StartTimeWorkflowExecutionFilter,
        CloseTimeWorkflowExecutionFilter,
    ],
    domain: str,
    property_filter: t.Union[
        IdWorkflowExecutionFilter,
        WorkflowTypeWorkflowExecutionFilter,
        TagWorkflowExecutionFilter,
        CloseStatusWorkflowExecutionFilter,
        None,
    ],
    client_method: t.Callable[..., t.Dict[str, t.Any]],
) -> int:
    """Get the number of executions matching filter."""
    kw = time_filter.get_api_args()
    if property_filter:
        kw.update(property_filter.get_api_args())
    response = client_method(domain=domain, **kw)
    if response["truncated"]:
        warnings.warn("Actual execution count greater than returned amount")
    return response["count"]


def get_number_of_closed_workflow_executions(
    domain: str,
    time_filter: t.Union[
        StartTimeWorkflowExecutionFilter,
        CloseTimeWorkflowExecutionFilter,
        None,
    ] = None,
    property_filter: t.Union[
        IdWorkflowExecutionFilter,
        WorkflowTypeWorkflowExecutionFilter,
        TagWorkflowExecutionFilter,
        CloseStatusWorkflowExecutionFilter,
        None,
    ] = None,
    client: t.Union["botocore.client.BaseClient", None] = None,
) -> int:
    """Get the number of closed workflow executions.

    Warns if the number of matching executions is greater than what's
    returned.

    Args:
        domain: domain of executions
        time_filter: execution start-time/close-time filter, default:
            executions closed less than 90 days ago
        property_filter: execution
            workflow-ID/workflow-type/tags/close-status filter
        client: SWF client

    Returns:
        number of matching workflow executions
    """

    client = _common.ensure_client(client)
    time_filter = time_filter or _default_time_filter(CloseTimeWorkflowExecutionFilter)
    return _get_number_of_executions(
        time_filter, domain, property_filter, client.count_closed_workflow_executions
    )


def get_number_of_open_workflow_executions(
    domain: str,
    started_filter: t.Union[StartTimeWorkflowExecutionFilter, None] = None,
    property_filter: t.Union[
        IdWorkflowExecutionFilter,
        WorkflowTypeWorkflowExecutionFilter,
        TagWorkflowExecutionFilter,
        None,
    ] = None,
    client: t.Union["botocore.client.BaseClient", None] = None,
) -> int:
    """Get the number of open workflow executions.

    Warns if the number of matching executions is greater than what's
    returned.

    Args:
        domain: domain of executions
        started_filter: execution start-time filter, default: executions
            opened less than 90 days ago
        property_filter: execution workflow-ID/workflow-type/tags filter
        client: SWF client

    Returns:
        number of matching workflow executions
    """

    client = _common.ensure_client(client)
    started_filter = started_filter or _default_time_filter(
        StartTimeWorkflowExecutionFilter,
    )
    return _get_number_of_executions(
        started_filter, domain, property_filter, client.count_open_workflow_executions
    )


def describe_workflow_execution(
    execution: WorkflowExecutionReference,
    domain: str,
    client: t.Union["botocore.client.BaseClient", None] = None,
) -> WorkflowExecutionDetails:
    """Describe a workflow execution.

    Args:
        execution: workflow execution to describe
        domain: domain of workflow execution
        client: SWF client

    Returns:
        workflow execution details, configuration, open-counts and snapshot
    """

    client = _common.ensure_client(client)
    response = client.describe_workflow_execution(
        domain=domain, execution=execution.to_api()
    )
    return WorkflowExecutionDetails.from_api(response)


def list_closed_workflow_executions(
    domain: str,
    time_filter: t.Union[
        StartTimeWorkflowExecutionFilter,
        CloseTimeWorkflowExecutionFilter,
        None,
    ] = None,
    property_filter: t.Union[
        IdWorkflowExecutionFilter,
        WorkflowTypeWorkflowExecutionFilter,
        TagWorkflowExecutionFilter,
        CloseStatusWorkflowExecutionFilter,
        None,
    ] = None,
    reverse: bool = False,
    client: t.Union["botocore.client.BaseClient", None] = None,
) -> _common.PageConsumer[WorkflowExecutionInfo]:
    """List closed workflow executions; retrieved semi-lazily.

    Args:
        domain: domain of executions
        time_filter: execution start-time/close-time filter, default:
            executions closed less than 90 days ago
        property_filter: execution
            workflow-ID/workflow-type/tags/close-status filter
        reverse: return results in reverse start/close order
        client: SWF client

    Returns:
        matching workflow executions
    """

    client = _common.ensure_client(client)
    time_filter = time_filter or _default_time_filter(CloseTimeWorkflowExecutionFilter)
    kw = time_filter.get_api_args()
    if property_filter:
        kw.update(property_filter.get_api_args())
    call = functools.partial(
        client.list_closed_workflow_executions,
        domain=domain,
        reverseOrder=reverse,
        **kw,
    )
    return _common.iter_paged(call, WorkflowExecutionInfo.from_api, "executionInfos")


def list_open_workflow_executions(
    domain: str,
    started_filter: t.Union[StartTimeWorkflowExecutionFilter, None] = None,
    property_filter: t.Union[
        IdWorkflowExecutionFilter,
        WorkflowTypeWorkflowExecutionFilter,
        TagWorkflowExecutionFilter,
        None,
    ] = None,
    reverse: bool = False,
    client: t.Union["botocore.client.BaseClient", None] = None,
) -> _common.PageConsumer[WorkflowExecutionInfo]:
    """List open workflow executions; retrieved semi-lazily.

    Args:
        domain: domain of executions
        started_filter: execution start-time filter, default: executions
            opened less than 90 days ago
        property_filter: execution workflow-ID/workflow-type/tags filter
        reverse: return results in reverse start order
        client: SWF client

    Returns:
        matching workflow executions
    """

    client = _common.ensure_client(client)
    started_filter = started_filter or _default_time_filter(
        StartTimeWorkflowExecutionFilter,
    )
    kw = {}
    if property_filter:
        kw.update(property_filter.get_api_args())
    call = functools.partial(
        client.list_open_workflow_executions,
        domain=domain,
        startTimeFilter=started_filter.to_api(),
        reverseOrder=reverse,
        **kw,
    )
    return _common.iter_paged(call, WorkflowExecutionInfo.from_api, "executionInfos")


def request_cancel_workflow_execution(
    execution: t.Union[CurrentWorkflowExecutionReference, WorkflowExecutionReference],
    domain: str,
    client: t.Union["botocore.client.BaseClient", None] = None,
) -> None:
    """Request the cancellation of a workflow execution.

    Args:
        execution: execution to cancel
        domain: domain of execution
        client: SWF client
    """

    client = _common.ensure_client(client)
    kw = {}
    if isinstance(execution, WorkflowExecutionReference):
        kw["runId"] = execution.run_id
    client.request_cancel_workflow_execution(
        domain=domain, workflowId=execution.id, **kw
    )


def signal_workflow_execution(
    execution: t.Union[CurrentWorkflowExecutionReference, WorkflowExecutionReference],
    signal: str,
    domain: str,
    input_: t.Union[str, None] = None,
    client: t.Union["botocore.client.BaseClient", None] = None,
) -> None:
    """Send a signal to a workflow execution.

    Args:
        execution: execution to signal
        signal: signal name
        domain: domain of execution
        input_: attached signal data
        client: SWF client
    """

    client = _common.ensure_client(client)
    kw = {}
    if isinstance(execution, WorkflowExecutionReference):
        kw["runId"] = execution.run_id
    if input_ or input_ == "":
        kw["input"] = input_
    client.request_cancel_workflow_execution(
        domain=domain,
        workflowId=execution.id,
        signalName=signal,
        **kw,
    )


def start_workflow_execution(
    workflow_type: "_workflows.WorkflowTypeReference",
    execution: CurrentWorkflowExecutionReference,
    domain: str,
    input: t.Union[str, None] = None,
    configuration: t.Union[PartialWorkflowExecutionConfiguration, None] = None,
    tags: t.Union[t.List[str], None] = None,
    client: t.Union["botocore.client.BaseClient", None] = None,
) -> WorkflowExecutionReference:
    """Start a workflow execution.

    Args:
        workflow_type: workflow type for execution
        execution: execution workflow-ID
        domain: domain for execution
        input: execution input
        configuration: execution configuration, default: use defaults for
            workflow type
        tags: execution tags
        client: SWF client

    Returns:
        workflow execution, with run-ID
    """

    client = _common.ensure_client(client)
    configuration = configuration or PartialWorkflowExecutionConfiguration()
    kw = configuration.get_api_args()
    if input or input == "":
        kw["input"] = input
    if tags or tags == []:
        kw["tagList"] = tags
    response = client.start_workflow_execution(
        domain=domain,
        workflowId=execution.id,
        workflowType=workflow_type.to_api(),
        **kw,
    )
    return WorkflowExecutionReference(id=execution.id, run_id=response["runId"])


def terminate_workflow_execution(
    execution: t.Union[CurrentWorkflowExecutionReference, WorkflowExecutionReference],
    domain: str,
    reason: t.Union[str, None] = None,
    details: t.Union[str, None] = None,
    child_execution_policy: t.Union[
        ChildWorkflowExecutionTerminationPolicy,
        None,
    ] = None,
    client: t.Union["botocore.client.BaseClient", None] = None,
) -> None:
    """Terminate (immediately close) a workflow execution.

    Args:
        execution: workflow execution to close
        domain: domain od execution
        reason: termination reason, usually for classification
        details: termination details, usually for explanation
        child_execution_policy: how to handle open child workflow
            executions, default: use default for workflow type
        client: SWF client
    """

    client = _common.ensure_client(client)
    kw = {}
    if isinstance(execution, WorkflowExecutionReference):
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
