"""SWF decision task management."""

import abc
import datetime
import warnings
import functools
import dataclasses
import typing as t

from . import _common
from . import _executions

if t.TYPE_CHECKING:
    import botocore.client
    from . import _tasks
    from . import _history
    from . import _workflows
    from . import _activities


@dataclasses.dataclass
class Decision(_common.Serialisable, metaclass=abc.ABCMeta):
    """Decider decision."""

    type: t.ClassVar[str]
    """Decision type name."""

    @abc.abstractmethod
    def to_api(self):
        return {"decisionType": self.type}


@dataclasses.dataclass
class CancelTimerDecision(Decision):
    """Cancel timer decider decision."""

    type: t.ClassVar[str] = "CancelTimer"

    timer_id: str
    """Timer ID."""

    def to_api(self):
        data = super().to_api()
        data["cancelTimerDecisionAttributes"] = {"timerId": self.timer_id}
        return data


@dataclasses.dataclass
class CancelWorkflowExecutionDecision(Decision):
    """Cancel workflow execution decider decision."""

    type: t.ClassVar[str] = "CancelWorkflowExecution"

    details: str = None
    """Execution cancellation details, usually for explanation."""

    def to_api(self):
        data = super().to_api()
        if self.details or self.details == "":
            data["cancelWorkflowExecutionDecisionAttributes"] = {
                "details": self.details
            }
        return data


@dataclasses.dataclass
class CompleteWorkflowExecutionDecision(Decision):
    """Complete workflow execution decider decision."""

    type: t.ClassVar[str] = "CompleteWorkflowExecution"

    execution_result: str = None
    """Execution result."""

    def to_api(self):
        data = super().to_api()
        if self.execution_result or self.execution_result == "":
            data["completeWorkflowExecutionDecisionAttributes"] = {
                "result": self.execution_result,
            }
        return data


@dataclasses.dataclass
class ContinueAsNewWorkflowExecutionDecision(Decision):
    """Continue as new workflow execution decider decision."""

    type: t.ClassVar[str] = "ContinueAsNewWorkflowExecution"

    execution_input: str = None
    """Continuing execution input."""

    workflow_version: str = None
    """Continuing execution workflow version."""

    execution_configuration: "_executions.PartialExecutionConfiguration" = None
    """Continuing execution configuration overrides."""

    tags: t.List[str] = None
    """Continuing execution tags."""

    def to_api(self):
        data = super().to_api()
        attr_key = "continueAsNewWorkflowExecutionDecisionAttributes"

        if self.execution_input or self.execution_input == "":
            data.setdefault(attr_key, {})["input"] = self.execution_input

        if self.workflow_version or self.workflow_version == "":
            data.setdefault(attr_key, {})["workflowTypeVersion"] = self.workflow_version

        if self.execution_configuration:
            execution_configuration_data = self.execution_configuration.get_api_args()
            data.setdefault(attr_key, {}).update(execution_configuration_data)

        if self.tags or self.tags == []:
            data.setdefault(attr_key, {})["tagList"] = self.tags

        return data


@dataclasses.dataclass
class FailWorkflowExecutionDecision(Decision):
    """Fail workflow execution decider decision."""

    type: t.ClassVar[str] = "FailWorkflowExecution"

    reason: str = None
    """Execution failure reason, usually for classification."""

    details: str = None
    """Execution failure details, usually for explanation."""

    def to_api(self):
        data = super().to_api()

        if self.reason or self.details:
            data["failWorkflowExecutionDecisionAttributes"] = decision_attributes = {}
            if self.reason or self.reason == "":
                decision_attributes["reason"] = self.reason
            if self.details or self.details == "":
                decision_attributes["details"] = self.details

        return data


@dataclasses.dataclass
class RecordMarkerDecision(Decision):
    """Record marker decider decision."""

    type: t.ClassVar[str] = "RecordMarker"

    marker_name: str
    """Marker name."""

    details: str = None
    """Attached marker data."""

    def to_api(self):
        data = super().to_api()
        data["recordMarkerDecisionAttributes"] = {"markerName": self.marker_name}
        if self.details or self.details == "":
            data["recordMarkerDecisionAttributes"]["details"] = self.details
        return data


@dataclasses.dataclass
class RequestCancelActivityTaskDecision(Decision):
    """Cancel activity task request decider decision."""

    type: t.ClassVar[str] = "RequestCancelActivityTask"

    task_id: str
    """ID of task to cancel."""

    def to_api(self):
        data = super().to_api()
        data["requestCancelActivityTaskDecisionAttributes"] = {
            "activityId": self.task_id,
        }
        return data


@dataclasses.dataclass
class RequestCancelExternalWorkflowExecutionDecision(Decision):
    """Cancel external workflow execution request decider decision."""

    type: t.ClassVar[str] = "RequestCancelExternalWorkflowExecution"

    execution: t.Union["_executions.ExecutionId", "_executions.CurrentExecutionId"]
    """ID of execution to cancel."""

    control: str = None
    """Message for future deciders."""

    def to_api(self):
        data = super().to_api()
        attr_key = "requestCancelExternalWorkflowExecutionDecisionAttributes"
        data[attr_key] = self.execution.to_api()
        if self.control or self.control == "":
            data[attr_key]["control"] = self.control
        return data


@dataclasses.dataclass
class ScheduleActivityTaskDecision(Decision):
    """Schedule activity task decider decision."""

    type: t.ClassVar[str] = "ScheduleActivityTask"

    activity: "_activities.ActivityId"
    """Task activity."""

    task_id: str
    """Task ID."""

    task_input: str = None
    """Task input."""

    task_configuration: "_tasks.PartialTaskConfiguration" = None
    """Task configuration overrides."""

    control: str = None
    """Message for future deciders."""

    def to_api(self):
        data = super().to_api()
        data["scheduleActivityTaskDecisionAttributes"] = decision_attributes = {
            "activityType": self.activity.to_api(),
            "activityId": self.task_id,
        }

        if self.task_input or self.task_input == "":
            decision_attributes["input"] = self.task_input

        if self.control or self.control == "":
            decision_attributes["control"] = self.control

        if self.task_configuration:
            task_configuration_data = self.task_configuration.get_api_args()
            decision_attributes.update(task_configuration_data)

        return data


@dataclasses.dataclass
class ScheduleLambdaFunctionDecision(Decision):
    """Schedule Lambda function invocation decider decision."""

    type: t.ClassVar[str] = "ScheduleLambdaFunction"

    lambda_function: str
    """Lambda function name or ARN (latest/version/alias) to invoke."""

    task_id: str
    """Task ID."""

    task_input: str = None
    """Lambda function input."""

    task_timeout: datetime.timedelta = _common.unset
    """Lambda function invocation timeout."""

    control: str = None
    """Message for future deciders."""

    def to_api(self):
        data = super().to_api()
        data["scheduleLambdaFunctionDecisionAttributes"] = decision_attributes = {
            "lambda": self.lambda_function,
            "id": self.task_id,
        }

        if self.task_input or self.task_input == "":
            decision_attributes["input"] = self.task_input

        if self.control or self.control == "":
            decision_attributes["control"] = self.control

        if self.task_timeout or self.task_timeout == datetime.timedelta(0):
            decision_attributes["startToCloseTimeout"] = str(
                int(self.task_timeout.total_seconds())
            )

        return data


@dataclasses.dataclass
class SignalExternalWorkflowExecutionDecision(Decision):
    """Signal external workflow execution decider decision."""

    type: t.ClassVar[str] = "SignalExternalWorkflowExecution"

    execution: t.Union["_executions.ExecutionId", "_executions.CurrentExecutionId"]
    """ID of execution to signal."""

    signal: str
    """Signal name."""

    signal_input: str = None
    """Signal input."""

    control: str = None
    """Message for future deciders."""

    def to_api(self):
        data = super().to_api()
        attr_key = "signalExternalWorkflowExecutionDecisionAttributes"
        data[attr_key] = self.execution.to_api()
        data[attr_key]["signalName"] = self.signal
        if self.signal_input or self.signal_input == "":
            data[attr_key]["input"] = self.signal_input
        if self.control or self.control == "":
            data[attr_key]["control"] = self.control
        return data


@dataclasses.dataclass
class StartChildWorkflowExecutionDecision(Decision):
    """Start child workflow execution decider decision."""

    type: t.ClassVar[str] = "StartChildWorkflowExecution"

    workflow: "_workflows.WorkflowId"
    """Child execution workflow."""

    execution: "_executions.CurrentExecutionId"
    """Child execution workflow-ID."""

    execution_input: str = None
    """Child execution input."""

    execution_configuration: "_executions.PartialExecutionConfiguration" = None
    """Child execution configuration overrides."""

    tags: t.List[str] = None
    """Child execution tags"""

    control: str = None
    """Message for future deciders."""

    def to_api(self):
        data = super().to_api()
        data["startChildWorkflowExecutionDecisionAttributes"] = decision_attributes = {
            "workflowType": self.workflow.to_api(),
            "workflowId": self.execution.id,
        }

        if self.execution_input or self.execution_input == "":
            decision_attributes["input"] = self.execution_input

        if self.execution_configuration:
            decision_attributes.update(self.execution_configuration.get_api_args())

        if self.control or self.control == "":
            decision_attributes["control"] = self.control

        if self.tags or self.tags == []:
            decision_attributes["tagList"] = self.tags

        return data


@dataclasses.dataclass
class StartTimerDecision(Decision):
    """Start timer decider decision."""

    type: t.ClassVar[str] = "StartTimer"

    timer_id: str
    """Timer ID."""

    timer_duration: datetime.timedelta
    """Timer duration."""

    control: str = None
    """Message for future deciders."""

    def to_api(self):
        data = super().to_api()
        data["startTimerDecisionAttributes"] = {
            "timerId": self.timer_id,
            "startToFireTimeout": str(int(self.timer_duration.total_seconds())),
        }
        if self.control or self.control == "":
            data["startTimerDecisionAttributes"]["control"] = self.control
        return data


@dataclasses.dataclass
class DecisionTask(_common.Deserialisable):
    """Decider decision task."""

    token: str
    """Task token, provided by SWF."""

    execution: "_executions.ExecutionId"
    """Execution which decisions are being made for."""

    workflow: "_workflows.WorkflowId"
    """Execution workflow."""

    _execution_history_iter: t.Iterable["_history.Event"]

    decision_task_started_execution_history_event_id: int
    """History event ID for decision-task start."""

    previous_decision_task_started_execution_history_event_id: int = None
    """History event ID for previous decision-task start."""

    _execution_history_list: t.List["_history.Event"] = dataclasses.field(init=False)

    def __post_init__(self):
        self._execution_history_list = []

    @classmethod
    def from_api(
        cls, data, execution_history_iter: t.Iterable["_history.Event"] = None
    ) -> "DecisionTask":
        """Deserialise decision task from SWF API response data.

        Args:
            data: SWF API response decision task data
            execution_history_iter: execution history events, for lazy handling
                of paginated history. Default: get from response data
        """

        from . import _workflows
        from . import _executions

        if not execution_history_iter:
            from . import _history

            execution_history_iter = (
                _history.Event.from_api(d) for d in data["events"]
            )

        return cls(
            token=data["taskToken"],
            execution=_executions.ExecutionId.from_api(data["workflowExecution"]),
            workflow=_workflows.WorkflowId.from_api(data["workflowType"]),
            _execution_history_iter=execution_history_iter,
            decision_task_started_execution_history_event_id=data["startedEventId"],
            previous_decision_task_started_execution_history_event_id=data.get(
                "previousStartedEventId"
            ),
        )

    @property
    def execution_history_iter(self) -> t.Generator["_history.Event", None, None]:
        """Execution history events iterable."""
        for event in self._execution_history_iter:
            self._execution_history_list.append(event)
            yield event

    @property
    def execution_history(self) -> t.List["_history.Event"]:
        """Execution history events."""
        self._execution_history_list += list(self._execution_history_iter)
        return self._execution_history_list


def get_number_of_pending_decision_tasks(
    task_list: str,
    domain: str,
    client: "botocore.client.BaseClient" = None,
) -> int:
    """Get the number of pending decision tasks.

    Warns if the number of pending tasks is greater than what's returned.

    Args:
        task_list: decision task-list
        domain: domain of task-list
        client: SWF client

    Returns:
        number of pending tasks
    """

    client = _common.ensure_client(client)
    response = client.count_pending_decision_tasks(
        domain=domain, taskList=dict(name=task_list)
    )
    if response["truncated"]:
        warnings.warn("Actual task count greater than returned amount")
    return response["count"]


def request_decision_task(
    task_list: str,
    domain: str,
    decider_identity: str = None,
    no_tasks_callback: t.Callable[[], None] = None,
    client: "botocore.client.BaseClient" = None,
) -> DecisionTask:
    """Request (poll for) a decision task; blocks until task is received.

    Workflow execution history events are retrieved semi-lazily.

    Args:
        task_list: decision task-list to request from
        domain: domain of task-list
        decider_identity: decider identity, recorded in execution history
        no_tasks_callback: called after no tasks were provided by SWF
        client: SWF client

    Returns:
        decision task
    """

    from . import _history

    def iter_history() -> t.Generator["_history.Event", None, None]:
        r = response
        while r.get("nextPageToken"):
            future = _common.executor.submit(call, nextPageToken=r["nextPageToken"])
            yield from (_history.Event.from_api(d) for d in r.get("events") or [])
            r = future.result()
        yield from (_history.Event.from_api(d) for d in r.get("events") or [])

    client = _common.ensure_client(client)
    kw = {}
    if decider_identity or decider_identity == "":
        kw["identity"] = decider_identity
    call = functools.partial(
        client.poll_for_decision_task,
        domain=domain,
        taskList=dict(name=task_list),
        **kw,
    )
    with _common.polling_socket_timeout():
        while True:
            response = call()
            if response["taskToken"]:
                break
            no_tasks_callback()
    return DecisionTask.from_api(response, iter_history())


def send_decisions(
    token: str,
    decisions: t.List[Decision],
    context: str = None,
    client: "botocore.client.BaseClient" = None,
) -> None:
    """Make decisions for a workflow execution, completing decision task.

    Args:
        token: decision task identifying token
        decisions: decisions to make
        context: workflow execution context to set
        client: SWF client
    """

    client = _common.ensure_client(client)
    kw = {}
    if context or context == "":
        kw["executonContext"] = context
    decisions_data = [d.to_api() for d in decisions]
    client.respond_decision_task_completed(
        taskToken=token, decisions=decisions_data, **kw
    )
