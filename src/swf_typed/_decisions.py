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

    @abc.abstractmethod
    def to_api(self):
        return {"decisionType": self.type}


@dataclasses.dataclass
class CancelTimerDecision(Decision):
    """Cancel timer decider decision."""

    type: t.ClassVar[str] = "CancelTimer"
    timer_id: str

    def to_api(self):
        data = super().to_api()
        data["cancelTimerDecisionAttributes"] = {"timerId": self.timer_id}
        return data


@dataclasses.dataclass
class CancelWorkflowExecutionDecision(Decision):
    """Cancel workflow execution decider decision."""

    type: t.ClassVar[str] = "CancelWorkflowExecution"
    details: str = None

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
    workflow_version: str = None
    execution_configuration: "_executions.PartialExecutionConfiguration" = None
    tags: t.List[str] = None

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
    details: str = None

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
    details: str = None

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
    control: str = None

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
    task_id: str
    task_input: str = None
    control: str = None
    task_configuration: "_tasks.PartialTaskConfiguration" = None

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
    task_id: str
    task_input: str = None
    control: str = None
    task_timeout: datetime.timedelta = _common.unset

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
    signal: str
    signal_input: str = None
    control: str = None

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
    execution: "_executions.CurrentExecutionId"
    execution_input: str = None
    execution_configuration: "_executions.PartialExecutionConfiguration" = None
    control: str = None
    tags: t.List[str] = None

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
    timer_duration: datetime.timedelta
    control: str = None

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
    execution: "_executions.ExecutionId"
    workflow: "_workflows.WorkflowId"
    _execution_history_iter: t.Iterable["_history.Event"]
    decision_task_started_execution_history_event_id: int
    previous_decision_task_started_execution_history_event_id: int = None
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
        domain=domain,
        task_list=dict(name=task_list),
    )
    if response["truncated"]:
        warnings.warn("Actual task count greater than returned amount")
    return response["count"]


def poll_for_decision_task(
    task_list: str,
    domain: str,
    worker_identity: str = None,
    client: "botocore.client.BaseClient" = None,
) -> DecisionTask:
    """Request (poll for) a decision task; blocks until task is received.

    Workflow execution history events are retrieved semi-lazily.

    Args:
        task_list: decision task-list to request from
        domain: domain of task-list
        worker_identity: decider identity, recorded in execution history
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
    if worker_identity or worker_identity == "":
        kw["identity"] = worker_identity
    call = functools.partial(
        client.poll_for_activity_task,
        domain=domain,
        task_list=dict(name=task_list),
        **kw,
    )
    with _common.polling_socket_timeout():
        response = {"taskToken": ""}
        while not response["taskToken"]:
            response = call()
    return DecisionTask.from_api(response, iter_history())


def complete_decision_task(
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
