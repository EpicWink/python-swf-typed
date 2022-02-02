"""SWF workflow execution history parsing."""

import abc
import enum
import datetime
import functools
import dataclasses
import typing as t

from . import _common

if t.TYPE_CHECKING:
    import botocore.client
    from . import _tasks
    from . import _workflows
    from . import _activities
    from . import _executions


class TimeoutType(str, enum.Enum):
    """Task/execution timeout type."""

    runtime = "START_TO_CLOSE"
    schedule = "SCHEDULE_TO_START"
    total = "SCHEDULE_TO_CLOSE"
    heartbeat = "HEARTBEAT"


class ExecutionTerminationCause(str, enum.Enum):
    """Workflow execution termination cause."""

    child_execution_policy_applied = "CHILD_POLICY_APPLIED"
    event_limit_exceeded = "EVENT_LIMIT_EXCEEDED"
    operator_initiated = "OPERATOR_INITIATED"


class DecisionFailureCause(str, enum.Enum):
    """Generic decision failure cause."""

    unhandled_decision = "UNHANDLED_DECISION"
    unauthorised = "OPERATION_NOT_PERMITTED"


class ContinueAsNewWorkflowExecutionFailureCause(str, enum.Enum):
    """Continue as new workflow execution decision failure cause."""

    unhandled_decision = "UNHANDLED_DECISION"
    workflow_deprecated = "WORKFLOW_TYPE_DEPRECATED"
    unknown_workflow = "WORKFLOW_TYPE_DOES_NOT_EXIST"
    timeout_undefined = "DEFAULT_EXECUTION_START_TO_CLOSE_TIMEOUT_UNDEFINED"
    decision_task_timeout_undefined = "DEFAULT_TASK_START_TO_CLOSE_TIMEOUT_UNDEFINED"
    task_list_undefined = "DEFAULT_TASK_LIST_UNDEFINED"
    child_execution_policy_undefined = "DEFAULT_CHILD_POLICY_UNDEFINED"
    rate_exceeded = "CONTINUE_AS_NEW_WORKFLOW_EXECUTION_RATE_EXCEEDED"
    unauthorised = "OPERATION_NOT_PERMITTED"


class SignalFailureCause(str, enum.Enum):
    """Signal workflow execution decision failure cause."""

    unknown_execution = "UNKNOWN_EXTERNAL_WORKFLOW_EXECUTION"
    rate_exceeded = "SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_RATE_EXCEEDED"
    unauthorised = "OPERATION_NOT_PERMITTED"


class CancelExecutionFailureCause(str, enum.Enum):
    """Cancel workflow execution decision failure cause."""

    unknown_execution = "UNKNOWN_EXTERNAL_WORKFLOW_EXECUTION"
    rate_exceeded = "REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_RATE_EXCEEDED"
    unauthorised = "OPERATION_NOT_PERMITTED"


class CancelTaskFailureCause(str, enum.Enum):
    """Cancel activity task decision failure cause."""

    unknown_task = "ACTIVITY_ID_UNKNOWN"
    unauthorised = "OPERATION_NOT_PERMITTED"


class CancelTimerFailureCause(str, enum.Enum):
    """Cancel timer decision failure cause."""

    unknown_timer = "TIMER_ID_UNKNOWN"
    unauthorised = "OPERATION_NOT_PERMITTED"


class StartChildExecutionFailureCause(str, enum.Enum):
    """Start child workflow execution decision failure cause."""

    unknown_workflow = "WORKFLOW_TYPE_DOES_NOT_EXIST"
    workflow_deprecated = "WORKFLOW_TYPE_DEPRECATED"
    open_child_executions_limit_exceeded = "OPEN_CHILDREN_LIMIT_EXCEEDED"
    open_executions_limit_exceeded = "OPEN_WORKFLOWS_LIMIT_EXCEEDED"
    rate_exceeded = "CHILD_CREATION_RATE_EXCEEDED"
    execution_exists = "WORKFLOW_ALREADY_RUNNING"
    timeout_undefined = "DEFAULT_EXECUTION_START_TO_CLOSE_TIMEOUT_UNDEFINED"
    task_list_undefined = "DEFAULT_TASK_LIST_UNDEFINED"
    decision_task_timeout_undefined = "DEFAULT_TASK_START_TO_CLOSE_TIMEOUT_UNDEFINED"
    child_execution_policy_undefined = "DEFAULT_CHILD_POLICY_UNDEFINED"
    unauthorised = "OPERATION_NOT_PERMITTED"


class ScheduleTaskFailureCause(str, enum.Enum):
    """Schedule activity task decision failure cause."""

    activity_deprecated = "ACTIVITY_TYPE_DEPRECATED"
    unknown_activity = "ACTIVITY_TYPE_DOES_NOT_EXIST"
    task_id_exists = "ACTIVITY_ID_ALREADY_IN_USE"
    open_tasks_limit_exceeded = "OPEN_ACTIVITIES_LIMIT_EXCEEDED"
    rate_exceeded = "ACTIVITY_CREATION_RATE_EXCEEDED"
    total_timeout_undefined = "DEFAULT_SCHEDULE_TO_CLOSE_TIMEOUT_UNDEFINED"
    task_list_undefined = "DEFAULT_TASK_LIST_UNDEFINED"
    scheduled_timeout_undefined = "DEFAULT_SCHEDULE_TO_START_TIMEOUT_UNDEFINED"
    runtime_timeout_undefined = "DEFAULT_START_TO_CLOSE_TIMEOUT_UNDEFINED"
    heartbeat_timeout_undefined = "DEFAULT_HEARTBEAT_TIMEOUT_UNDEFINED"
    unauthorised = "OPERATION_NOT_PERMITTED"


class ScheduleLambdaFailureCause(str, enum.Enum):
    """Schedule Lambda function invocation decision failure cause."""

    task_id_exists = "ID_ALREADY_IN_USE"
    open_tasks_limit_exceeded = "OPEN_LAMBDA_FUNCTIONS_LIMIT_EXCEEDED"
    rate_exceeded = "LAMBDA_FUNCTION_CREATION_RATE_EXCEEDED"
    lambda_service_unavailable = "LAMBDA_SERVICE_NOT_AVAILABLE_IN_REGION"


class StartLambdaFailureCause(str, enum.Enum):
    """Lambda function invocation failure cause."""

    assume_iam_role_failed = "ASSUME_ROLE_FAILED"


class StartTimerFailureCause(str, enum.Enum):
    """Start timer decision failure cause."""

    timer_in_use = "TIMER_ID_ALREADY_IN_USE"
    open_timers_limit_exceeded = "OPEN_TIMERS_LIMIT_EXCEEDED"
    rate_exceeded = "TIMER_CREATION_RATE_EXCEEDED"
    unauthorised = "OPERATION_NOT_PERMITTED"


@dataclasses.dataclass
class Event(_common.Deserialisable, metaclass=abc.ABCMeta):
    """Workflow execution history event."""

    type: t.ClassVar[str]
    """Event type."""

    _types: t.ClassVar[t.List[t.Type["Event"]]] = []

    id: int
    """Event ID."""

    occured: datetime.datetime
    """Event occured date."""

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._types.append(cls)

    @classmethod
    @abc.abstractmethod
    def from_api(cls, data) -> "Event":
        types = {c.type: c for c in cls._types}
        type_cls = types[data["eventType"]]
        return type_cls.from_api(data)


@dataclasses.dataclass
class ActivityTaskCancelRequestedEvent(Event):
    """Activity task cancellated requested workflow execution history event."""

    type: t.ClassVar[str] = "ActivityTaskCancelRequested"

    task_id: str
    """ID of task whose cancellation was requested."""

    decision_event_id: int
    """Task cancellation decision event ID."""

    @classmethod
    def from_api(cls, data):
        attrs = data["activityTaskCancelRequestedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            task_id=attrs["activityId"],
            decision_event_id=attrs["decisionTaskCompletedEventId"],
        )


@dataclasses.dataclass
class ActivityTaskCancelledEvent(Event):
    """Activity task cancelled workflow execution history event."""

    type: t.ClassVar[str] = "ActivityTaskCanceled"

    task_scheduled_event_id: int
    """Task schedule event ID."""

    task_started_event_id: int
    """Task start event ID."""

    task_cancel_requested_event_id: int = None
    """Task cancellation request event ID."""

    details: str = None
    """Cancellation details, usually for explanation."""

    @classmethod
    def from_api(cls, data):
        attrs = data["activityTaskCanceledEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            task_scheduled_event_id=attrs["scheduledEventId"],
            task_started_event_id=attrs["startedEventId"],
            task_cancel_requested_event_id=attrs.get("latestCancelRequestedEventId"),
            details=attrs.get("details"),
        )


@dataclasses.dataclass
class ActivityTaskCompletedEvent(Event):
    """Activity task completed workflow execution history event."""

    type: t.ClassVar[str] = "ActivityTaskCompleted"

    task_scheduled_event_id: int
    """Task schedule event ID."""

    task_started_event_id: int
    """Task start event ID."""

    task_result: str = None
    """Task result."""

    @classmethod
    def from_api(cls, data):
        attrs = data["activityTaskCompletedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            task_scheduled_event_id=attrs["scheduledEventId"],
            task_started_event_id=attrs["startedEventId"],
            task_result=attrs.get("result"),
        )


@dataclasses.dataclass
class ActivityTaskFailedEvent(Event):
    """Activity task failed workflow execution history event."""

    type: t.ClassVar[str] = "ActivityTaskFailed"

    task_scheduled_event_id: int
    """Task schedule event ID."""

    task_started_event_id: int
    """Task start event ID."""

    reason: str = None
    """Failure reason, usually for classification."""

    details: str = None
    """Failure details, usually for explanation."""

    @classmethod
    def from_api(cls, data):
        attrs = data["activityTaskFailedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            task_scheduled_event_id=attrs["scheduledEventId"],
            task_started_event_id=attrs["startedEventId"],
            reason=attrs.get("reason"),
            details=attrs.get("details"),
        )


@dataclasses.dataclass
class ActivityTaskScheduledEvent(Event):
    """Activity task scheduled workflow execution history event."""

    type: t.ClassVar[str] = "ActivityTaskScheduled"

    task_id: str
    """Scheduled task ID."""

    activity: "_activities.ActivityId"
    """Task activity type."""

    task_configuration: "_tasks.PartialTaskConfiguration"
    """Task configuration (task-list guaranteed)."""

    decision_event_id: int
    """Task schedule decision event ID."""

    task_input: str = None
    """Task input."""

    control: str = None
    """Message from task scheduling decider."""

    @classmethod
    def from_api(cls, data):
        from . import _tasks
        from . import _activities

        attrs = data["activityTaskScheduledEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            task_id=attrs["activityId"],
            activity=_activities.ActivityId.from_api(attrs["activityType"]),
            task_configuration=_tasks.PartialTaskConfiguration.from_api(attrs),
            decision_event_id=attrs["decisionTaskCompletedEventId"],
            task_input=attrs.get("input"),
            control=attrs.get("control"),
        )


@dataclasses.dataclass
class ActivityTaskStartedEvent(Event):
    """Activity task started workflow execution history event."""

    type: t.ClassVar[str] = "ActivityTaskStarted"

    task_scheduled_event_id: int
    """Task schedule event ID."""

    worker_identity: str = None
    """Identity of worker which acquired task."""

    @classmethod
    def from_api(cls, data):
        attrs = data["activityTaskStartedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            task_scheduled_event_id=attrs["scheduledEventId"],
            worker_identity=attrs.get("identity"),
        )


@dataclasses.dataclass
class ActivityTaskTimedOutEvent(Event):
    """Activity task timed-out workflow execution history event."""

    type: t.ClassVar[str] = "ActivityTaskTimedOut"

    timeout_type: TimeoutType
    """Task timeout type."""

    task_scheduled_event_id: int
    """Task schedule event ID."""

    task_started_event_id: int
    """Task start event ID."""

    details: str = None
    """Most recent progress message from worker (see ``send_heartbeat``)."""

    @classmethod
    def from_api(cls, data):
        attrs = data["activityTaskTimedOutEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            timeout_type=TimeoutType(attrs["timeoutType"]),
            task_scheduled_event_id=attrs["scheduledEventId"],
            task_started_event_id=attrs["startedEventId"],
            details=attrs.get("details"),
        )


@dataclasses.dataclass
class CancelTimerFailedEvent(Event):
    """Timer cancellation failed workflow execution history event."""

    type: t.ClassVar[str] = "CancelTimerFailed"

    timer_id: str
    """ID of timer whose cancellation failed."""

    cause: CancelTimerFailureCause
    """Failure cause."""

    decision_event_id: int
    """Timer cancellation decision event ID."""

    @classmethod
    def from_api(cls, data):
        attrs = data["cancelTimerFailedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            timer_id=data["timerId"],
            cause=CancelTimerFailureCause(attrs["cause"]),
            decision_event_id=attrs["decisionTaskCompletedEventId"],
        )


@dataclasses.dataclass
class CancelWorkflowExecutionFailedEvent(Event):
    """Workflow execution cancellation failed workflow execution history
    event.
    """

    type: t.ClassVar[str] = "CancelWorkflowExecutionFailed"

    cause: DecisionFailureCause
    """Failure cause."""

    decision_event_id: int
    """Execution cancellation decision event ID."""

    @classmethod
    def from_api(cls, data):
        attrs = data["cancelWorkflowExecutionFailedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            cause=DecisionFailureCause(attrs["cause"]),
            decision_event_id=attrs["decisionTaskCompletedEventId"],
        )


@dataclasses.dataclass
class ChildWorkflowExecutionCancelledEvent(Event):
    """Child workflow execution cancelled workflow execution history event."""

    type: t.ClassVar[str] = "ChildWorkflowExecutionCanceled"

    execution: "_executions.ExecutionId"
    """Cancelled execution."""

    workflow: "_workflows.WorkflowId"
    """Child execution workflow type."""

    initiated_event_id: int
    """Child execution start begun event ID."""

    execution_started_event_id: int
    """Child execution start event ID."""

    details: str = None
    """Cancellation details, usually for explanation."""

    @classmethod
    def from_api(cls, data):
        from . import _workflows
        from . import _executions

        attrs = data["childWorkflowExecutionCanceledEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            execution=_executions.ExecutionId.from_api(attrs["workflowExecution"]),
            workflow=_workflows.WorkflowId.from_api(attrs["workflowType"]),
            initiated_event_id=attrs["initiatedEventId"],
            execution_started_event_id=attrs["startedEventId"],
            details=attrs.get("details"),
        )


@dataclasses.dataclass
class ChildWorkflowExecutionCompletedEvent(Event):
    """Child workflow execution completed workflow execution history event."""

    type: t.ClassVar[str] = "ChildWorkflowExecutionCompleted"

    execution: "_executions.ExecutionId"
    """Completed execution."""

    workflow: "_workflows.WorkflowId"
    """Child execution workflow type."""

    initiated_event_id: int
    """Child execution start begun event ID."""

    execution_started_event_id: int
    """Child execution start event ID."""

    execution_result: str = None
    """Child execution result."""

    @classmethod
    def from_api(cls, data):
        from . import _workflows
        from . import _executions

        attrs = data["childWorkflowExecutionCompletedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            execution=_executions.ExecutionId.from_api(attrs["workflowExecution"]),
            workflow=_workflows.WorkflowId.from_api(attrs["workflowType"]),
            initiated_event_id=attrs["initiatedEventId"],
            execution_started_event_id=attrs["startedEventId"],
            execution_result=attrs.get("result"),
        )


@dataclasses.dataclass
class ChildWorkflowExecutionFailedEvent(Event):
    """Child workflow execution failed workflow execution history event."""

    type: t.ClassVar[str] = "ChildWorkflowExecutionFailed"

    execution: "_executions.ExecutionId"
    """Failed execution."""

    workflow: "_workflows.WorkflowId"
    """Child execution workflow type."""

    initiated_event_id: int
    """Child execution start begun event ID."""

    execution_started_event_id: int
    """Child execution start event ID."""

    reason: str = None
    """Failure reason, usually for classification."""

    details: str = None
    """Failure details, usually for explanation."""

    @classmethod
    def from_api(cls, data):
        from . import _workflows
        from . import _executions

        attrs = data["childWorkflowExecutionFailedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            execution=_executions.ExecutionId.from_api(attrs["workflowExecution"]),
            workflow=_workflows.WorkflowId.from_api(attrs["workflowType"]),
            initiated_event_id=attrs["initiatedEventId"],
            execution_started_event_id=attrs["startedEventId"],
            reason=attrs.get("reason"),
            details=attrs.get("details"),
        )


@dataclasses.dataclass
class ChildWorkflowExecutionStartedEvent(Event):
    """Child workflow execution started workflow execution history event."""

    type: t.ClassVar[str] = "ChildWorkflowExecutionStarted"

    execution: "_executions.ExecutionId"
    """Started execution."""

    workflow: "_workflows.WorkflowId"
    """Child execution workflow type."""

    initiated_event_id: int
    """Child execution start begun event ID."""

    @classmethod
    def from_api(cls, data):
        from . import _workflows
        from . import _executions

        attrs = data["childWorkflowExecutionStartedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            execution=_executions.ExecutionId.from_api(attrs["workflowExecution"]),
            workflow=_workflows.WorkflowId.from_api(attrs["workflowType"]),
            initiated_event_id=attrs["initiatedEventId"],
        )


@dataclasses.dataclass
class ChildWorkflowExecutionTerminatedEvent(Event):
    """Child workflow execution terminated workflow execution history event."""

    type: t.ClassVar[str] = "ChildWorkflowExecutionTerminated"

    execution: "_executions.ExecutionId"
    """Terminated execution."""

    workflow: "_workflows.WorkflowId"
    """Child execution workflow type."""

    initiated_event_id: int
    """Child execution start begun event ID."""

    execution_started_event_id: int
    """Child execution start event ID."""

    @classmethod
    def from_api(cls, data):
        from . import _workflows
        from . import _executions

        attrs = data["childWorkflowExecutionTerminatedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            execution=_executions.ExecutionId.from_api(attrs["workflowExecution"]),
            workflow=_workflows.WorkflowId.from_api(attrs["workflowType"]),
            initiated_event_id=attrs["initiatedEventId"],
            execution_started_event_id=attrs["startedEventId"],
        )


@dataclasses.dataclass
class ChildWorkflowExecutionTimedOutEvent(Event):
    """Child workflow execution timed-out workflow execution history event."""

    type: t.ClassVar[str] = "ChildWorkflowExecutionTimedOut"

    execution: "_executions.ExecutionId"
    """Timed-out execution."""

    workflow: "_workflows.WorkflowId"
    """Child execution workflow type."""

    initiated_event_id: int
    """Child execution start begun event ID."""

    execution_started_event_id: int
    """Child execution start event ID."""

    @classmethod
    def from_api(cls, data):
        from . import _workflows
        from . import _executions

        attrs = data["childWorkflowExecutionTimedOutEventAttributes"]
        assert attrs["timeoutType"] == "START_TO_CLOSE"
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            execution=_executions.ExecutionId.from_api(attrs["workflowExecution"]),
            workflow=_workflows.WorkflowId.from_api(attrs["workflowType"]),
            initiated_event_id=attrs["initiatedEventId"],
            execution_started_event_id=attrs["startedEventId"],
        )

    @property
    def timeout_type(self) -> TimeoutType:
        """Timeout type."""
        return TimeoutType.runtime


@dataclasses.dataclass
class CompleteWorkflowExecutionFailedEvent(Event):
    """Workflow execution completion failed workflow execution history event."""

    type: t.ClassVar[str] = "CompleteWorkflowExecutionFailed"

    cause: DecisionFailureCause
    """Failure cause."""

    decision_event_id: int
    """Execution complete decision event ID."""

    @classmethod
    def from_api(cls, data):
        attrs = data["completeWorkflowExecutionFailedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            cause=DecisionFailureCause(attrs["cause"]),
            decision_event_id=attrs["decisionTaskCompletedEventId"],
        )


@dataclasses.dataclass
class ContinueAsNewWorkflowExecutionFailedEvent(Event):
    """Workflow execution continue-as-new failed workflow execution history
    event.
    """

    type: t.ClassVar[str] = "ContinueAsNewWorkflowExecutionFailed"

    cause: ContinueAsNewWorkflowExecutionFailureCause
    """Failure cause."""

    decision_event_id: int
    """Continue as new execution decision event ID."""

    @classmethod
    def from_api(cls, data):
        attrs = data["continueAsNewWorkflowExecutionFailedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            cause=ContinueAsNewWorkflowExecutionFailureCause(attrs["cause"]),
            decision_event_id=attrs["decisionTaskCompletedEventId"],
        )


@dataclasses.dataclass
class DecisionTaskCompletedEvent(Event):
    """Decision task completed workflow execution history event."""

    type: t.ClassVar[str] = "DecisionTaskCompleted"

    decision_task_scheduled_event_id: int
    """Decision task schedule event ID."""

    decision_task_started_event_id: int
    """Decision task start event ID."""

    decision_context: str = None
    """Context provided by decider, accessible when describing execution."""

    @classmethod
    def from_api(cls, data):
        attrs = data["decisionTaskCompletedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            decision_task_scheduled_event_id=attrs["scheduledEventId"],
            decision_task_started_event_id=attrs["startedEventId"],
            decision_context=attrs.get("executionContext"),
        )


@dataclasses.dataclass
class DecisionTaskScheduledEvent(Event):
    """Decision task scheduled workflow execution history event."""

    type: t.ClassVar[str] = "DecisionTaskScheduled"

    decision_task_list: str
    """Task-list which decision task was placed on."""

    decision_task_timeout: t.Union[datetime.timedelta, None] = _common.unset
    """Decision runtime timeout."""

    decision_task_priority: int = None
    """Decision task priority."""

    @classmethod
    def from_api(cls, data):
        attrs = data["decisionTaskScheduledEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            decision_task_list=attrs["taskList"]["name"],
            decision_task_timeout=(
                attrs.get("startToCloseTimeout", _common.unset) and
                _common.parse_timeout(attrs["startToCloseTimeout"])
            ),
            decision_task_priority=attrs.get("taskPriority"),
        )


@dataclasses.dataclass
class DecisionTaskStartedEvent(Event):
    """Decision task started workflow execution history event."""

    type: t.ClassVar[str] = "DecisionTaskStarted"

    decision_task_scheduled_event_id: int
    """Decision task schedule event ID."""

    decider_identity: str = None
    """Identity of decider which acquired task."""

    @classmethod
    def from_api(cls, data):
        attrs = data["decisionTaskStartedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            decision_task_scheduled_event_id=attrs["scheduledEventId"],
            decider_identity=attrs.get("identity"),
        )


@dataclasses.dataclass
class DecisionTaskTimedOutEvent(Event):
    """Decision task timed-out workflow execution history event."""

    type: t.ClassVar[str] = "DecisionTaskTimedOut"

    decision_task_scheduled_event_id: int
    """Decision task schedule event ID."""

    decision_task_started_event_id: int
    """Decision task start event ID."""

    @classmethod
    def from_api(cls, data):
        attrs = data["decisionTaskTimedOutEventAttributes"]
        assert attrs["timeoutType"] == "START_TO_CLOSE"
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            decision_task_scheduled_event_id=attrs["scheduledEventId"],
            decision_task_started_event_id=attrs["startedEventId"],
        )

    @property
    def timeout_type(self) -> TimeoutType:
        """Timeout type."""
        return TimeoutType.runtime


@dataclasses.dataclass
class ExternalWorkflowExecutionCancelRequestedEvent(Event):
    """External workflow execution cancellation requested workflow execution
    history event.
    """

    type: t.ClassVar[str] = "ExternalWorkflowExecutionCancelRequested"

    execution: "_executions.ExecutionId"
    """Execution whose cancellation is requested."""

    initiated_event_id: int
    """Execution cancellation begun event ID."""

    @classmethod
    def from_api(cls, data):
        from . import _executions

        attrs = data["externalWorkflowExecutionCancelRequestedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            execution=_executions.ExecutionId.from_api(attrs["workflowExecution"]),
            initiated_event_id=attrs["initiatedEventId"],
        )


@dataclasses.dataclass
class ExternalWorkflowExecutionSignaledEvent(Event):
    """External workflow execution signaled workflow execution history event."""

    type: t.ClassVar[str] = "ExternalWorkflowExecutionSignaled"

    execution: "_executions.ExecutionId"
    """Signalled execution."""

    initiated_event_id: int
    """External workflow signal begun event ID."""

    @classmethod
    def from_api(cls, data):
        from . import _executions

        attrs = data["externalWorkflowExecutionSignaledEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            execution=_executions.ExecutionId.from_api(attrs["workflowExecution"]),
            initiated_event_id=attrs["initiatedEventId"],
        )


@dataclasses.dataclass
class FailWorkflowExecutionFailedEvent(Event):
    """Workflow execution failure failed workflow execution history event."""

    type: t.ClassVar[str] = "FailWorkflowExecutionFailed"

    cause: DecisionFailureCause
    """Failure cause."""

    decision_event_id: int
    """Workflow fail decision event ID."""

    @classmethod
    def from_api(cls, data):
        attrs = data["failWorkflowExecutionFailedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            cause=DecisionFailureCause(attrs["cause"]),
            decision_event_id=attrs["decisionTaskCompletedEventId"],
        )


@dataclasses.dataclass
class MarkerRecordedEvent(Event):
    """Marker recorded workflow execution history event."""

    type: t.ClassVar[str] = "MarkerRecorded"

    marker_name: str
    """Marker name."""

    decision_event_id: int
    """Marker record decision event ID."""

    details: str = None
    """Attached marker data."""

    @classmethod
    def from_api(cls, data):
        attrs = data["markerRecordedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            marker_name=attrs["markerName"],
            decision_event_id=attrs["decisionTaskCompletedEventId"],
            details=attrs.get("details"),
        )


@dataclasses.dataclass
class LambdaFunctionScheduledEvent(Event):
    """Lambda task scheduled workflow execution history event."""

    type: t.ClassVar[str] = "LambdaFunctionScheduled"

    task_id: str
    """Scheduled task ID."""

    lambda_function: str
    """Name or ARN (latest/version/alias) of Lambda function to be invoked."""

    decision_event_id: int
    """Task schedule event ID."""

    task_input: str = None
    """Lambda function input."""

    task_timeout: datetime.timedelta = _common.unset
    """Lambda function invocation timeout."""

    control: str = None
    """Message from task scheduling decider."""

    @classmethod
    def from_api(cls, data):
        attrs = data["lambdaFunctionScheduledEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            task_id=attrs["id"],
            lambda_function=attrs["name"],
            decision_event_id=attrs["decisionTaskCompletedEventId"],
            task_input=attrs.get("input"),
            task_timeout=(
                attrs.get("startToCloseTimeout", _common.unset) and
                _common.parse_timeout(attrs["startToCloseTimeout"])
            ),
        )


@dataclasses.dataclass
class LambdaFunctionStartedEvent(Event):
    """Lambda task started workflow execution history event."""

    type: t.ClassVar[str] = "LambdaFunctionStarted"

    task_scheduled_event_id: int
    """Task schedule event ID."""

    @classmethod
    def from_api(cls, data):
        attrs = data["lambdaFunctionStartedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            task_scheduled_event_id=attrs["scheduledEventId"],
        )


@dataclasses.dataclass
class LambdaFunctionCompletedEvent(Event):
    """Lambda task completed workflow execution history event."""

    type: t.ClassVar[str] = "LambdaFunctionCompleted"

    task_scheduled_event_id: int
    """Task schedule event ID."""

    task_started_event_id: int
    """Task start event ID."""

    task_result: str = None
    """Lambda function invocation result."""

    @classmethod
    def from_api(cls, data):
        attrs = data["lambdaFunctionCompletedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            task_scheduled_event_id=attrs["scheduledEventId"],
            task_started_event_id=attrs["startedEventId"],
            task_result=attrs.get("result"),
        )


@dataclasses.dataclass
class LambdaFunctionFailedEvent(Event):
    """Lambda task failed workflow execution history event."""

    type: t.ClassVar[str] = "LambdaFunctionFailed"

    task_scheduled_event_id: int
    """Task schedule event ID."""

    task_started_event_id: int
    """Task start event ID."""

    reason: str = None
    """Failure reason, usually for classification."""

    details: str = None
    """Failure details, usually for explanation."""

    @classmethod
    def from_api(cls, data):
        attrs = data["lambdaFunctionFailedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            task_scheduled_event_id=attrs["scheduledEventId"],
            task_started_event_id=attrs["startedEventId"],
            reason=attrs.get("reason"),
            details=attrs.get("details"),
        )


@dataclasses.dataclass
class LambdaFunctionTimedOutEvent(Event):
    """Lambda task timed-out workflow execution history event."""

    type: t.ClassVar[str] = "LambdaFunctionTimedOut"

    task_scheduled_event_id: int
    """Task schedule event ID."""

    task_started_event_id: int
    """Task start event ID."""

    @classmethod
    def from_api(cls, data):
        attrs = data["lambdaFunctionTimedOutEventAttributes"]
        assert attrs["timeoutType"] == "START_TO_CLOSE"
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            task_scheduled_event_id=attrs["scheduledEventId"],
            task_started_event_id=attrs["startedEventId"],
        )

    @property
    def timeout_type(self) -> TimeoutType:
        """Timeout type."""
        return TimeoutType.runtime


@dataclasses.dataclass
class RecordMarkerFailedEvent(Event):
    """Marker recording failed workflow execution history event."""

    type: t.ClassVar[str] = "RecordMarkerFailed"

    marker_name: str
    """Marker name."""

    decision_event_id: int
    """Marker record decision event ID."""

    @classmethod
    def from_api(cls, data):
        attrs = data["recordMarkerFailedEventAttributes"]
        assert attrs["cause"] == "OPERATION_NOT_PERMITTED"
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            marker_name=attrs["markerName"],
            decision_event_id=attrs["decisionTaskCompletedEventId"],
        )

    @property
    def cause(self) -> DecisionFailureCause:
        """Failure cause."""
        return DecisionFailureCause.unauthorised


@dataclasses.dataclass
class RequestCancelActivityTaskFailedEvent(Event):
    """Activity task cancellation request failed workflow execution history
    event.
    """

    type: t.ClassVar[str] = "RequestCancelActivityTaskFailed"

    task_id: str
    """ID of task whose cancellation was requested."""

    cause: CancelTaskFailureCause
    """Failure cause."""

    decision_event_id: int
    """Task cancellation request decision event ID."""

    @classmethod
    def from_api(cls, data):
        attrs = data["requestCancelActivityTaskFailedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            task_id=attrs["activityId"],
            cause=CancelTaskFailureCause(attrs["cause"]),
            decision_event_id=attrs["decisionTaskCompletedEventId"],
        )


@dataclasses.dataclass
class RequestCancelExternalWorkflowExecutionFailedEvent(Event):
    """External workflow execution cancellation request failed workflow
    execution history event.
    """

    type: t.ClassVar[str] = "RequestCancelExternalWorkflowExecutionFailed"

    execution: "_executions.ExecutionId"
    """Execution whose cancellation was requested."""

    cause: CancelExecutionFailureCause
    """Failure cause."""

    initiated_event_id: int
    """Cancellation request begun event ID."""

    decision_event_id: int
    """Cancellation request decision event ID."""

    control: str = None
    """Message from cancellation requesting decider."""

    @classmethod
    def from_api(cls, data):
        attrs = data["requestCancelExternalWorkflowExecutionFailedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            execution=_get_execution_from_partial_id(data),
            cause=CancelExecutionFailureCause(attrs["cause"]),
            initiated_event_id=data["initiatedEventId"],
            decision_event_id=attrs["decisionTaskCompletedEventId"],
            control=attrs.get("control"),
        )


@dataclasses.dataclass
class RequestCancelExternalWorkflowExecutionInitiatedEvent(Event):
    """External workflow execution cancellation request initiation workflow
    execution history event.
    """

    type: t.ClassVar[str] = "RequestCancelExternalWorkflowExecutionInitiated"

    execution: t.Union["_executions.ExecutionId", "_executions.CurrentExecutionId"]
    """Execution whose cancellation was requested."""

    decision_event_id: int
    """Cancellation request decision event ID."""

    control: str = None
    """Message from cancellation requesting decider."""

    @classmethod
    def from_api(cls, data):
        attrs = data["requestCancelExternalWorkflowExecutionInitiatedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            execution=_get_execution_from_partial_id(data),
            decision_event_id=attrs["decisionTaskCompletedEventId"],
            control=attrs.get("control"),
        )


@dataclasses.dataclass
class ScheduleActivityTaskFailedEvent(Event):
    """Activity task scheduling failed workflow execution history event."""

    type: t.ClassVar[str] = "ScheduleActivityTaskFailed"

    task_id: str
    """ID of task to be scheduled."""

    activity: "_activities.ActivityId"
    """Task activity type."""

    cause: ScheduleTaskFailureCause
    """Failure cause."""

    decision_event_id: int
    """Task schedule decision event ID."""

    @classmethod
    def from_api(cls, data):
        from . import _activities

        attrs = data["scheduleActivityTaskFailedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            task_id=attrs["activityId"],
            activity=_activities.ActivityId.from_api(attrs["activityType"]),
            cause=ScheduleTaskFailureCause(attrs["cause"]),
            decision_event_id=attrs["decisionTaskCompletedEventId"],
        )


@dataclasses.dataclass
class ScheduleLambdaFunctionFailedEvent(Event):
    """Lambda task scheduling failed workflow execution history event."""

    type: t.ClassVar[str] = "ScheduleLambdaFunctionFailed"

    task_id: str
    """ID of task to be scheduled."""

    lambda_function: str
    """Name or ARN (latest/version/alias) of Lambda function to be invoked."""

    cause: ScheduleLambdaFailureCause
    """Failure cause."""

    decision_event_id: int
    """Task schedule decision event ID."""

    @classmethod
    def from_api(cls, data):
        attrs = data["scheduleLambdaFunctionFailedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            task_id=attrs["id"],
            lambda_function=attrs["name"],
            cause=ScheduleLambdaFailureCause(attrs["cause"]),
            decision_event_id=attrs["decisionTaskCompletedEventId"],
        )


@dataclasses.dataclass
class SignalExternalWorkflowExecutionFailedEvent(Event):
    """External workflow execution signalling failed workflow execution
    history event.
    """

    type: t.ClassVar[str] = "SignalExternalWorkflowExecutionFailed"

    execution: t.Union["_executions.ExecutionId", "_executions.CurrentExecutionId"]
    """Execution to be signalled."""

    cause: SignalFailureCause
    """Failure cause."""

    initiated_event_id: int
    """External workflow signal begun event ID."""

    decision_event_id: int
    """External workflow signal decision event ID."""

    control: str = None
    """Message from signalling decider."""

    @classmethod
    def from_api(cls, data):
        attrs = data["signalExternalWorkflowExecutionFailedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            execution=_get_execution_from_partial_id(data),
            cause=SignalFailureCause(attrs["cause"]),
            initiated_event_id=attrs["initiatedEventId"],
            decision_event_id=attrs["decisionTaskCompletedEventId"],
            control=attrs.get("control"),
        )


@dataclasses.dataclass
class SignalExternalWorkflowExecutionInitiatedEvent(Event):
    """External workflow execution signalling initiated workflow execution
    history event.
    """

    type: t.ClassVar[str] = "SignalExternalWorkflowExecutionInitiated"

    execution: t.Union["_executions.ExecutionId", "_executions.CurrentExecutionId"]
    """Execution to be signalled."""

    signal_name: str
    """Signal name."""

    decision_event_id: int
    """External workflow signal decision event ID."""

    signal_input: str = None
    """Attached signal data."""

    control: str = None
    """Message from signalling decider."""

    @classmethod
    def from_api(cls, data):
        attrs = data["signalExternalWorkflowExecutionInitiatedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            execution=_get_execution_from_partial_id(data),
            signal_name=attrs["signalName"],
            decision_event_id=attrs["decisionTaskCompletedEventId"],
            signal_input=attrs.get("input"),
            control=attrs.get("control"),
        )


@dataclasses.dataclass
class StartActivityTaskFailedEvent(Event):
    """Activity task starting failed workflow execution history event."""

    type: t.ClassVar[str] = "StartActivityTaskFailed"

    task_id: str = None
    """ID of task to be started."""

    cause: str = None
    """Failure cause."""

    task_scheduled_event_id: int = None
    """Task schedule decision event ID."""

    @classmethod
    def from_api(cls, data):
        # Undocumented attributes (event likely never occurs)
        attrs = data.get("startActivityTaskFailedEventAttributes") or {}
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            task_id=attrs.get("activityId"),
            cause=attrs.get("cause"),
            task_scheduled_event_id=attrs.get("scheduledEventId"),
        )


@dataclasses.dataclass
class StartLambdaFunctionFailedEvent(Event):
    """Lambda task starting failed workflow execution history event."""

    type: t.ClassVar[str] = "StartLambdaFunctionFailed"

    cause: StartLambdaFailureCause = None
    """Failure cause."""

    message: str = None
    """Failure explanation."""

    task_scheduled_event_id: int = None
    """Task schedule decision event ID."""

    @classmethod
    def from_api(cls, data):
        attrs = data["startLambdaFunctionFailedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            cause=attrs.get("cause") and StartLambdaFailureCause(attrs.get("cause")),
            message=attrs.get("message"),
            task_scheduled_event_id=attrs.get("scheduledEventId"),
        )


@dataclasses.dataclass
class StartChildWorkflowExecutionFailedEvent(Event):
    """Child workflow execution starting failed workflow execution history
    event.
    """

    type: t.ClassVar[str] = "StartChildWorkflowExecutionFailed"

    execution: "_executions.CurrentExecutionId"
    """Execution to be started."""

    workflow: "_workflows.WorkflowId"
    """Child execution workflow type."""

    cause: StartChildExecutionFailureCause
    """Failure cause."""

    initiated_event_id: int
    """Child execution start begun event ID."""

    decision_event_id: int
    """Child workflow start decision event ID."""

    control: str = None
    """Message from child execution starting decider."""

    @classmethod
    def from_api(cls, data):
        from . import _workflows
        from . import _executions

        attrs = data["startChildWorkflowExecutionFailedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            execution=_executions.CurrentExecutionId.from_api(attrs),
            workflow=_workflows.WorkflowId.from_api(attrs["workflowType"]),
            cause=StartChildExecutionFailureCause(attrs["cause"]),
            initiated_event_id=attrs["initiatedEventId"],
            decision_event_id=attrs["decisionTaskCompletedEventId"],
            control=attrs.get("control"),
        )


@dataclasses.dataclass
class StartChildWorkflowExecutionInitiatedEvent(Event):
    """Child workflow execution starting initiated workflow execution history
    event.
    """

    type: t.ClassVar[str] = "StartChildWorkflowExecutionInitiated"

    execution: "_executions.CurrentExecutionId"
    """Execution to be started."""

    workflow: "_workflows.WorkflowId"
    """Child execution workflow type."""

    execution_configuration: "_executions.PartialExecutionConfiguration"
    """Child execution configuration (termination policy and decision
    task-list guaranteed).
    """

    decision_event_id: int
    """Child execution start decision event ID."""

    execution_input: str = None
    """Child execution input."""

    execution_tags: t.List[str] = None
    """Child execution tags."""

    control: str = None
    """Message from child execution starting decider."""

    @classmethod
    def from_api(cls, data):
        from . import _workflows
        from . import _executions

        attrs = data["startChildWorkflowExecutionInitiatedEventAttributes"]
        config = _executions.PartialExecutionConfiguration.from_api(attrs)
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            execution=_executions.CurrentExecutionId.from_api(attrs),
            workflow=_workflows.WorkflowId.from_api(attrs["workflowType"]),
            execution_configuration=config,
            decision_event_id=attrs["decisionTaskCompletedEventId"],
            execution_input=attrs.get("input"),
            execution_tags=attrs.get("tagList"),
            control=attrs.get("control"),
        )


@dataclasses.dataclass
class StartTimerFailedEvent(Event):
    """Timer starting failed workflow execution history event."""

    type: t.ClassVar[str] = "StartTimerFailed"

    timer_id: str
    """Timer ID."""

    cause: StartTimerFailureCause
    """Failure cause."""

    decision_event_id: int
    """Timer start decision event ID."""

    @classmethod
    def from_api(cls, data):
        attrs = data["startTimerFailedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            timer_id=attrs["timerId"],
            cause=StartTimerFailureCause(attrs["cause"]),
            decision_event_id=attrs["decisionTaskCompletedEventId"],
        )


@dataclasses.dataclass
class TimerCancelledEvent(Event):
    """Timer cancelled workflow execution history event."""

    type: t.ClassVar[str] = "TimerCanceled"

    timer_id: str
    """Cancelled timer ID."""

    timer_started_event_id: int
    """Timer start event ID."""

    decision_event_id: int
    """Timer start decision event ID."""

    @classmethod
    def from_api(cls, data):
        attrs = data["timerCanceledEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            timer_id=attrs["timerId"],
            timer_started_event_id=attrs["startedEventId"],
            decision_event_id=attrs["decisionTaskCompletedEventId"],
        )


@dataclasses.dataclass
class TimerFiredEvent(Event):
    """Timer fired workflow execution history event."""

    type: t.ClassVar[str] = "TimerFired"

    timer_id: str
    """Fired timer ID."""

    timer_started_event_id: int
    """Timer start event ID."""

    @classmethod
    def from_api(cls, data):
        attrs = data["timerFiredEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            timer_id=attrs["timerId"],
            timer_started_event_id=attrs["startedEventId"],
        )


@dataclasses.dataclass
class TimerStartedEvent(Event):
    """Timer started workflow execution history event."""

    type: t.ClassVar[str] = "TimerStarted"

    timer_id: str
    """Started timer ID."""

    timer_duration: datetime.timedelta
    """Timer duration."""

    decision_event_id: int
    """Timer start decision event ID."""

    control: str = None
    """Message from timer starting decider."""

    @classmethod
    def from_api(cls, data):
        attrs = data["timerStartedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            timer_id=attrs["timerId"],
            timer_duration=datetime.timedelta(seconds=int(attrs["startToFireTimeout"])),
            decision_event_id=attrs["decisionTaskCompletedEventId"],
            control=attrs.get("control"),
        )


@dataclasses.dataclass
class WorkflowExecutionCancelRequestedEvent(Event):
    """Workflow execution cancellation requested workflow execution history
    event.
    """

    type: t.ClassVar[str] = "WorkflowExecutionCancelRequested"

    cause: ExecutionTerminationCause = None
    """Cancellation request cause."""

    cancelling_execution: "_executions.ExecutionId" = None
    """Execution which requested the cancellation."""

    cancel_decision_event_id: int = None
    """Cancellation request decision event ID in execution which requested the
    cancellation.
    """

    @classmethod
    def from_api(cls, data):
        from . import _executions

        attrs = data["workflowExecutionCancelRequestedEventAttributes"]
        assert not attrs.get("cause") or attrs["cause"] == "CHILD_POLICY_APPLIED"
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            cause=attrs.get("cause") and ExecutionTerminationCause(attrs["cause"]),
            cancelling_execution=(
                attrs.get("externalWorkflowExecution")
                and _executions.ExecutionId.from_api(attrs["externalWorkflowExecution"])
            ),
            cancel_decision_event_id=attrs.get("externalInitiatedEventId"),
        )


@dataclasses.dataclass
class WorkflowExecutionCancelledEvent(Event):
    """Workflow execution cancelled workflow execution history event."""

    type: t.ClassVar[str] = "WorkflowExecutionCanceled"

    decision_event_id: int
    """Execution cancel decision event ID."""

    details: str = None
    """Execution cancellation details, usually for explanation."""

    @classmethod
    def from_api(cls, data):
        attrs = data["workflowExecutionCanceledEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            decision_event_id=attrs["decisionTaskCompletedEventId"],
            details=attrs.get("details"),
        )


@dataclasses.dataclass
class WorkflowExecutionCompletedEvent(Event):
    """Workflow execution completed workflow execution history event."""

    type: t.ClassVar[str] = "WorkflowExecutionCompleted"

    decision_event_id: int
    """Execution complete decision event ID."""

    execution_result: str = None
    """Execution result."""

    @classmethod
    def from_api(cls, data):
        attrs = data["workflowExecutionCompletedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            decision_event_id=attrs["decisionTaskCompletedEventId"],
            execution_result=attrs.get("result"),
        )


@dataclasses.dataclass
class WorkflowExecutionContinuedAsNewEvent(Event):
    """Workflow execution continued as new workflow execution history event."""

    type: t.ClassVar[str] = "WorkflowExecutionContinuedAsNew"

    execution_run_id: str
    """New execution run ID."""

    execution_configuration: "_executions.PartialExecutionConfiguration"
    """New execution configuration (termination policy and decision task-list
    guaranteed).
    """

    workflow: "_workflows.WorkflowId"
    """New execution workflow type."""

    decision_event_id: int
    """Continue as new execution decision event ID."""

    execution_input: str = None
    """New execution input."""

    execution_tags: t.List[str] = None
    """New execution tags."""

    @classmethod
    def from_api(cls, data):
        from . import _workflows
        from . import _executions

        attrs = data["workflowExecutionContinuedAsNewEventAttributes"]
        config = _executions.PartialExecutionConfiguration.from_api(attrs)
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            execution_run_id=attrs["newExecutionRunId"],
            execution_configuration=config,
            workflow=_workflows.WorkflowId.from_api(attrs["workflowType"]),
            decision_event_id=attrs["decisionTaskCompletedEventId"],
            execution_input=attrs.get("input"),
            execution_tags=attrs.get("tagList"),
        )


@dataclasses.dataclass
class WorkflowExecutionFailedEvent(Event):
    """Workflow execution failed workflow execution history event."""

    type: t.ClassVar[str] = "WorkflowExecutionFailed"

    decision_event_id: int
    """Execution fail decision event ID."""

    reason: str = None
    """Failure reason, usually for classification."""

    details: str = None
    """Failure details, usually for explanation."""

    @classmethod
    def from_api(cls, data):
        attrs = data["workflowExecutionFailedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            decision_event_id=attrs["decisionTaskCompletedEventId"],
            reason=attrs.get("reason"),
            details=attrs.get("details"),
        )


@dataclasses.dataclass
class WorkflowExecutionSignaledEvent(Event):
    """Workflow execution signaled workflow execution history event."""

    type: t.ClassVar[str] = "WorkflowExecutionSignaled"

    signal_name: str
    """Signal name."""

    signal_input: str = None
    """Attached signal data."""

    signalling_execution: "_executions.ExecutionId" = None
    """Execution which sent the signal."""

    signal_decision_event_id: int = None
    """Signal decision event ID in execution which sent the signal."""

    @classmethod
    def from_api(cls, data):
        attrs = data["workflowExecutionSignaledEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            signal_name=attrs["signalName"],
            signal_input=attrs.get("input"),
            signalling_execution=(
                attrs.get("externalWorkflowExecution")
                and _executions.ExecutionId.from_api(attrs["externalWorkflowExecution"])
            ),
            signal_decision_event_id=attrs.get("externalInitiatedEventId"),
        )


@dataclasses.dataclass
class WorkflowExecutionStartedEvent(Event):
    """Workflow execution started workflow execution history event."""

    type: t.ClassVar[str] = "WorkflowExecutionStarted"

    workflow: "_workflows.WorkflowId"
    """Execution workflow type."""

    execution_configuration: "_executions.PartialExecutionConfiguration"
    """Execution configuration (termination policy and decision task-list
    guaranteed).
    """

    execution_input: str = None
    """Execution input."""

    execution_tags: t.List[str] = None
    """Execution tags."""

    continued_execution_run_id: str = None
    """Run ID of execution which this execution continues from."""

    parent_execution: "_executions.ExecutionId" = None
    """Parent execution (which started this execution)."""

    parent_initiated_event_id: int = None
    """ID of event in parent execution which begins the starting of this
    execution."""

    @classmethod
    def from_api(cls, data):
        from . import _workflows
        from . import _executions

        attrs = data["workflowExecutionStartedEventAttributes"]
        config = _executions.PartialExecutionConfiguration.from_api(attrs)
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            workflow=_workflows.WorkflowId.from_api(attrs["workflowType"]),
            execution_configuration=config,
            execution_input=attrs.get("input"),
            execution_tags=attrs.get("tagList"),
            continued_execution_run_id=attrs.get("continuedExecutionRunId"),
            parent_execution=(
                attrs.get("parentWorkflowExecution") and
                _executions.ExecutionId.from_api(attrs["parentWorkflowExecution"])
            ),
            parent_initiated_event_id=attrs.get("parentInitiatedEventId"),
        )


@dataclasses.dataclass
class WorkflowExecutionTerminatedEvent(Event):
    """Workflow execution terminated workflow execution history event."""

    type: t.ClassVar[str] = "WorkflowExecutionTerminated"

    child_execution_policy: "_executions.ChildExecutionTerminationPolicy"
    """Child execution policy (how open child executions were handled)."""

    cause: ExecutionTerminationCause = None
    """Failure cause."""

    reason: str = None
    """Termination reason, usually for classification."""

    details: str = None
    """Termination details, usually for explanation."""

    @classmethod
    def from_api(cls, data):
        from . import _executions

        attrs = data["workflowExecutionTerminatedEventAttributes"]
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            child_execution_policy=_executions.ChildExecutionTerminationPolicy(
                attrs["childPolicy"],
            ),
            cause=attrs.get("cause") and ExecutionTerminationCause(attrs["cause"]),
            reason=attrs.get("reason"),
            details=attrs.get("details"),
        )


@dataclasses.dataclass
class WorkflowExecutionTimedOutEvent(Event):
    """Workflow execution timed-out workflow execution history event."""

    type: t.ClassVar[str] = "WorkflowExecutionTimedOut"

    child_execution_policy: "_executions.ChildExecutionTerminationPolicy"
    """Child execution policy (how open child executions were handled)."""

    @classmethod
    def from_api(cls, data):
        from . import _executions

        attrs = data["workflowExecutionTimedOutEventAttributes"]
        assert attrs["timeoutType"] == "START_TO_CLOSE"
        return cls(
            id=data["eventId"],
            occured=data["eventTimestamp"],
            child_execution_policy=_executions.ChildExecutionTerminationPolicy(
                attrs["childPolicy"],
            ),
        )

    @property
    def timeout_type(self) -> TimeoutType:
        """Timeout type."""
        return TimeoutType.runtime


def _get_execution_from_partial_id(
    data: t.Dict[str, t.Any],
) -> t.Union["_executions.ExecutionId", "_executions.CurrentExecutionId"]:
    """Get workflow execution identifier from SWF API response data."""
    from . import _executions

    execution_cls = _executions.CurrentExecutionId
    if data.get("runId"):
        execution_cls = _executions.ExecutionId
    return execution_cls.from_api(data)


def get_execution_history(
    execution: "_executions.ExecutionId",
    domain: str,
    reverse: bool = False,
    client: "botocore.client.BaseClient" = None,
) -> t.Generator[Event, None, None]:
    """Get workflow execution history; retrieved semi-lazily.

    Args:
        execution: workflow execution to get history of
        domain: domain of workflow execution
        reverse: return latest events first
        client: SWF client

    Returns:
        workflow execution history events
    """

    client = _common.ensure_client(client)
    call = functools.partial(
        client.get_workflow_execution_history,
        domain=domain,
        execution=execution.to_api(),
        reverseOrder=reverse,
    )
    return _common.iter_paged(call, Event.from_api, "events")


def get_last_execution_history_event(
    execution: "_executions.ExecutionId",
    domain: str,
    client: "botocore.client.BaseClient" = None,
) -> Event:
    """Get last workflow execution history event.

    Args:
        execution: workflow execution to get history event of
        domain: domain of workflow execution
        client: SWF client

    Returns:
        most recent workflow execution history event
    """

    client = _common.ensure_client(client)
    response = client.get_workflow_execution_history(
        domain=domain,
        execution=execution.to_api(),
        reverseOrder=True,
        maximumPageSize=1,
    )
    last_event_data = response["events"][0]  # all executions have events
    return Event.from_api(last_event_data)
