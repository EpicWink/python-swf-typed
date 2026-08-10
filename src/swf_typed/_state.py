"""SWF workflow execution state construction."""

import enum
import datetime
import dataclasses
import typing as t

if t.TYPE_CHECKING:
    from . import _tasks
    from . import _history
    from . import _workflows
    from . import _activities
    from . import _executions


class TaskStatus(enum.Enum):
    """Activity task status."""

    scheduled = enum.auto()
    """Task has been scheduled."""

    started = enum.auto()
    """Task is running."""

    completed = enum.auto()
    """Task has finished."""

    failed = enum.auto()
    """Task has failed."""

    cancelled = enum.auto()
    """Task has been cancelled."""

    timed_out = enum.auto()
    """Task has timed out."""


class TimerStatus(enum.Enum):
    """Timer status."""

    started = enum.auto()
    """Timer has started."""

    fired = enum.auto()
    """Timer has finished."""

    cancelled = enum.auto()
    """Timer has been cancelled."""


@dataclasses.dataclass
class DecisionFailure:
    """Decision failure event."""

    event: "_history.Event"
    """History event for decision failure."""

    is_new: bool = True
    """Most recent decision failed."""

    def to_dict(self) -> t.Dict[str, t.Any]:
        from . import _common, _history

        state_dict = {
            "event": {
                "id": self.event.id,
                "type": self.event.type,
                "occured": _common.serialise_datetime(self.event.occured),
                "decisionEventId": self.event.decision_event_id,
            },
            "isNew": self.is_new,
        }  # type: t.Dict[str, t.Any]

        if isinstance(self.event, _history.RecordMarkerFailedEvent):
            state_dict["event"]["cause"] = getattr(
                self.event, "cause", "OPERATION_NOT_PERMITTED"
            )
        else:
            state_dict["event"]["cause"] = str(self.event.cause)

        return state_dict


@dataclasses.dataclass
class TaskState:
    """Activity task state."""

    id: str
    """Task ID."""

    status: TaskStatus
    """Task status."""

    activity: "_activities.ActivityId"
    """Task activity."""

    configuration: "_tasks.TaskConfiguration"
    """Task configuration."""

    scheduled: datetime.datetime
    """Task scheduled date."""

    started: datetime.datetime = None
    """Task start date."""

    ended: datetime.datetime = None
    """Task end date."""

    input: str = None
    """Task input."""

    worker_identity: str = None
    """Identity of worker which acquired task."""

    cancel_requested: bool = False
    """Task cancellation has been requested."""

    result: str = None
    """Task result."""

    timeout_type: "_history.TimeoutType" = None
    """Task timeout type."""

    failure_reason: str = None
    """Task failure reason."""

    stop_details: str = None
    """Task ended details."""

    decider_control: str = None
    """Message from decider attached to task."""

    @property
    def has_ended(self) -> bool:
        """Activity task has completed/failed/cancelled/timed-out."""
        return self.status not in (TaskStatus.scheduled, TaskStatus.started)

    def to_dict(self) -> t.Dict[str, t.Any]:
        from . import _common

        def serialise_timedelta(
            td: t.Union[datetime.timedelta, None],
        ) -> t.Union[str, None]:
            return None if td is None else _common.serialise_timedelta(td)

        state_dict = {
            "id": self.id,
            "status": self.status.value,
            "activity": {"name": self.activity.name, "version": self.activity.version},
            "configuration": {
                "taskList": self.configuration.task_list,
                "runtimeTimeout": serialise_timedelta(
                    self.configuration.runtime_timeout,
                ),
                "scheduleTimeout": serialise_timedelta(
                    self.configuration.schedule_timeout,
                ),
                "totalTimeout": serialise_timedelta(self.configuration.total_timeout),
                "priority": self.configuration.priority,
            },
            "scheduled": _common.serialise_datetime(self.scheduled),
            "cancelRequested": self.cancel_requested,
        }  # type: t.Dict[str, t.Any]

        if self.configuration.heartbeat_timeout is not _common.unset:
            state_dict["configuration"]["heartbeatTimeout"] = serialise_timedelta(
                self.configuration.heartbeat_timeout,
            )
        if self.configuration.priority is not None:
            state_dict["configuration"]["priority"] = self.configuration.priority
        if self.started is not None:
            state_dict["started"] = _common.serialise_datetime(self.started)
        if self.ended is not None:
            state_dict["ended"] = _common.serialise_datetime(self.ended)
        if self.input is not None:
            state_dict["input"] = self.input
        if self.worker_identity is not None:
            state_dict["workerIdentity"] = self.worker_identity
        if self.result is not None:
            state_dict["result"] = self.result
        if self.timeout_type is not None:
            state_dict["timeoutType"] = str(self.timeout_type)
        if self.failure_reason is not None:
            state_dict["failureReason"] = self.failure_reason
        if self.stop_details is not None:
            state_dict["stopDetails"] = self.stop_details
        if self.decider_control is not None:
            state_dict["deciderControl"] = self.decider_control

        return state_dict


@dataclasses.dataclass
class LambdaTaskState:
    """Lambda task state."""

    id: str
    """Task ID."""

    status: TaskStatus
    """Task status."""

    lambda_function: str
    """Name of Lambda function invoked for task."""

    scheduled: datetime.datetime
    """Task schedule date."""

    started: datetime.datetime = None
    """Lambda function invocation date."""

    ended: datetime.datetime = None
    """Lambda function invocation end date."""

    timeout: datetime.timedelta = None
    """Lambda function invocation timeout."""

    input: str = None
    """Lambda function input."""

    result: str = None
    """Lambda function result."""

    failure_reason: str = None
    """Lambda function error reason."""

    stop_details: str = None
    """Lambda function ended details."""

    decider_control: str = None
    """Message from decider attached to task."""

    @property
    def has_ended(self) -> bool:
        """Lambda task has completed/failed/cancelled/timed-out."""
        return self.status not in (TaskStatus.scheduled, TaskStatus.started)

    def to_dict(self) -> t.Dict[str, t.Any]:
        from . import _common

        state_dict = {
            "id": self.id,
            "status": self.status.value,
            "lambdaFunction": self.lambda_function,
            "scheduled": _common.serialise_datetime(self.scheduled),
        }  # type: t.Dict[str, t.Any]

        if self.started is not None:
            state_dict["started"] = _common.serialise_datetime(self.started)
        if self.ended is not None:
            state_dict["ended"] = _common.serialise_datetime(self.ended)
        if self.timeout is not None:
            state_dict["timeout"] = _common.serialise_timedelta(self.timeout)
        if self.input is not None:
            state_dict["input"] = self.input
        if self.result is not None:
            state_dict["result"] = self.result
        if self.failure_reason is not None:
            state_dict["failureReason"] = self.failure_reason
        if self.stop_details is not None:
            state_dict["stopDetails"] = self.stop_details
        if self.decider_control is not None:
            state_dict["deciderControl"] = self.decider_control

        return state_dict


@dataclasses.dataclass
class ChildExecutionState:
    """Child workflow execution state."""

    execution: "_executions.ExecutionId"
    """Child execution ID."""

    workflow: "_workflows.WorkflowId"
    """Child execution workflow."""

    status: "_executions.ExecutionStatus"
    """Child execution status."""

    configuration: "_executions.ExecutionConfiguration"
    """Child execution configuration."""

    started: datetime.datetime
    """Child execution start date."""

    ended: datetime.datetime = None
    """Child execution end date."""

    input: str = None
    """Child execution input."""

    result: str = None
    """Child execution result."""

    timeout_type: "_history.TimeoutType" = None
    """Child execution timeout type."""

    failure_reason: str = None
    """Child execution failure reason."""

    stop_details: str = None
    """Child execution ended details."""

    decider_control: str = None
    """Message from decider attached to child execution."""

    def to_dict(self) -> t.Dict[str, t.Any]:
        from . import _common

        def serialise_timedelta(
            td: t.Union[datetime.timedelta, None],
        ) -> t.Union[str, None]:
            return None if td is None else _common.serialise_timedelta(td)

        state_dict = {
            "execution": {"id": self.execution.id, "run_id": self.execution.run_id},
            "workflow": {"name": self.workflow.name, "version": self.workflow.version},
            "status": self.status.value,
            "configuration": {
                "timeout": serialise_timedelta(self.configuration.timeout),
                "decisionTaskTimeout": serialise_timedelta(
                    self.configuration.decision_task_timeout,
                ),
                "decisionTaskList": self.configuration.decision_task_list,
                "childExecutionPolicyOnTermination": str(
                    self.configuration.child_execution_policy_on_termination,
                ),
            },
            "started": _common.serialise_datetime(self.started),
        }  # type: t.Dict[str, t.Any]

        if self.configuration.decision_task_priority is not None:
            state_dict["configuration"]["decisionTaskPriority"] = (
                self.configuration.decision_task_priority
            )
        if self.configuration.lambda_iam_role_arn is not None:
            state_dict["configuration"]["lambdaIamRoleArn"] = (
                self.configuration.lambda_iam_role_arn
            )
        if self.ended is not None:
            state_dict["ended"] = _common.serialise_datetime(self.ended)
        if self.input is not None:
            state_dict["input"] = self.input
        if self.result is not None:
            state_dict["result"] = self.result
        if self.timeout_type is not None:
            state_dict["timeoutType"] = str(self.timeout_type)
        if self.failure_reason is not None:
            state_dict["failureReason"] = self.failure_reason
        if self.stop_details is not None:
            state_dict["stopDetails"] = self.stop_details
        if self.decider_control is not None:
            state_dict["deciderControl"] = self.decider_control

        return state_dict


@dataclasses.dataclass
class TimerState:
    """Timer state."""

    id: str
    """Timer ID."""

    status: TimerStatus
    """Timer status."""

    duraction: datetime.timedelta
    """Timer duration."""

    started: datetime.datetime
    """Timer start date."""

    ended: datetime.datetime = None
    """Timer finish date."""

    input: str = None
    """Timer input."""

    decider_control: str = None
    """Message from decider attached to timer."""

    def to_dict(self) -> t.Dict[str, t.Any]:
        from . import _common

        state_dict = {
            "id": self.id,
            "status": self.status.value,
            "duration": _common.serialise_timedelta(self.duraction),
            "started": _common.serialise_datetime(self.started),
        }  # type: t.Dict[str, t.Any]

        if self.ended is not None:
            state_dict["ended"] = _common.serialise_datetime(self.ended)
        if self.input is not None:
            state_dict["input"] = self.input
        if self.decider_control is not None:
            state_dict["deciderControl"] = self.decider_control

        return state_dict


@dataclasses.dataclass
class SignalState:
    """Signal state."""

    name: str
    """Signal name."""

    received: datetime.datetime
    """Signal date."""

    input: str = None
    """Signal input."""

    is_new: bool = True
    """Execution was signalled after most recent decision."""

    def to_dict(self) -> t.Dict[str, t.Any]:
        from . import _common

        state_dict = {
            "name": self.name,
            "received": _common.serialise_datetime(self.received),
            "isNew": self.is_new,
        }  # type: t.Dict[str, t.Any]

        if self.input is not None:
            state_dict["input"] = self.input

        return state_dict


@dataclasses.dataclass
class MarkerState:
    """Marker state."""

    name: str
    """Marker name."""

    recorded: datetime.datetime
    """Marker record date."""

    details: str = None
    """Marker details."""

    is_new: bool = True
    """Marker was recorded after most recent decision."""

    def to_dict(self) -> t.Dict[str, t.Any]:
        from . import _common

        state_dict = {
            "name": self.name,
            "recorded": _common.serialise_datetime(self.recorded),
            "isNew": self.is_new,
        }  # type: t.Dict[str, t.Any]

        if self.details is not None:
            state_dict["details"] = self.details

        return state_dict


@dataclasses.dataclass
class ExecutionState:
    """Workflow execution state."""

    workflow: "_workflows.WorkflowId"
    """Child execution workflow."""

    status: "_executions.ExecutionStatus"
    """Execution status."""

    configuration: "_executions.ExecutionConfiguration"
    """Execution configuration."""

    started: datetime.datetime
    """Execution start date."""

    ended: datetime.datetime = None
    """Execution end date."""

    tasks: t.List[t.Union[TaskState, LambdaTaskState]] = dataclasses.field(
        default_factory=list
    )
    """Execution activity and Lambda function invocation tasks."""

    child_executions: t.List[ChildExecutionState] = dataclasses.field(
        default_factory=list
    )
    """Execution child executions."""

    timers: t.List[TimerState] = dataclasses.field(default_factory=list)
    """Execution timers."""

    signals: t.List[SignalState] = dataclasses.field(default_factory=list)
    """Execution signals."""

    markers: t.List[MarkerState] = dataclasses.field(default_factory=list)
    """Execution markers."""

    decision_failures: t.List[DecisionFailure] = dataclasses.field(default_factory=list)
    """Execution decision failures."""

    input: str = None
    """Execution input."""

    cancel_requested: bool = False
    """Execution cancellation has been requested."""

    result: str = None
    """Execution result."""

    failure_reason: str = None
    """Execution failure reason."""

    stop_details: str = None
    """Execution ended details."""

    continuing_execution_run_id: str = None
    """ID of execution continuing this execution."""

    def to_dict(self) -> t.Dict[str, t.Any]:
        from . import _common

        def serialise_timedelta(
            td: t.Union[datetime.timedelta, None],
        ) -> t.Union[str, None]:
            if td is None:
                return None
            return _common.serialise_timedelta(td)

        state_dict = {
            "workflow": {"name": self.workflow.name, "version": self.workflow.version},
            "status": self.status.value,
            "configuration": {
                "decisionTaskList": self.configuration.decision_task_list,
                "childExecutionPolicyOnTermination": str(
                    self.configuration.child_execution_policy_on_termination,
                ),
            },
            "started": _common.serialise_datetime(self.started),
            "tasks": [x.to_dict() for x in self.tasks],
            "childExecutions": [x.to_dict() for x in self.child_executions],
            "timers": [x.to_dict() for x in self.timers],
            "signals": [x.to_dict() for x in self.signals],
            "markers": [x.to_dict() for x in self.markers],
            "decision_failures": [x.to_dict() for x in self.decision_failures],
            "cancel_requested": self.cancel_requested,
        }  # type: t.Dict[str, t.Any]

        if self.configuration.timeout is not None:
            state_dict["configuration"]["timeout"] = serialise_timedelta(
                self.configuration.timeout,
            )
        if self.configuration.decision_task_timeout is not None:
            state_dict["configuration"]["decision_task_timeout"] = serialise_timedelta(
                self.configuration.decision_task_timeout,
            )
        if self.configuration.decision_task_priority is not None:
            state_dict["configuration"]["decision_task_priority"] = (
                self.configuration.decision_task_priority
            )
        if self.configuration.lambda_iam_role_arn is not None:
            state_dict["configuration"]["lambda_iam_role_arn"] = (
                self.configuration.lambda_iam_role_arn
            )
        if self.ended is not None:
            state_dict["ended"] = _common.serialise_datetime(self.ended)
        if self.input is not None:
            state_dict["input"] = self.input
        if self.result is not None:
            state_dict["result"] = self.result
        if self.failure_reason is not None:
            state_dict["failure_reason"] = self.failure_reason
        if self.stop_details is not None:
            state_dict["stop_details"] = self.stop_details
        if self.continuing_execution_run_id is not None:
            state_dict["continuing_execution_run_id"] = self.continuing_execution_run_id

        return state_dict


class _StateBuilder:
    """Workflow execution state builder."""

    execution_history: t.Iterable["_history.Event"]
    execution: ExecutionState
    _tasks: t.Dict[int, t.Union[TaskState, LambdaTaskState]]
    _child_executions: t.Dict[int, ChildExecutionState]
    _child_execution_initiation_events: t.List[
        "_history.StartChildWorkflowExecutionInitiatedEvent"
    ]
    _timers: t.Dict[int, TimerState]
    _latest_decision_event_id: int
    _could_be_new: t.List[
        t.Tuple[int, t.Union[DecisionFailure, SignalState, MarkerState]]
    ]

    def __init__(self, execution_history: t.Iterable["_history.Event"]):
        """Initialise builder.

        Args:
            execution_history: workflow execution history events
        """

        self.execution_history = execution_history
        self._tasks = {}
        self._child_executions = {}
        self._child_execution_initiation_events = []
        self._timers = {}
        self._could_be_new = []

    def _process_event(self, event: "_history.Event") -> None:
        """Update workflow execution state with event."""
        from . import _history
        from . import _executions

        # Decisions
        if isinstance(event, _history.DecisionTaskCompletedEvent):
            self._latest_decision_event_id = event.id

        elif (
            isinstance(event, _history.CancelTimerFailedEvent) or
            isinstance(event, _history.CancelWorkflowExecutionFailedEvent) or
            isinstance(event, _history.CompleteWorkflowExecutionFailedEvent) or
            isinstance(event, _history.ContinueAsNewWorkflowExecutionFailedEvent) or
            isinstance(event, _history.FailWorkflowExecutionFailedEvent) or
            isinstance(event, _history.RecordMarkerFailedEvent) or
            isinstance(event, _history.RequestCancelActivityTaskFailedEvent) or
            isinstance(
                event, _history.RequestCancelExternalWorkflowExecutionFailedEvent
            ) or
            isinstance(event, _history.ScheduleActivityTaskFailedEvent) or
            isinstance(event, _history.ScheduleLambdaFunctionFailedEvent) or
            isinstance(event, _history.SignalExternalWorkflowExecutionFailedEvent) or
            isinstance(event, _history.StartChildWorkflowExecutionFailedEvent) or
            isinstance(event, _history.StartTimerFailedEvent)
        ):
            decision_failure = DecisionFailure(event)
            self.execution.decision_failures.append(decision_failure)
            self._could_be_new.append(
                (self._latest_decision_event_id, decision_failure)
            )

        # Execution
        elif isinstance(event, _history.WorkflowExecutionStartedEvent):
            self.execution = ExecutionState(
                workflow=event.workflow,
                status=_executions.ExecutionStatus.started,
                configuration=event.execution_configuration,
                started=event.occured,
                input=event.execution_input,
            )
        elif isinstance(event, _history.WorkflowExecutionCompletedEvent):
            self.execution.status = _executions.ExecutionStatus.completed
            self.execution.ended = event.occured
            self.execution.result = event.execution_result
        elif isinstance(event, _history.WorkflowExecutionFailedEvent):
            self.execution.status = _executions.ExecutionStatus.failed
            self.execution.ended = event.occured
            self.execution.failure_reason = event.reason
            self.execution.stop_details = event.details
        elif isinstance(event, _history.WorkflowExecutionCancelledEvent):
            self.execution.status = _executions.ExecutionStatus.cancelled
            self.execution.ended = event.occured
            self.execution.stop_details = event.details
        elif isinstance(event, _history.WorkflowExecutionTerminatedEvent):
            self.execution.status = _executions.ExecutionStatus.terminated
            self.execution.ended = event.occured
            self.execution.failure_reason = event.reason
            self.execution.stop_details = event.details
        elif isinstance(event, _history.WorkflowExecutionTimedOutEvent):
            self.execution.status = _executions.ExecutionStatus.timed_out
            self.execution.ended = event.occured
        elif isinstance(event, _history.WorkflowExecutionContinuedAsNewEvent):
            self.execution.status = _executions.ExecutionStatus.continued_as_new
            self.execution.ended = event.occured
            self.execution.continuing_execution_run_id = event.execution_run_id

        elif isinstance(event, _history.WorkflowExecutionCancelRequestedEvent):
            self.execution.cancel_requested = True

        # Tasks
        elif isinstance(event, _history.ActivityTaskScheduledEvent):
            task = TaskState(
                id=event.task_id,
                status=TaskStatus.scheduled,
                activity=event.activity,
                configuration=event.task_configuration,
                scheduled=event.occured,
                input=event.task_input,
                decider_control=event.control,
            )
            self.execution.tasks.append(task)
            self._tasks[event.id] = task
        elif isinstance(event, _history.ActivityTaskStartedEvent):
            task = self._tasks[event.task_scheduled_event_id]
            task.status = TaskStatus.started
            task.started = event.occured
            task.worker_identity = event.worker_identity
        elif isinstance(event, _history.ActivityTaskCompletedEvent):
            task = self._tasks[event.task_scheduled_event_id]
            task.status = TaskStatus.completed
            task.ended = event.occured
            task.result = event.task_result
        elif isinstance(event, _history.ActivityTaskFailedEvent):
            task = self._tasks[event.task_scheduled_event_id]
            task.status = TaskStatus.failed
            task.ended = event.occured
            task.failure_reason = event.reason
            task.stop_details = event.details
        elif isinstance(event, _history.ActivityTaskCancelledEvent):
            task = self._tasks[event.task_scheduled_event_id]
            task.status = TaskStatus.cancelled
            task.ended = event.occured
            task.stop_details = event.details
        elif isinstance(event, _history.ActivityTaskTimedOutEvent):
            task = self._tasks[event.task_scheduled_event_id]
            task.status = TaskStatus.timed_out
            task.ended = event.occured
            task.timeout_type = event.timeout_type
            task.stop_details = event.details

        elif isinstance(event, _history.ActivityTaskCancelRequestedEvent):
            tasks = (task for task in self.execution.tasks if task.id == event.task_id)
            try:
                task, = tasks
            except ValueError:
                raise LookupError(event.task_id) from None
            task.cancel_requested = True

        # elif isinstance(event, _history.StartActivityTaskFailedEvent):
        #     task.status = TaskStatus.failed

        # Lambda tasks
        elif isinstance(event, _history.LambdaFunctionScheduledEvent):
            task = LambdaTaskState(
                id=event.task_id,
                status=TaskStatus.scheduled,
                lambda_function=event.lambda_function,
                scheduled=event.occured,
                timeout=event.task_timeout,
                input=event.task_input,
                decider_control=event.control,
            )
            self.execution.tasks.append(task)
            self._tasks[event.id] = task
        elif isinstance(event, _history.LambdaFunctionStartedEvent):
            task = self._tasks[event.task_scheduled_event_id]
            task.status = TaskStatus.started
            task.started = event.occured
        elif isinstance(event, _history.LambdaFunctionCompletedEvent):
            task = self._tasks[event.task_scheduled_event_id]
            task.status = TaskStatus.completed
            task.ended = event.occured
            task.result = event.task_result
        elif isinstance(event, _history.LambdaFunctionFailedEvent):
            task = self._tasks[event.task_scheduled_event_id]
            task.status = TaskStatus.failed
            task.ended = event.occured
            task.failure_reason = event.reason
            task.stop_details = event.details
        elif isinstance(event, _history.LambdaFunctionTimedOutEvent):
            task = self._tasks[event.task_scheduled_event_id]
            task.status = TaskStatus.timed_out
            task.ended = event.occured

        elif isinstance(event, _history.StartLambdaFunctionFailedEvent):
            task = self._tasks[event.task_scheduled_event_id]
            task.status = TaskStatus.failed

        # Child executions
        elif isinstance(event, _history.StartChildWorkflowExecutionInitiatedEvent):
            self._child_execution_initiation_events.append(event)
        elif isinstance(event, _history.ChildWorkflowExecutionStartedEvent):
            events = (
                e for e in self._child_execution_initiation_events
                if e.id == event.initiated_event_id
            )
            try:
                initiation_event, = events
            except ValueError:
                raise LookupError(event.initiated_event_id) from None

            execution = ChildExecutionState(
                execution=event.execution,
                workflow=initiation_event.workflow,
                status=_executions.ExecutionStatus.started,
                configuration=initiation_event.execution_configuration,
                started=event.occured,
                input=initiation_event.execution_input,
                decider_control=initiation_event.control,
            )
            self.execution.child_executions.append(execution)
            self._child_executions[initiation_event.id] = execution
        elif isinstance(event, _history.ChildWorkflowExecutionCompletedEvent):
            execution = self._child_executions[event.initiated_event_id]
            execution.status = _executions.ExecutionStatus.completed
            execution.ended = event.occured
            execution.result = event.execution_result
        elif isinstance(event, _history.ChildWorkflowExecutionFailedEvent):
            execution = self._child_executions[event.initiated_event_id]
            execution.status = _executions.ExecutionStatus.failed
            execution.ended = event.occured
            execution.failure_reason = event.reason
            execution.stop_details = event.details
        elif isinstance(event, _history.ChildWorkflowExecutionCancelledEvent):
            execution = self._child_executions[event.initiated_event_id]
            execution.status = _executions.ExecutionStatus.cancelled
            execution.ended = event.occured
            execution.stop_details = event.details
        elif isinstance(event, _history.ChildWorkflowExecutionTerminatedEvent):
            execution = self._child_executions[event.initiated_event_id]
            execution.status = _executions.ExecutionStatus.terminated
            execution.ended = event.occured
        elif isinstance(event, _history.ChildWorkflowExecutionTimedOutEvent):
            execution = self._child_executions[event.initiated_event_id]
            execution.status = _executions.ExecutionStatus.terminated
            execution.ended = event.occured

        # Timers
        elif isinstance(event, _history.TimerStartedEvent):
            timer = TimerState(
                id=event.timer_id,
                status=TimerStatus.started,
                duraction=event.timer_duration,
                started=event.occured,
                decider_control=event.control,
            )
            self.execution.timers.append(timer)
            self._timers[event.id] = timer
        elif isinstance(event, _history.TimerFiredEvent):
            timer = self._timers[event.timer_started_event_id]
            timer.status = TimerStatus.fired
            timer.ended = event.occured
        elif isinstance(event, _history.TimerCancelledEvent):
            timer = self._timers[event.timer_started_event_id]
            timer.status = TimerStatus.cancelled
            timer.ended = event.occured

        # Signals
        elif isinstance(event, _history.WorkflowExecutionSignaledEvent):
            signal = SignalState(
                name=event.signal_name,
                received=event.occured,
                input=event.signal_input,
            )
            self.execution.signals.append(signal)
            self._could_be_new.append((self._latest_decision_event_id, signal))

        # Markers
        elif isinstance(event, _history.MarkerRecordedEvent):
            marker = MarkerState(
                name=event.marker_name,
                recorded=event.occured,
                details=event.details,
            )
            self.execution.markers.append(marker)
            self._could_be_new.append((self._latest_decision_event_id, marker))

    def _update_is_new(self) -> None:
        """Mark execution state which happended after last decision."""
        for prior_decision_event_id, state in self._could_be_new:
            state.is_new = prior_decision_event_id == self._latest_decision_event_id

    def build(self) -> None:
        """Build workflow execution state."""
        for event in self.execution_history:
            self._process_event(event)
        self._update_is_new()


def build_state(execution_history: t.Iterable["_history.Event"]) -> ExecutionState:
    """Build workflow execution state.

    Args:
        execution_history: workflow execution history events, earliest
            events must be first

    Returns:
        workflow execution state
    """

    builder = _StateBuilder(execution_history)
    builder.build()
    return builder.execution
