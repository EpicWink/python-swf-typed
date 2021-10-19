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
    scheduled = enum.auto()
    started = enum.auto()
    completed = enum.auto()
    failed = enum.auto()
    cancelled = enum.auto()
    timed_out = enum.auto()


class TimerStatus(enum.Enum):
    started = enum.auto()
    fired = enum.auto()
    cancelled = enum.auto()


@dataclasses.dataclass
class TaskState:
    id: str
    status: TaskStatus
    activity: "_activities.ActivityId"
    configuration: "_tasks.TaskConfiguration"
    scheduled: datetime.datetime
    started: datetime.datetime = None
    ended: datetime.datetime = None
    input: str = None
    worker_identity: str = None
    cancel_requested: bool = False
    result: str = None
    timeout_type: "_history.TimeoutType" = None
    failure_reason: str = None
    stop_details: str = None
    decider_control: str = None

    @property
    def has_ended(self) -> bool:
        return self.status not in (TaskStatus.scheduled, TaskStatus.started)


@dataclasses.dataclass
class ChildExecutionState:
    execution: "_executions.ExecutionId"
    workflow: "_workflows.WorkflowId"
    status: "_executions.ExecutionStatus"
    configuration: "_executions.ExecutionConfiguration"
    started: datetime.datetime
    ended: datetime.datetime = None
    input: str = None
    result: str = None
    timeout_type: "_history.TimeoutType" = None
    failure_reason: str = None
    stop_details: str = None
    decider_control: str = None


@dataclasses.dataclass
class TimerState:
    id: str
    status: TimerStatus
    duraction: datetime.timedelta
    started: datetime.datetime
    ended: datetime.datetime = None
    input: str = None
    decider_control: str = None


@dataclasses.dataclass
class SignalState:
    name: str
    received: datetime.datetime
    is_new: t.Callable[[], bool]
    input: str = None


@dataclasses.dataclass
class ExecutionState:
    status: "_executions.ExecutionStatus"
    configuration: "_executions.ExecutionConfiguration"
    started: datetime.datetime
    ended: datetime.datetime = None
    tasks: t.List[TaskState] = dataclasses.field(default_factory=list)
    child_executions: t.List[ChildExecutionState] = dataclasses.field(
        default_factory=list
    )
    timers: t.List[TimerState] = dataclasses.field(default_factory=list)
    signals: t.List[SignalState] = dataclasses.field(default_factory=list)
    input: str = None
    cancel_requested: bool = False
    result: str = None
    failure_reason: str = None
    stop_details: str = None


class _StateBuilder:
    execution_history: t.Iterable["_history.Event"]
    execution: ExecutionState
    _tasks: t.Dict[int, TaskState]
    _child_executions: t.Dict[int, ChildExecutionState]
    _child_execution_initiation_events: t.List[
        "_history.StartChildWorkflowExecutionInitiatedEvent"
    ]
    _timers: t.Dict[int, TimerState]
    _latest_decision_event_id: int

    def __init__(self, execution_history: t.Iterable["_history.Event"]):
        self.execution_history = execution_history
        self._tasks = {}
        self._child_executions = {}
        self._child_execution_initiation_events = []
        self._timers = {}

    def _process_event(self, event: "_history.Event") -> None:
        from . import _history
        from . import _executions

        # Decisions
        if isinstance(event, _history.DecisionTaskCompletedEvent):
            self._latest_decision_event_id = event.id

        # Execution
        elif isinstance(event, _history.WorkflowExecutionStartedEvent):
            self.execution = ExecutionState(
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

        # Child executions
        elif isinstance(event, _history.StartChildWorkflowExecutionInitiatedEvent):
            self._child_execution_initiation_events.append(event)
        elif isinstance(event, _history.ChildWorkflowExecutionStartedEvent):
            events = (
                e for e in self._child_execution_initiation_events
                if e.id == event.execution_initiated_event_id
            )
            try:
                initiation_event, = events
            except ValueError:
                raise LookupError(event.execution_initiated_event_id) from None

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
            execution = self._child_executions[event.execution_initiated_event_id]
            execution.status = _executions.ExecutionStatus.completed
            execution.ended = event.occured
            execution.result = event.execution_result
        elif isinstance(event, _history.ChildWorkflowExecutionFailedEvent):
            execution = self._child_executions[event.execution_initiated_event_id]
            execution.status = _executions.ExecutionStatus.failed
            execution.ended = event.occured
            execution.failure_reason = event.reason
            execution.stop_details = event.details
        elif isinstance(event, _history.ChildWorkflowExecutionCancelledEvent):
            execution = self._child_executions[event.execution_initiated_event_id]
            execution.status = _executions.ExecutionStatus.cancelled
            execution.ended = event.occured
            execution.stop_details = event.details
        elif isinstance(event, _history.ChildWorkflowExecutionTerminatedEvent):
            execution = self._child_executions[event.execution_initiated_event_id]
            execution.status = _executions.ExecutionStatus.terminated
            execution.ended = event.occured
        elif isinstance(event, _history.ChildWorkflowExecutionTimedOutEvent):
            execution = self._child_executions[event.execution_initiated_event_id]
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
            latest_decision_event_id = self._latest_decision_event_id
            signal = SignalState(
                name=event.signal_name,
                received=event.occured,
                is_new=lambda: (
                    latest_decision_event_id == self._latest_decision_event_id
                ),
                input=event.signal_input,
            )
            self.execution.signals.append(signal)

    def build(self) -> None:
        for event in self.execution_history:
            self._process_event(event)


def build_state(execution_history: t.Iterable["_history.Event"]) -> ExecutionState:
    # Must be forward history
    builder = _StateBuilder(execution_history)
    builder.build()
    return builder.execution
