"""SWF ```swf_typed`` clients."""

import typing as t
import dataclasses

from . import (
    _activities,
    _common,
    _decisions,
    _domains,
    _executions,
    _history,
    _tasks,
    _workflows,
)

if t.TYPE_CHECKING:
    # noinspection PyPackageRequirements
    import botocore.client


@dataclasses.dataclass
class _BaseClient:
    client: "botocore.client.BaseClient"
    """AWS SWF SDK client."""


@dataclasses.dataclass
class DomainClient(_BaseClient):
    """Specific SWF domain interaction through ``swf_typed``."""

    domain: str
    """SWF domain name."""

    _arn_cache: t.Union[str, None] = dataclasses.field(
        default=None, init=False, compare=False, repr=False, hash=False
    )

    def _get_arn(self) -> str:
        if not self._arn_cache:
            self._arn_cache = self.describe_domain().info.arn
        return t.cast(str, self._arn_cache)

    def delete_activity(self, activity: _activities.ActivityId) -> None:
        """Delete a (deprecated/inactive) activity type.

        Args:
            activity: activity type to delete
        """

        _activities.delete_activity(
            activity=activity, domain=self.domain, client=self.client
        )

    def deprecate_activity(self, activity: _activities.ActivityId) -> None:
        """Deprecate (deactivate) an activity type.

        Args:
            activity: activity type to deprecate
        """

        _activities.deprecate_activity(
            activity=activity, domain=self.domain, client=self.client
        )

    def describe_activity(
        self, activity: _activities.ActivityId
    ) -> _activities.ActivityDetails:
        """Describe an activity type.

        Args:
            activity: activity type to describe

        Returns:
            activity type details and default activity task configuration
        """

        return _activities.describe_activity(
            activity=activity, domain=self.domain, client=self.client
        )

    def list_activities(
        self,
        deprecated: bool = False,
        activity_filter: t.Union[_activities.ActivityIdFilter, None] = None,
        reverse: bool = False,
    ) -> _common.PageConsumer[_activities.ActivityInfo]:
        """List activity types; retrieved semi-lazily.

        Args:
            deprecated: list deprecated activity types instead of
                non-deprecated
            activity_filter: filter returned activity types by name
            reverse: return results in reverse alphabetical order

        Returns:
            matching activity types
        """

        return _activities.list_activities(
            domain=self.domain,
            deprecated=deprecated,
            activity_filter=activity_filter,
            reverse=reverse,
            client=self.client,
        )

    def register_activity(
        self,
        activity: _activities.ActivityId,
        description: t.Union[str, None] = None,
        default_task_configuration: t.Union[
            _activities.DefaultTaskConfiguration, None
        ] = None,
    ) -> None:
        """Register a new activity type.

        Args:
            activity: activity type name and version
            description: activity type description
            default_task_configuration: default configuration for activity
                tasks with this activity type
        """

        _activities.register_activity(
            activity=activity,
            domain=self.domain,
            description=description,
            default_task_configuration=default_task_configuration,
            client=self.client,
        )

    def undeprecate_activity(self, activity: _activities.ActivityId) -> None:
        """Undeprecate (reactivate) an activity type.

        Args:
            activity: activity type to undeprecate
        """

        _activities.undeprecate_activity(
            activity=activity, domain=self.domain, client=self.client
        )

    def get_number_of_pending_decision_tasks(self, task_list: str) -> int:
        """Get the number of pending decision tasks.

        Warns if the number of pending tasks is greater than what's returned.

        Args:
            task_list: decision task-list

        Returns:
            number of pending tasks
        """

        return _decisions.get_number_of_pending_decision_tasks(
            task_list=task_list, domain=self.domain, client=self.client
        )

    def request_decision_task(
        self,
        task_list: str,
        decider_identity: t.Union[str, None] = None,
        no_tasks_callback: t.Callable[[], None] = lambda: None,
    ) -> _decisions.DecisionTask:
        """Request (poll for) a decision task; blocks until task is received.

        Workflow execution history events are retrieved semi-lazily.

        Args:
            task_list: decision task-list to request from
            decider_identity: decider identity, recorded in execution history
            no_tasks_callback: called after no tasks were provided by SWF

        Returns:
            decision task
        """

        return _decisions.request_decision_task(
            task_list=task_list,
            domain=self.domain,
            decider_identity=decider_identity,
            no_tasks_callback=no_tasks_callback,
            client=self.client,
        )

    def deprecate_domain(self) -> None:
        """Deprecate (deactivate) domain."""
        _domains.deprecate_domain(domain=self.domain, client=self.client)

    def describe_domain(self) -> _domains.DomainDetails:
        """Describe domain.

        Returns:
            domain details and configuration
        """

        return _domains.describe_domain(domain=self.domain, client=self.client)

    def get_domain_tags(self) -> t.Dict[str, str]:
        """Get domain's tags.

        May first describe domain to discover its ARN.

        Returns:
            domain's resource tags
        """

        return _domains.get_domain_tags(domain_arn=self._get_arn(), client=self.client)

    def register_domain(
        self,
        configuration: _domains.DomainConfiguration,
        description: t.Union[str, None] = None,
        tags: t.Union[t.Dict[str, str], None] = None,
    ) -> None:
        """Register new domain.

        Args:
            configuration: configuration
            description: domain description
            tags: domain's resource tags
        """

        _domains.register_domain(
            domain=self.domain,
            configuration=configuration,
            description=description,
            tags=tags,
            client=self.client,
        )

    def tag_domain(self, tags: t.Dict[str, str]) -> None:
        """Add tags to domain.

        May first describe domain to discover its ARN.

        Args:
            tags: tags to add
        """

        _domains.tag_domain(domain_arn=self._get_arn(), tags=tags, client=self.client)

    def undeprecate_domain(self) -> None:
        """Undeprecate (reactivate) domain."""
        _domains.undeprecate_domain(domain=self.domain, client=self.client)

    def untag_domain(self, tags: t.List[str]) -> None:
        """Remove tags from a domain.

        May first describe domain to discover its ARN.

        Args:
            tags: tags (keys) to remove
        """

        _domains.untag_domain(domain_arn=self._get_arn(), tags=tags, client=self.client)

    def describe_execution(self):
        # TODO
        return _executions.describe_execution(domain=self.domain, client=self.client)

    def get_number_of_closed_executions(self):
        # TODO
        return _executions.get_number_of_closed_executions(
            domain=self.domain, client=self.client
        )

    def get_number_of_open_executions(self):
        # TODO
        return _executions.get_number_of_open_executions(
            domain=self.domain, client=self.client
        )

    def list_closed_executions(self):
        # TODO
        return _executions.list_closed_executions(
            domain=self.domain, client=self.client
        )

    def list_open_executions(self):
        # TODO
        return _executions.list_open_executions(domain=self.domain, client=self.client)

    def request_cancel_execution(self):
        # TODO
        return _executions.request_cancel_execution(
            domain=self.domain, client=self.client
        )

    def signal_execution(self):
        # TODO
        return _executions.signal_execution(domain=self.domain, client=self.client)

    def start_execution(self):
        # TODO
        return _executions.start_execution(domain=self.domain, client=self.client)

    def terminate_execution(self):
        # TODO
        return _executions.terminate_execution(domain=self.domain, client=self.client)

    def get_execution_history(
        self,
        execution: _executions.ExecutionId,
        reverse: bool = False,
    ) -> _common.PageConsumer[_history.Event]:
        """Get workflow execution history; retrieved semi-lazily.

        Args:
            execution: workflow execution to get history of
            reverse: return latest events first

        Returns:
            workflow execution history events
        """

        return _history.get_execution_history(
            execution=execution, domain=self.domain, reverse=reverse, client=self.client
        )

    def get_last_execution_history_event(
        self,
        execution: _executions.ExecutionId,
    ) -> _history.Event:
        """Get last workflow execution history event.

        Args:
            execution: workflow execution to get history event of

        Returns:
            most recent workflow execution history event
        """

        return _history.get_last_execution_history_event(
            execution=execution, domain=self.domain, client=self.client
        )

    def get_number_of_pending_tasks(self, task_list: str) -> int:
        """Get the number of pending activity tasks.

        Warns if the number of pending tasks is greater than what's returned.

        Args:
            task_list: activity task-list

        Returns:
            number of pending tasks
        """

        return _tasks.get_number_of_pending_tasks(
            task_list=task_list, domain=self.domain, client=self.client
        )

    def request_task(
        self,
        task_list: str,
        worker_identity: t.Union[str, None] = None,
        no_tasks_callback: t.Union[t.Callable[[], None], None] = None,
    ) -> _tasks.WorkerTask:
        """Request (poll for) an activity task; blocks until task is received.

        Args:
            task_list: activity task-list to request from
            worker_identity: activity worker identity, recorded in execution
                history
            no_tasks_callback: called after no tasks were provided by SWF

        Returns:
            activity task
        """

        return _tasks.request_task(
            task_list=task_list,
            domain=self.domain,
            worker_identity=worker_identity,
            no_tasks_callback=no_tasks_callback,
            client=self.client,
        )

    def delete_workflow(self):
        # TODO
        return _workflows.delete_workflow(domain=self.domain, client=self.client)

    def deprecate_workflow(self):
        # TODO
        return _workflows.deprecate_workflow(domain=self.domain, client=self.client)

    def describe_workflow(self):
        # TODO
        return _workflows.describe_workflow(domain=self.domain, client=self.client)

    def list_workflows(self):
        # TODO
        return _workflows.list_workflows(domain=self.domain, client=self.client)

    def register_workflow(self):
        # TODO
        return _workflows.register_workflow(domain=self.domain, client=self.client)

    def undeprecate_workflow(self):
        # TODO
        return _workflows.undeprecate_workflow(domain=self.domain, client=self.client)


@dataclasses.dataclass
class TaskClient(_BaseClient):
    """Individual SWF task interaction through ``swf_typed``."""

    token: str
    """SWF task token."""

    def send_decisions(
        self,
        decisions: t.Iterable[_decisions.Decision],
        context: str = None,
        task_list_override: t.Union[_decisions.TaskListOverride, None] = None,
    ) -> None:
        """Make decisions for a workflow execution, completing decision task.

        Args:
            decisions: decisions to make
            context: workflow execution context to set
            task_list_override: decision task list override configuration
        """

        _decisions.send_decisions(
            token=self.token,
            decisions=decisions,
            context=context,
            task_list_override=task_list_override,
            client=self.client,
        )

    def cancel_task(self, details: t.Union[str, None] = None) -> None:
        """Cancel the current activity task.

        Only valid if the activity task is open and has a cancellation request.

        Args:
            details: extra information, usually for explanation
        """

        _tasks.cancel_task(token=self.token, details=details, client=self.client)

    def complete_task(self, result: t.Union[str, None] = None) -> None:
        """Complete the current activity task.

        Only valid if the activity task is open.

        Args:
            result: task result
        """

        _tasks.complete_task(token=self.token, result=result, client=self.client)

    def fail_task(
        self,
        reason: t.Union[str, None] = None,
        details: t.Union[str, None] = None,
    ) -> None:
        """Fail the current activity task.

        Only valid if the activity task is open.

        Args:
            reason: failure reason, usually for classification
            details: failure details, usually for explanation
        """

        _tasks.fail_task(
            token=self.token, reason=reason, details=details, client=self.client
        )

    def send_heartbeat(self, details: t.Union[str, None] = None) -> None:
        """Send a heartbeat to SWF for the current activity task.

        Args:
            details: activity task progress message

        Raises:
            _tasks.Cancelled: if activity task was cancelled by decider
        """

        _tasks.send_heartbeat(token=self.token, details=details, client=self.client)


@dataclasses.dataclass
class SWFClient(_BaseClient):
    """SWF interaction through ``swf_typed``."""

    @classmethod
    def new(cls) -> "SWFClient":
        """Construct using (mostly) default configuration in ``boto3``.

        "Mostly" as adaptive retries are enabled.
        """

        import boto3
        import botocore.config

        return cls(
            client=boto3.client(
                "swf", config=botocore.config.Config(retries=dict(mode="adaptive"))
            ),
        )

    def get_domain_client(self, domain: str) -> "DomainClient":
        """Get client for an SWF domain.

        Args:
            domain: SWF domain name

        Returns:
            domain client
        """

        return DomainClient(domain=domain, client=self.client)

    def get_actor_client(self, token: str) -> "TaskClient":
        """Get client for an SWF (activity or decision) task.

        Args:
            token: SWF task token

        Returns:
            task client
        """

        return TaskClient(token=token, client=self.client)

    def list_domains(
        self,
        deprecated: bool = False,
        reverse: bool = False,
    ) -> _common.PageConsumer[_domains.DomainInfo]:
        """List domains; retrieved semi-lazily.

        Args:
            deprecated: list deprecated domains instead of non-deprecated
            reverse: return results in reverse alphabetical order

        Returns:
            domains
        """

        return _domains.list_domains(
            deprecated=deprecated, reverse=reverse, client=self.client
        )
