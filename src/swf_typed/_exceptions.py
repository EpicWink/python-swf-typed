"""Explicit exceptions for SWF faults/errors."""

import functools
import dataclasses
import typing as t

if t.TYPE_CHECKING:
    import botocore.client
    import botocore.exceptions

T = t.TypeVar("T")

_swf_client_methods = (
    "count_closed_workflow_executions",
    "count_open_workflow_executions",
    "count_pending_activity_tasks",
    "count_pending_decision_tasks",
    "deprecate_activity_type",
    "deprecate_domain",
    "deprecate_workflow_type",
    "describe_activity_type",
    "describe_domain",
    "describe_workflow_execution",
    "describe_workflow_type",
    "generate_presigned_url",
    "get_workflow_execution_history",
    "list_activity_types",
    "list_closed_workflow_executions",
    "list_domains",
    "list_open_workflow_executions",
    "list_tags_for_resource",
    "list_workflow_types",
    "poll_for_activity_task",
    "poll_for_decision_task",
    "record_activity_task_heartbeat",
    "register_activity_type",
    "register_domain",
    "register_workflow_type",
    "request_cancel_workflow_execution",
    "respond_activity_task_canceled",
    "respond_activity_task_completed",
    "respond_activity_task_failed",
    "respond_decision_task_completed",
    "signal_workflow_execution",
    "start_workflow_execution",
    "tag_resource",
    "terminate_workflow_execution",
    "undeprecate_activity_type",
    "undeprecate_domain",
    "undeprecate_workflow_type",
    "untag_resource",
)
_pass_through_exceptions = (
    "IncompleteSignature",
    "InvalidAction",
    "InvalidQueryParameter",
    "MalformedQueryString",
    "MissingAuthenticationToken",
    "MissingParameter",
)


class SwfError(Exception):
    """Base SWF error."""

    _child_classes = {}

    def __init_subclass__(cls, **kwargs):
        name = cls.__name__
        if name[-5:] == "Error" and name != "ValidationError":
            name = name[:-5]
        SwfError._child_classes[name] = cls
        if name == "ValidationError":
            SwfError._child_classes["ValidationException"] = cls
        super().__init_subclass__(**kwargs)

    @classmethod
    def from_botocore_exception(
        cls, exception: "botocore.exceptions.ClientError"
    ) -> "SwfError":
        error = exception.response["Error"]["Code"]
        exception_class = cls._child_classes[error]
        return exception_class(exception.response["Error"].get("Message"))


class AccessDeniedException(SwfError):
    """You do not have sufficient access to perform this action."""


class DefaultUndefinedFault(SwfError):
    """The required parameters were not set for execution start.

    Some workflow execution parameters, such as the decision task-list, must
    be set to start the execution. However, these parameters might have been
    set as defaults when the workflow was registered. In this case, you can
    omit these arguments to ``start_execution``  and Amazon SWF uses the
    values defined in the workflow.

    Note: if these parameters aren't set and no default parameters were
    defined in the workflow, this error is displayed.
    """


class DomainAlreadyExistsFault(SwfError):
    """The domain already exists.

    You may get this fault if you are registering a domain that is either
    already registered or deprecated, or if you undeprecate a domain that is
    currently registered.
    """


class DomainDeprecatedFault(SwfError):
    """The specified domain has been deprecated."""


class InternalFailureError(SwfError):
    """The request processing has failed because of an unknown error,
    exception or failure.
    """


class InvalidClientTokenIdError(SwfError):
    """The X.509 certificate or AWS access key ID provided does not exist in
    our records.
    """


class InvalidParameterCombinationError(SwfError):
    """Parameters that must not be used together were used together"""


class InvalidParameterValueError(SwfError):
    """An invalid or out-of-range value was supplied for the input parameter."""


class LimitExceededFault(SwfError):
    """A system-imposed limitation has been reached.

    To address this fault you should either clean up unused resources or
    increase the limit by contacting AWS.
    """


class MissingActionError(SwfError):
    """The request is missing an action or a required parameter."""


class NotAuthorizedError(SwfError):
    """You do not have permission to perform this action."""


class OperationNotPermittedFault(SwfError):
    """The caller doesn't have sufficient permissions to invoke the action."""


class OptInRequiredError(SwfError):
    """The AWS access key ID needs a subscription for the service."""


class RequestExpiredError(SwfError):
    """The request reached the service more than 15 minutes after the date
    stamp on the request or more than 15 minutes after the request expiration
    date (such as for pre-signed URLs), or the date stamp on the request is
    more than 15 minutes in the future.
    """


class ServiceUnavailableError(SwfError):
    """The request has failed due to a temporary failure of the server."""


class ThrottlingException(SwfError):
    """The request was denied due to request throttling."""


class TooManyTagsFault(SwfError):
    """You've exceeded the number of tags allowed for a domain."""


class TypeAlreadyExistsFault(SwfError):
    """The activity/workflow already exists in the specified domain.

    You may get this fault if you are registering an activity/workflow that is
    either already registered or deprecated, or if you undeprecate an
    activity/workflow that is currently registered.
    """


class TypeDeprecatedFault(SwfError):
    """The specified activity or workflow type was already deprecated."""


class UnknownResourceFault(SwfError):
    """The named resource cannot be found with in the scope of this operation
    (region or domain).

    This could happen if the named resource was never created or is no longer
    available for this operation.
    """


class ValidationError(SwfError):
    """The input fails to satisfy the constraints specified by an AWS service."""


class WorkflowExecutionAlreadyStartedFault(SwfError):
    """An open execution with the same ID is already running in the specified
    domain.
    """


@dataclasses.dataclass
class ExceptionRedirectMethodWrapper(t.Generic[T]):
    _f: t.Callable[..., T]

    def __post_init__(self):
        functools.update_wrapper(self, self._f)

    def __call__(self, *args, **kwargs) -> T:
        import botocore.exceptions

        try:
            return self._f(*args, **kwargs)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] in _pass_through_exceptions:
                raise
            raise SwfError.from_botocore_exception(e) from None

    def __getattr__(self, item: str):
        return getattr(self._f, item)


def redirect_exceptions_in_swf_client(swf_client: "botocore.client.BaseClient") -> None:
    """Redirect ``botocore`` client-error exceptions to custom exceptions.

    Args:
        swf_client: client to redirect exceptions from, modified in-place
    """

    for name in _swf_client_methods:
        attr = getattr(swf_client, name)
        attr = ExceptionRedirectMethodWrapper(attr)
        setattr(swf_client, name, attr)
