"""SWF workflow type management."""

import datetime
import functools
import dataclasses
import typing as t

from . import _common
from . import _executions

if t.TYPE_CHECKING:
    import botocore.client


@dataclasses.dataclass
class WorkflowTypeReference(_common.Deserialisable, _common.Serialisable):
    """Workflow type identifier."""

    name: str
    """Workflow name."""

    version: str
    """Workflow version."""

    @classmethod
    def from_api(cls, data) -> "WorkflowTypeReference":
        return cls(data["name"], data["version"])

    def to_api(self):
        return {"name": self.name, "version": self.version}


@dataclasses.dataclass
class WorkflowTypeInfo(_common.Deserialisable):
    """Workflow type details."""

    workflow_type: WorkflowTypeReference
    """Workflow ID."""

    is_deprecated: bool
    """Workflow is deprecated and not active."""

    created: datetime.datetime
    """Creation date."""

    description: str = None
    """Workflow description."""

    deprecated: datetime.datetime = None
    """Deprecation date."""

    @classmethod
    def from_api(cls, data) -> "WorkflowTypeInfo":
        return cls(
            workflow_type=WorkflowTypeReference.from_api(data["workflowType"]),
            is_deprecated=_common.is_deprecated_by_registration_status[data["status"]],
            created=data["creationDate"],
            description=data.get("description"),
            deprecated=data.get("deprecationDate"),
        )


class DefaultExecutionConfiguration(_executions.PartialExecutionConfiguration):
    """Default workflow execution configuration."""

    @classmethod
    def from_api(cls, data) -> "DefaultExecutionConfiguration":
        data = {k[7].lower() + k[8:]: v for k, v in data.items()}
        return super().from_api(data)

    def get_api_args(self):
        kw = super().get_api_args()
        return {"default" + k[0].upper() + k[1:]: v for k, v in kw.items()}


@dataclasses.dataclass
class WorkflowTypeDetails(_common.Deserialisable):
    """Workflow type details and default workflow execution configuration."""

    info: WorkflowTypeInfo
    """Workflow details."""

    default_execution_configuration: DefaultExecutionConfiguration
    """Default execution configuration, can be overriden when starting."""

    @classmethod
    def from_api(cls, data) -> "WorkflowTypeDetails":
        configuration = DefaultExecutionConfiguration.from_api(data)
        return cls(
            info=WorkflowTypeInfo.from_api(data["typeInfo"]),
            default_execution_configuration=configuration,
        )


@dataclasses.dataclass
class WorkflowTypeFilter(_common.Serialisable, _common.SerialisableToArguments):
    """Workflow type filter on workflow name."""

    name: str
    """Workflow name."""

    def to_api(self):
        return {"name": self.name}

    def get_api_args(self):
        return {"name": self.name}


def delete_workflow_type(
    workflow_type: WorkflowTypeReference,
    domain: str,
    client: "botocore.client.BaseClient" = None,
) -> None:
    """Delete a (deprecated/inactive) workflow type.

    Args:
        workflow_type: workflow type to delete
        domain: domain of workflow type
        client: SWF client
    """

    client = _common.ensure_client(client)
    client.delete_workflow_type(domain=domain, workflowType=workflow_type.to_api())


def deprecate_workflow_type(
    workflow_type: WorkflowTypeReference,
    domain: str,
    client: "botocore.client.BaseClient" = None,
) -> None:
    """Deprecate (deactivate) a workflow type.

    Args:
        workflow_type: workflow type to deprecate
        domain: domain of workflow type
        client: SWF client
    """

    client = _common.ensure_client(client)
    client.deprecate_workflow_type(domain=domain, workflowType=workflow_type.to_api())


def describe_workflow_type(
    workflow_type: WorkflowTypeReference,
    domain: str,
    client: "botocore.client.BaseClient" = None,
) -> WorkflowTypeDetails:
    """Describe a workflow type.

    Args:
        workflow_type: workflow type to describe
        domain: domain of workflow type
        client: SWF client

    Returns:
        workflow type details and default workflow execution configuration
    """

    client = _common.ensure_client(client)
    response = client.describe_workflow_type(
        domain=domain, workflowType=workflow_type.to_api()
    )
    return WorkflowTypeDetails.from_api(response)


def list_workflow_types(
    domain: str,
    deprecated: bool = False,
    workflow_type_filter: WorkflowTypeFilter = None,
    reverse: bool = False,
    client: "botocore.client.BaseClient" = None,
) -> t.Generator[WorkflowTypeInfo, None, None]:
    """List workflow types; retrieved semi-lazily.

    Args:
        domain: domain of workflow types
        deprecated: list deprecated workflow types instead of
            non-deprecated
        workflow_type_filter: filter returned workflow types by name
        reverse: return results in reverse alphabetical order
        client: SWF client

    Returns:
        matching workflow types
    """

    client = _common.ensure_client(client)
    kw = {}
    if workflow_type_filter:
        kw.update(workflow_type_filter.get_api_args())
    call = functools.partial(
        client.list_workflow_types,
        domain=domain,
        registrationStatus=_common.registration_status_by_is_deprecated[deprecated],
        reverseOrder=reverse,
        **kw,
    )
    return _common.iter_paged(call, WorkflowTypeInfo.from_api, "typeInfos")


def register_workflow_type(
    workflow_type: WorkflowTypeReference,
    domain: str,
    description: str = None,
    default_execution_configuration: DefaultExecutionConfiguration = None,
    client: "botocore.client.BaseClient" = None,
) -> None:
    """Register a new workflow type.

    Args:
        workflow_type: workflow type name and version
        domain: domain to register in
        description: workflow type description
        default_execution_configuration: default configuration for workflow
            executions with this workflow type
        client: SWF client
    """

    client = _common.ensure_client(client)
    default_execution_configuration = (
        default_execution_configuration or DefaultExecutionConfiguration()
    )
    kw = default_execution_configuration.get_api_args()
    if description or description == "":
        kw["description"] = description
    client.register_workflow_type(
        domain=domain,
        name=workflow_type.name,
        version=workflow_type.version,
        **kw,
    )


def undeprecate_workflow_type(
    workflow_type: WorkflowTypeReference,
    domain: str,
    client: "botocore.client.BaseClient" = None,
) -> None:
    """Undeprecate (reactivate) a workflow type.

    Args:
        workflow_type: workflow type to undeprecate
        domain: domain of workflow type
        client: SWF client
    """

    client = _common.ensure_client(client)
    client.undeprecate_workflow_type(domain=domain, workflowType=workflow_type.to_api())
