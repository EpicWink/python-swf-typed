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
class WorkflowId:
    name: str
    version: str

    @classmethod
    def from_api(cls, data: t.Dict[str, t.Any]) -> "WorkflowId":
        return cls(data["name"], data["version"])

    def to_api(self) -> t.Dict[str, str]:
        return {"name": self.name, "version": self.version}


@dataclasses.dataclass
class WorkflowInfo:
    workflow: WorkflowId
    is_deprecated: bool
    created: datetime.datetime
    description: str = None
    deprecated: datetime.datetime = None

    @classmethod
    def from_api(cls, data: t.Dict[str, t.Any]) -> "WorkflowInfo":
        return cls(
            workflow=WorkflowId.from_api(data["workflowType"]),
            is_deprecated=_common.is_deprecated_by_registration_status[data["status"]],
            created=data["createdDate"],
            description=data.get("description"),
            deprecated=data["deprecationDate"],
        )


class DefaultExecutionConfiguration(_executions.PartialExecutionConfiguration):
    @classmethod
    def from_api(cls, data: t.Dict[str, t.Any]) -> "DefaultExecutionConfiguration":
        data = {k[7].lower() + k[8:]: v for k, v in data.items()}
        return super().from_api(data)

    def get_api_args(self):
        kw = super().get_api_args()
        return {"default" + k[0].upper() + k[1:]: v for k, v in kw.items()}


@dataclasses.dataclass
class WorkflowDetails:
    info: WorkflowInfo
    default_execution_configuration: DefaultExecutionConfiguration

    @classmethod
    def from_api(cls, data: t.Dict[str, t.Any]) -> "WorkflowDetails":
        configuration = DefaultExecutionConfiguration.from_api(data)
        return cls(
            info=WorkflowInfo.from_api(data["typeInfo"]),
            default_execution_configuration=configuration,
        )


@dataclasses.dataclass
class WorkflowIdFilter:
    name: str

    def to_api(self) -> t.Dict[str, str]:
        return {"name": self.name}


def deprecate_workflow(
    workflow: WorkflowId,
    domain: str,
    client: "botocore.client.BaseClient" = None,
) -> None:
    client = _common.ensure_client(client)
    client.deprecate_workflow_type(domain=domain, workflowType=workflow.to_api())


def describe_workflow(
    workflow: WorkflowId,
    domain: str,
    client: "botocore.client.BaseClient" = None,
) -> WorkflowDetails:
    client = _common.ensure_client(client)
    response = client.describe_workflow_type(
        domain=domain, workflowType=workflow.to_api()
    )
    return WorkflowDetails.from_api(response)


def list_workflows(
    domain: str,
    deprecated: bool = False,
    workflow_filter: WorkflowIdFilter = None,
    reverse: bool = False,
    client: "botocore.client.BaseClient" = None,
) -> t.Generator[WorkflowInfo, None, None]:
    client = _common.ensure_client(client)
    kw = {}
    if workflow_filter:
        kw["name"] = workflow_filter.name
    call = functools.partial(
        client.list_workflow_types,
        domain=domain,
        registrationStatus=_common.registration_status_by_is_deprecated[deprecated],
        reverseOrder=reverse,
        **kw,
    )
    return _common.iter_paged(call, WorkflowInfo.from_api, "typeInfos")


def register_workflow(
    workflow: WorkflowId,
    domain: str,
    description: str = None,
    default_execution_configuration: DefaultExecutionConfiguration = None,
    client: "botocore.client.BaseClient" = None,
) -> None:
    client = _common.ensure_client(client)
    default_execution_configuration = (
        default_execution_configuration or DefaultExecutionConfiguration()
    )
    kw = default_execution_configuration.get_api_args()
    if description or description == "":
        kw["description"] = description
    client.register_workflow_type(
        domain=domain,
        name=workflow.name,
        version=workflow.version,
        **kw,
    )


def undeprecate_workflow(
    workflow: WorkflowId,
    domain: str,
    client: "botocore.client.BaseClient" = None,
) -> None:
    client = _common.ensure_client(client)
    client.undeprecate_workflow(domain=domain, workflowType=workflow.to_api())
