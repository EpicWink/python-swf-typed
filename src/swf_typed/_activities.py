"""SWF activity type management."""

import datetime
import functools
import dataclasses
import typing as t

from . import _tasks
from . import _common

if t.TYPE_CHECKING:
    import botocore.client


@dataclasses.dataclass
class ActivityId:
    name: str
    version: str

    @classmethod
    def from_api(cls, data: t.Dict[str, t.Any]) -> "ActivityId":
        return cls(data["name"], data["version"])

    def to_api(self) -> t.Dict[str, str]:
        return {"name": self.name, "version": self.name}


@dataclasses.dataclass
class ActivityInfo:
    activity: ActivityId
    is_deprecated: bool
    created: datetime.datetime
    description: str = None
    deprecated: datetime.datetime = None

    @classmethod
    def from_api(cls, data: t.Dict[str, t.Any]) -> "ActivityInfo":
        return cls(
            activity=ActivityId.from_api(data["activityType"]),
            is_deprecated=_common.is_deprecated_by_registration_status[data["status"]],
            created=data["createdDate"],
            description=data.get("description"),
            deprecated=data["deprecationDate"],
        )


class DefaultTaskConfiguration(_tasks.PartialTaskConfiguration):
    @classmethod
    def from_api(cls, data: t.Dict[str, t.Any]) -> "DefaultTaskConfiguration":
        data = {k[7].lower() + k[8:]: v for k, v in data.items()}
        return super().from_api(data)

    def get_api_args(self):
        data = super().get_api_args()
        return {"default" + k[0].upper() + k[1:]: v for k, v in data.items()}


@dataclasses.dataclass
class ActivityDetails:
    info: ActivityInfo
    default_task_configuration: DefaultTaskConfiguration

    @classmethod
    def from_api(cls, data: t.Dict[str, t.Any]) -> "ActivityDetails":
        return cls(
            info=ActivityInfo.from_api(data["typeInfo"]),
            default_task_configuration=DefaultTaskConfiguration.from_api(data),
        )


@dataclasses.dataclass
class ActivityIdFilter:
    name: str

    def to_api(self) -> t.Dict[str, str]:
        return {"name": self.name}


def deprecate_activity(
    activity: ActivityId,
    domain: str,
    client: "botocore.client.BaseClient" = None,
) -> None:
    client = _common.ensure_client(client)
    client.deprecate_activity_type(domain=domain, activityType=activity.to_api())


def describe_activity(
    activity: ActivityId,
    domain: str,
    client: "botocore.client.BaseClient" = None,
) -> ActivityDetails:
    client = _common.ensure_client(client)
    response = client.describe_activity_type(
        domain=domain, activityType=activity.to_api()
    )
    return ActivityDetails.from_api(response)


def list_activities(
    domain: str,
    deprecated: bool = False,
    activity_filter: ActivityIdFilter = None,
    reverse: bool = False,
    client: "botocore.client.BaseClient" = None,
) -> t.Generator[ActivityInfo, None, None]:
    client = _common.ensure_client(client)
    kw = {}
    if activity_filter:
        kw["name"] = activity_filter.name
    call = functools.partial(
        client.list_activity_types,
        domain=domain,
        registrationStatus=_common.registration_status_by_is_deprecated[deprecated],
        reverseOrder=reverse,
        **kw,
    )
    return _common.iter_paged(call, ActivityInfo.from_api, "typeInfos")


def register_activity(
    activity: ActivityId,
    domain: str,
    description: str = None,
    default_task_configuration: DefaultTaskConfiguration = None,
    client: "botocore.client.BaseClient" = None,
) -> None:
    client = _common.ensure_client(client)
    default_task_configuration = (
        default_task_configuration or DefaultTaskConfiguration()
    )
    kw = default_task_configuration.get_api_args()
    if description or description == "":
        kw["description"] = description
    client.register_activity_type(
        domain=domain,
        name=activity.name,
        version=activity.version,
        **kw,
    )


def undeprecate_activity(
    domain: str,
    activity: ActivityId,
    client: "botocore.client.BaseClient" = None,
) -> None:
    client = _common.ensure_client(client)
    client.undeprecate_activity_type(domain=domain, activityType=activity.to_api())
