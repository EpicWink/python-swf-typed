"""SWF activity type management."""

import typing as t
import datetime
import functools
import dataclasses

from . import _common, _tasks

if t.TYPE_CHECKING:
    import botocore.client


@dataclasses.dataclass
class ActivityType(_common.Deserialisable, _common.Serialisable):
    """Activity type identifier."""

    name: str
    """Activity name."""

    version: str
    """Activity version."""

    @classmethod
    def from_api(cls, data) -> "ActivityType":
        return cls(data["name"], data["version"])

    def to_api(self) -> t.Dict[str, str]:
        return {"name": self.name, "version": self.version}


@dataclasses.dataclass
class ActivityTypeInfo(_common.Deserialisable):
    """Activity type details."""

    activity_type: ActivityType
    """Activity type identifier."""

    is_deprecated: bool
    """Activity is deprecated and not active."""

    created: datetime.datetime
    """Creation date."""

    description: t.Union[str, None] = None
    """Activity description."""

    deprecated: t.Union[datetime.datetime, None] = None
    """Deprecation date."""

    @classmethod
    def from_api(cls, data) -> "ActivityTypeInfo":
        return cls(
            activity_type=ActivityType.from_api(data["activityType"]),
            is_deprecated=_common.is_deprecated_by_registration_status[data["status"]],
            created=data["creationDate"],
            description=data.get("description"),
            deprecated=data.get("deprecationDate"),
        )


class DefaultTaskConfiguration(_tasks.PartialTaskConfiguration):
    """Default activity task configuration."""

    @classmethod
    def from_api(cls, data) -> "DefaultTaskConfiguration":
        data = {k[7].lower() + k[8:]: v for k, v in data.items()}
        return super().from_api(data)

    def get_api_args(self):
        data = super().get_api_args()
        return {"default" + k[0].upper() + k[1:]: v for k, v in data.items()}


@dataclasses.dataclass
class ActivityTypeDetails(_common.Deserialisable):
    """Activity type details and default activity task configuration."""

    info: ActivityTypeInfo
    """Activity type info."""

    default_task_configuration: DefaultTaskConfiguration
    """Default task configuration, can be overriden when scheduling."""

    @classmethod
    def from_api(cls, data) -> "ActivityTypeDetails":
        return cls(
            info=ActivityTypeInfo.from_api(data["typeInfo"]),
            default_task_configuration=DefaultTaskConfiguration.from_api(data),
        )


@dataclasses.dataclass
class ActivityTypeFilter(_common.SerialisableToArguments):
    """Activity type filter on activity name."""

    name: str
    """Activity name."""

    def get_api_args(self):
        return {"name": self.name}


def delete_activity_type(
    activity_type: ActivityType,
    domain: str,
    client: t.Union["botocore.client.BaseClient", None] = None,
) -> None:
    """Delete a (deprecated/inactive) activity type.

    Args:
        activity_type: activity type to delete
        domain: domain of activity type
        client: SWF client
    """

    client = _common.ensure_client(client)
    client.delete_activity_type(domain=domain, activityType=activity_type.to_api())


def deprecate_activity_type(
    activity_type: ActivityType,
    domain: str,
    client: t.Union["botocore.client.BaseClient", None] = None,
) -> None:
    """Deprecate (deactivate) an activity type.

    Args:
        activity_type: activity type to deprecate
        domain: domain of activity type
        client: SWF client
    """

    client = _common.ensure_client(client)
    client.deprecate_activity_type(domain=domain, activityType=activity_type.to_api())


def describe_activity_type(
    activity_type: ActivityType,
    domain: str,
    client: t.Union["botocore.client.BaseClient", None] = None,
) -> ActivityTypeDetails:
    """Describe an activity type.

    Args:
        activity_type: activity type to describe
        domain: domain of activity type
        client: SWF client

    Returns:
        activity type details and default activity task configuration
    """

    client = _common.ensure_client(client)
    response = client.describe_activity_type(
        domain=domain, activityType=activity_type.to_api()
    )
    return ActivityTypeDetails.from_api(response)


def list_activity_types(
    domain: str,
    deprecated: bool = False,
    activity_type_filter: t.Union[ActivityTypeFilter, None] = None,
    reverse: bool = False,
    client: t.Union["botocore.client.BaseClient", None] = None,
) -> _common.PageConsumer[ActivityTypeInfo]:
    """List activity types; retrieved semi-lazily.

    Args:
        domain: domain of activity types
        deprecated: list deprecated activity types instead of
            non-deprecated
        activity_type_filter: filter returned activity types by name
        reverse: return results in reverse alphabetical order
        client: SWF client

    Returns:
        matching activity types
    """

    client = _common.ensure_client(client)
    kw = {}
    if activity_type_filter:
        kw.update(activity_type_filter.get_api_args())
    call = functools.partial(
        client.list_activity_types,
        domain=domain,
        registrationStatus=_common.registration_status_by_is_deprecated[deprecated],
        reverseOrder=reverse,
        **kw,
    )
    return _common.iter_paged(call, ActivityTypeInfo.from_api, "typeInfos")


def register_activity_type(
    activity_type: ActivityType,
    domain: str,
    description: t.Union[str, None] = None,
    default_task_configuration: t.Union[DefaultTaskConfiguration, None] = None,
    client: t.Union["botocore.client.BaseClient", None] = None,
) -> None:
    """Register a new activity type.

    Args:
        activity_type: activity type name and version
        domain: domain to register in
        description: activity type description
        default_task_configuration: default configuration for activity
            tasks with this activity type
        client: SWF client
    """

    client = _common.ensure_client(client)
    default_task_configuration = (
        default_task_configuration or DefaultTaskConfiguration()
    )
    kw = default_task_configuration.get_api_args()
    if description or description == "":
        kw["description"] = description
    client.register_activity_type(
        domain=domain,
        name=activity_type.name,
        version=activity_type.version,
        **kw,
    )


def undeprecate_activity_type(
    activity_type: ActivityType,
    domain: str,
    client: t.Union["botocore.client.BaseClient", None] = None,
) -> None:
    """Undeprecate (reactivate) an activity type.

    Args:
        activity_type: activity type to undeprecate
        domain: domain of activity type
        client: SWF client
    """

    client = _common.ensure_client(client)
    client.undeprecate_activity_type(domain=domain, activityType=activity_type.to_api())
