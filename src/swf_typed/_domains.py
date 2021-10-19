"""SWF domain management."""

import datetime
import functools
import dataclasses
import typing as t

from . import _common

if t.TYPE_CHECKING:
    import botocore.client


@dataclasses.dataclass
class DomainInfo:
    name: str
    is_deprecated: bool
    arn: str
    description: str = None

    @classmethod
    def from_api(cls, data: t.Dict[str, t.Any]) -> "DomainInfo":
        return cls(
            name=data["name"],
            is_deprecated=_common.is_deprecated_by_registration_status[data["status"]],
            arn=data["arn"],
            description=data.get("description"),
        )


@dataclasses.dataclass
class DomainConfiguration:
    execution_retention: datetime.timedelta

    @classmethod
    def from_api(cls, data: t.Dict[str, t.Any]) -> "DomainConfiguration":
        execution_retention_data = data["workflowExecutionRetentionPeriodInDays"]
        return cls(
            execution_retention=datetime.timedelta(days=int(execution_retention_data)),
        )


@dataclasses.dataclass
class DomainDetails:
    info: DomainInfo
    configuration: DomainConfiguration

    @classmethod
    def from_api(cls, data: t.Dict[str, t.Any]) -> "DomainDetails":
        return cls(
            info=DomainInfo.from_api(data["domainInfo"]),
            configuration=DomainConfiguration.from_api(data["domainInfo"]),
        )


def deprecate_domain(
    domain: str,
    client: "botocore.client.BaseClient" = None,
) -> None:
    client = _common.ensure_client(client)
    client.deprecate_domain(name=domain)


def describe_domain(
    domain: str,
    client: "botocore.client.BaseClient" = None,
) -> DomainDetails:
    client = _common.ensure_client(client)
    response = client.describe_domain(name=domain)
    return DomainDetails.from_api(response)


def list_domains(
    is_deprecated: bool = False,
    reverse: bool = False,
    client: "botocore.client.BaseClient" = None,
) -> t.Generator[DomainInfo, None, None]:
    client = _common.ensure_client(client)
    call = functools.partial(
        client.list_domains,
        registrationStatus=_common.registration_status_by_is_deprecated[is_deprecated],
        reverseOrder=reverse,
    )
    return _common.iter_paged(call, DomainInfo.from_api, "domainInfos")


def list_domain_tags(
    domain_arn: str,
    client: "botocore.client.BaseClient" = None,
) -> t.Dict[str, str]:
    client = _common.ensure_client(client)
    response = client.list_tags_for_resource(resourceArn=domain_arn)
    return {tag["key"]: tag["value"] for tag in response["tags"]}


def register_domain(
    domain: str,
    configuration: DomainConfiguration,
    description: str = None,
    tags: t.Dict[str, str] = None,
    client: "botocore.client.BaseClient" = None,
) -> None:
    client = _common.ensure_client(client)
    kw = {}
    if description or description == "":
        kw["description"] = description
    if tags:
        kw["tags"] = [{"key": k, "value": v} for k, v in tags.items()]
    execution_retention_data = str(configuration.execution_retention.days)
    client.register_domain(
        name=domain,
        workflowExecutionRetentionPeriodInDays=execution_retention_data,
        **kw,
    )


def tag_domain(
    domain_arn: str,
    tags: t.Dict[str, str],
    client: "botocore.client.BaseClient" = None,
) -> None:
    client = _common.ensure_client(client)
    tags = [{"key": k, "value": v} for k, v in tags.items()]
    client.tag_resource(resourceArn=domain_arn, tags=tags)


def undeprecate_domain(
    domain: str,
    client: "botocore.client.BaseClient" = None,
) -> None:
    client = _common.ensure_client(client)
    client.undeprecate_domain(name=domain)


def untag_domain(
    domain_arn: str,
    tags: t.List[str],
    client: "botocore.client.BaseClient" = None,
) -> None:
    client = _common.ensure_client(client)
    client.untag_resource(resourceArn=domain_arn, tagKeys=tags)
