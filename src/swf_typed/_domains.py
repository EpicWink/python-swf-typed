"""SWF domain management."""

import datetime
import functools
import dataclasses
import typing as t

from . import _common

if t.TYPE_CHECKING:
    import botocore.client


@dataclasses.dataclass
class DomainInfo(_common.Deserialisable):
    """Domain details."""

    name: str
    """Domain name."""

    is_deprecated: bool
    """Domain is deprecated and not active."""

    arn: str
    """Domain ARN."""

    description: str = None
    """Domain description."""

    @classmethod
    def from_api(cls, data) -> "DomainInfo":
        return cls(
            name=data["name"],
            is_deprecated=_common.is_deprecated_by_registration_status[data["status"]],
            arn=data["arn"],
            description=data.get("description"),
        )


@dataclasses.dataclass
class DomainConfiguration(_common.Deserialisable):
    """Domain configuration."""

    execution_retention: datetime.timedelta
    """Maximum age of retained executions."""

    @classmethod
    def from_api(cls, data) -> "DomainConfiguration":
        execution_retention_data = data["workflowExecutionRetentionPeriodInDays"]
        return cls(
            execution_retention=datetime.timedelta(days=int(execution_retention_data)),
        )


@dataclasses.dataclass
class DomainDetails(_common.Deserialisable):
    """Domain details and configuration."""

    info: DomainInfo
    """Domain details."""

    configuration: DomainConfiguration
    """Domain configuration."""

    @classmethod
    def from_api(cls, data) -> "DomainDetails":
        return cls(
            info=DomainInfo.from_api(data["domainInfo"]),
            configuration=DomainConfiguration.from_api(data["configuration"]),
        )


def deprecate_domain(
    domain: str,
    client: "botocore.client.BaseClient" = None,
) -> None:
    """Deprecate (deactivate) a domain.

    Args:
        domain: domain to deprecate
        client: SWF client
    """

    client = _common.ensure_client(client)
    client.deprecate_domain(name=domain)


def describe_domain(
    domain: str,
    client: "botocore.client.BaseClient" = None,
) -> DomainDetails:
    """Describe a domain.

    Args:
        domain: domain to describe
        client: SWF client

    Returns:
        domain details and configuration
    """

    client = _common.ensure_client(client)
    response = client.describe_domain(name=domain)
    return DomainDetails.from_api(response)


def list_domains(
    deprecated: bool = False,
    reverse: bool = False,
    client: "botocore.client.BaseClient" = None,
) -> t.Generator[DomainInfo, None, None]:
    """List domains; retrieved semi-lazily.

    Args:
        deprecated: list deprecated domains instead of non-deprecated
        reverse: return results in reverse alphabetical order
        client: SWF client

    Returns:
        domains
    """

    client = _common.ensure_client(client)
    call = functools.partial(
        client.list_domains,
        registrationStatus=_common.registration_status_by_is_deprecated[deprecated],
        reverseOrder=reverse,
    )
    return _common.iter_paged(call, DomainInfo.from_api, "domainInfos")


def get_domain_tags(
    domain_arn: str,
    client: "botocore.client.BaseClient" = None,
) -> t.Dict[str, str]:
    """Get a domain's tags.

    Args:
        domain_arn: domain AWS ARN
        client: SWF client

    Returns:
        domain's resource tags
    """

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
    """Register a new domain.

    Args:
        domain: domain name
        configuration: configuration
        description: domain description
        tags: domain's resource tags
        client: SWF client
    """

    client = _common.ensure_client(client)
    kw = {}
    if description or description == "":
        kw["description"] = description
    if tags:
        kw["tags"] = [dict(key=k, value=v) for k, v in tags.items()]
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
    """Add tags to domain.

    Args:
        domain_arn: domain AWS ARN
        tags: tags to add
        client: SWF client
    """

    client = _common.ensure_client(client)
    tags = [dict(key=k, value=v) for k, v in tags.items()]
    client.tag_resource(resourceArn=domain_arn, tags=tags)


def undeprecate_domain(
    domain: str,
    client: "botocore.client.BaseClient" = None,
) -> None:
    """Undeprecate (reactivate) a domain.

    Args:
        domain: domain to undeprecate
        client: SWF client
    """

    client = _common.ensure_client(client)
    client.undeprecate_domain(name=domain)


def untag_domain(
    domain_arn: str,
    tags: t.List[str],
    client: "botocore.client.BaseClient" = None,
) -> None:
    """Remove tags from a domain.

    Args:
        domain_arn: domain AWS ARN
        tags: tags (keys) to remove
        client: SWF client
    """

    client = _common.ensure_client(client)
    client.untag_resource(resourceArn=domain_arn, tagKeys=tags)
