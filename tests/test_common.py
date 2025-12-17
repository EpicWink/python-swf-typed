"""Test ``swf_typed._common``."""

import time
import socket
import typing as t
import datetime
import unittest.mock

import boto3
import pytest

# noinspection PyProtectedMember
import swf_typed._common

# noinspection PyProtectedMember
import swf_typed._exceptions


@pytest.mark.parametrize("status", ["REGISTERED", "DEPRECATED"])
def test_registration_status_by_is_deprecated(status: str) -> None:
    """Test reverse map ``registration_status_by_is_deprecated``."""
    assert (
        swf_typed._common.registration_status_by_is_deprecated[
            swf_typed._common.is_deprecated_by_registration_status[status]
        ]
        == status
    )


def test_unset_repr() -> None:
    """Test ``unset`` sentinel string representation."""
    assert repr(swf_typed._common.unset)[-2:] == "()"


def test_unset_str() -> None:
    """Test ``unset`` sentinel pretty-printed representation."""
    assert str(swf_typed._common.unset) == "< not given >"


def test_unset_bool() -> None:
    """Test ``unset`` sentinel truthiness value."""
    assert bool(swf_typed._common.unset) is False


@pytest.mark.parametrize("client", [
    pytest.param(None, id="new"),
    pytest.param(unittest.mock.Mock(), id="existing"),
])  # fmt: skip
def test_ensure_client(client: t.Union[unittest.mock.Mock, None]) -> None:
    """Test ``ensure_client``."""
    boto3_client_mock = unittest.mock.Mock()
    boto3_client_patch = unittest.mock.patch.object(
        target=boto3, attribute="client", new=boto3_client_mock
    )

    with boto3_client_patch:
        result = swf_typed._common.ensure_client(client)

    if client:
        assert result is client or result.foo() is client.foo()  # wrapped
        boto3_client_mock.assert_not_called()
    else:
        assert (
            result is boto3_client_mock.return_value
            or result.foo() is boto3_client_mock.return_value.foo()  # wrapped
        )
        boto3_client_mock.assert_called_once_with("swf")


@pytest.mark.parametrize(("timeout_data", "expected"), [
    pytest.param("NONE", None, id="none"),
    pytest.param("0", datetime.timedelta(seconds=0), id="0"),
    pytest.param("42", datetime.timedelta(seconds=42), id="42"),
])  # fmt: skip
def test_parse_timeout(timeout_data: str, expected: t.Union[int, None]) -> None:
    """Test ``parse_timeout``."""
    assert swf_typed._common.parse_timeout(timeout_data) == expected


def test_iter_paged() -> None:
    """Test ``iter_paged``."""

    def side_effect(**kwargs) -> t.Dict[str, t.Any]:
        time.sleep(0.01)
        return responses[kwargs.get("nextPageToken")]

    responses = {
        None: {"foo": [{"x": 1}, {"x": 2}], "nextPageToken": "page-2"},
        "page-2": {"foo": [{"x": 3}, {"x": 4}], "nextPageToken": "page-3"},
        "page-3": {"foo": [{"x": 5}, {"x": 6}]},
    }
    call_mock = unittest.mock.Mock(side_effect=side_effect)

    results_iter = swf_typed._common.iter_paged(
        call=call_mock, model=lambda x: x["x"], data_key="foo"
    )

    assert iter(results_iter) is results_iter
    assert list(results_iter) == [1, 2, 3, 4, 5, 6]

    assert call_mock.mock_calls == [
        unittest.mock.call(),
        unittest.mock.call(nextPageToken="page-2"),
        unittest.mock.call(nextPageToken="page-3"),
    ]


def test_polling_socket_timeout() -> None:
    """Test ``polling_socket_timeout``."""
    original = socket.getdefaulttimeout()
    # noinspection PyTypeChecker
    with swf_typed._common.polling_socket_timeout(
        timeout=datetime.timedelta(seconds=0.1),
    ):
        assert socket.getdefaulttimeout() == 0.1
    assert socket.getdefaulttimeout() == original
