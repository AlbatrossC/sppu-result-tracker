from unittest.mock import Mock, patch

import pytest
import requests

from src.extract import FetchError, fetch_html


def response(status, text="", headers=None):
    result = Mock(spec=requests.Response)
    result.status_code = status
    result.text = text
    result.content = text.encode()
    result.headers = headers or {}
    return result


@patch("src.extract.time.sleep", return_value=None)
def test_fetch_retries_temporary_server_error(_sleep):
    session = Mock(spec=requests.Session)
    session.get.side_effect = [response(503), response(200, "<html>ok</html>")]

    assert fetch_html("https://example.test", session=session) == "<html>ok</html>"
    assert session.get.call_count == 2


@patch("src.extract.time.sleep", return_value=None)
def test_fetch_retries_timeout_then_raises(_sleep):
    session = Mock(spec=requests.Session)
    session.get.side_effect = requests.Timeout("slow")

    with pytest.raises(FetchError, match="4 attempts"):
        fetch_html("https://example.test", session=session)
    assert session.get.call_count == 4


def test_fetch_does_not_retry_permanent_client_error():
    session = Mock(spec=requests.Session)
    session.get.return_value = response(404)

    with pytest.raises(FetchError, match="HTTP 404"):
        fetch_html("https://example.test", session=session)
    assert session.get.call_count == 1
