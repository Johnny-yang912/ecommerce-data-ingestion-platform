from unittest.mock import MagicMock
from fastapi import Request


def make_mock_request(ip="127.0.0.1"):
    r = MagicMock(spec=Request)
    r.client.host = ip
    return r
