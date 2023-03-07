from datetime import datetime, timezone
from unittest.mock import patch

from faker import Faker

from pydantic_dynamo.utils import chunks, utc_now, get_error_code

fake = Faker()


def test_get_error_code():
    ex = RuntimeError()
    code = fake.bs()
    ex.response = {"Error": {"Code": code}}

    actual = get_error_code(ex)
    assert actual == code


def test_get_error_code_no_code():
    ex = RuntimeError()
    code = fake.bs()
    ex.response = {"Error": {"Zode": code}}

    actual = get_error_code(ex)
    assert actual is None


def test_get_error_code_no_error():
    ex = RuntimeError()
    code = fake.bs()
    ex.response = {"Zerror": {"Zode": code}}

    actual = get_error_code(ex)
    assert actual is None


def test_get_error_code_no_response():
    ex = RuntimeError()
    code = fake.bs()
    ex.zesponse = {"Error": {"Code": code}}

    actual = get_error_code(ex)
    assert actual is None


@patch("pydantic_dynamo.utils.datetime")
def test_utc_now(dt_mock):
    now = datetime.now()
    dt_mock.now.return_value = now
    actual = utc_now()

    assert dt_mock.now.call_args == ((), {"tz": timezone.utc})
    assert actual == now


def test_chunks_lt_size():
    items = [fake.bothify() for _ in range(2)]
    chunked = list(chunks(items, 3))
    assert len(chunked) == 1
    assert len(chunked[0]) == 2


def test_chunks_gt_size():
    items = [fake.bothify() for _ in range(2)]
    chunked = list(chunks(items, 1))
    assert len(chunked) == 2
    for chunk in chunked:
        assert len(chunk) == 1


def test_chunks_gt_size_partial():
    items = [fake.bothify() for _ in range(25)]
    chunked = list(chunks(items, 10))
    assert len(chunked) == 3
    assert len(chunked[0]) == 10
    assert len(chunked[1]) == 10
    assert len(chunked[2]) == 5
