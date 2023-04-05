import random
from copy import deepcopy
from datetime import datetime, timezone, date, time
from unittest.mock import patch

from faker import Faker

from pydantic_dynamo.models import UpdateCommand
from pydantic_dynamo.utils import (
    chunks,
    utc_now,
    get_error_code,
    clean_dict,
    build_update_args_for_command,
)
from tests.factories import UpdateCommandFactory
from tests.models import ExtraModel, CountEnum

fake = Faker()


def _random_enum():
    return random.choice([s for s in CountEnum])


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


def test_clean_dict():
    original = {
        "nested": {
            "nested_dict": fake.pydict(),
            "nested_enum": _random_enum(),
            "nested_list_of_dict": [fake.pydict() for _ in range(3)],
            "nested_datetime": datetime.now(tz=timezone.utc),
            "nested_list_of_dt": [datetime.now(tz=timezone.utc) for _ in range(3)],
            "nested_list_of_date": [date.today() for _ in range(3)],
            "nested_list_of_time": [time() for _ in range(3)],
        },
        "dict": fake.pydict(),
        "enum": _random_enum(),
        "list_of_dict": [fake.pydict() for _ in range(3)],
        "datetime": datetime.now(tz=timezone.utc),
        "list_of_dt": [datetime.now(tz=timezone.utc) for _ in range(3)],
        "list_of_date": [date.today() for _ in range(3)],
        "list_of_time": [time() for _ in range(3)],
    }
    og = deepcopy(original)
    cleaned = clean_dict(original)
    assert cleaned["nested"]["nested_dict"].keys() == og["nested"]["nested_dict"].keys()
    assert cleaned["nested"]["nested_enum"] == og["nested"]["nested_enum"].value
    assert len(cleaned["nested"]["nested_list_of_dict"]) == len(og["nested"]["nested_list_of_dict"])
    assert cleaned["nested"]["nested_datetime"] == og["nested"]["nested_datetime"].isoformat()
    assert cleaned["nested"]["nested_list_of_dt"] == [
        dt.isoformat() for dt in og["nested"]["nested_list_of_dt"]
    ]
    assert cleaned["nested"]["nested_list_of_date"] == [
        dt.isoformat() for dt in og["nested"]["nested_list_of_date"]
    ]
    assert cleaned["nested"]["nested_list_of_time"] == [
        t.isoformat() for t in og["nested"]["nested_list_of_time"]
    ]
    assert cleaned["dict"].keys() == og["dict"].keys()
    assert cleaned["enum"] == og["enum"].value
    assert len(cleaned["list_of_dict"]) == len(og["list_of_dict"])
    assert cleaned["datetime"] == og["datetime"].isoformat()
    assert cleaned["list_of_dt"] == [dt.isoformat() for dt in og["list_of_dt"]]
    assert cleaned["list_of_date"] == [dt.isoformat() for dt in og["list_of_date"]]
    assert cleaned["list_of_time"] == [t.isoformat() for t in og["list_of_time"]]


def test_clean_dict_pydantic_base_model():
    unclean = {
        "items": [ExtraModel(a="a", b="b"), ExtraModel(c=ExtraModel(c="c"), d=[ExtraModel(d="d")])]
    }
    cleaned = clean_dict(unclean)
    assert cleaned["items"] == [{"a": "a", "b": "b"}, {"c": {"c": "c"}, "d": [{"d": "d"}]}]


@patch("pydantic_dynamo.utils.utc_now")
def test_build_update_item_arguments(utc_now):
    now = datetime.now(tz=timezone.utc)
    utc_now.return_value = now
    some_string = fake.bs()
    some_enum = _random_enum()
    incr_attr = fake.bs()
    incr_attr2 = fake.bs()
    incr = fake.pyint()
    current_version = fake.pyint()
    el1 = fake.bs()
    el2 = fake.bs()
    el3 = fake.bs()
    command = UpdateCommand(
        current_version=current_version,
        set_commands={"some_attr": some_string, "some_enum": some_enum},
        increment_attrs={incr_attr: incr, incr_attr2: 1},
        append_attrs={"some_list": [el1, el2], "some_list_2": [el3]},
    )
    key = {"partition_key": fake.bs(), "sort_key": fake.bs()}

    update_args = build_update_args_for_command(command, key=key)

    actual_condition = update_args.condition_expression
    assert actual_condition.expression_operator == "AND"
    assert actual_condition._values[1]._values[0].name == "_object_version"
    assert actual_condition._values[1]._values[1] == current_version
    assert actual_condition._values[0]._values[1].expression_operator == "="
    assert actual_condition._values[0]._values[1]._values[0].name == "sort_key"
    assert actual_condition._values[0]._values[1]._values[1] == key["sort_key"]
    assert actual_condition._values[0]._values[0].expression_operator == "="
    assert actual_condition._values[0]._values[0]._values[0].name == "partition_key"
    assert actual_condition._values[0]._values[0]._values[1] == key["partition_key"]

    assert (
        update_args.update_expression == "SET #att0 = :val0, #att1 = :val1, #att2 = :val2, "
        "#att3 = if_not_exists(#att3, :zero) + :val3, "
        "#att4 = if_not_exists(#att4, :zero) + :val4, "
        "#att5 = if_not_exists(#att5, :zero) + :val5, "
        "#att6 = list_append(if_not_exists(#att6, :empty_list), :val6), "
        "#att7 = list_append(if_not_exists(#att7, :empty_list), :val7)"
    )
    assert update_args.attribute_names == {
        "#att0": "some_attr",
        "#att1": "some_enum",
        "#att2": "_timestamp",
        "#att3": incr_attr,
        "#att4": incr_attr2,
        "#att5": "_object_version",
        "#att6": "some_list",
        "#att7": "some_list_2",
    }
    assert update_args.attribute_values == {
        ":zero": 0,
        ":empty_list": [],
        ":val0": some_string,
        ":val1": some_enum.value,
        ":val2": now.isoformat(),
        ":val3": incr,
        ":val4": 1,
        ":val5": 1,
        ":val6": [el1, el2],
        ":val7": [el3],
    }


def test_build_update_item_arguments_null_version():
    command = UpdateCommandFactory(current_version=None)

    update_args = build_update_args_for_command(command)

    assert update_args.condition_expression is None


# TODO: Fix this flaky test
# def test_build_filter_expression():
#     filters = FilterCommand(
#         not_exists={"ne_a", "ne_b"},
#         equals={"eq_a": True, "eq_b": 1},
#         not_equals={"dne_a": "dne_b", "dne_c": False},
#     )
#     expression = _build_filter_expression(filters)
#     assert expression == (
#         Attr("ne_a").not_exists()
#         & Attr("ne_b").not_exists()
#         & Attr("eq_a").eq(True)
#         & Attr("eq_b").eq(1)
#         & Attr("dne_a").ne("dne_b")
#         & Attr("dne_c").ne(False)
#     )
