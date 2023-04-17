import random
from copy import deepcopy
from datetime import datetime, timezone, date, time
from typing import Dict
from unittest.mock import patch, MagicMock

import pytest
from faker import Faker

from pydantic_dynamo.exceptions import RequestObjectStateError
from pydantic_dynamo.models import UpdateCommand, FilterCommand
from pydantic_dynamo.utils import (
    chunks,
    utc_now,
    get_error_code,
    clean_dict,
    build_update_args_for_command,
    internal_timestamp,
    execute_update_item,
    validate_command_for_schema,
    validate_filters_for_schema,
)
from tests.factories import (
    UpdateCommandFactory,
    UpdateItemArgumentsFactory,
    ExampleFactory,
    FieldModelFactory,
)
from tests.models import ExtraModel, CountEnum, Example, FieldModel

fake = Faker()


def _random_enum():
    return random.choice([s for s in CountEnum])


@patch("pydantic_dynamo.utils.datetime")
def test_utc_now(dt_mock):
    now = datetime.now()
    dt_mock.now.return_value = now
    actual = utc_now()

    assert dt_mock.now.call_args == ((), {"tz": timezone.utc})
    assert actual == now


@patch("pydantic_dynamo.utils.utc_now")
def test_internal_timestamp(utc_now):
    now = fake.date_time()
    utc_now.return_value = now

    actual = internal_timestamp()
    assert actual == {"_timestamp": now.isoformat()}


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


def test_execute_update_item():
    table = MagicMock()
    key = {fake.bothify(): fake.bothify() for _ in range(2)}
    args = UpdateItemArgumentsFactory()

    execute_update_item(table, key, args)

    assert table.update_item.call_args == (
        (),
        {
            "Key": key,
            "UpdateExpression": args.update_expression,
            "ExpressionAttributeNames": args.attribute_names,
            "ExpressionAttributeValues": args.attribute_values,
            "ConditionExpression": args.condition_expression,
        },
    )


def test_execute_update_item_condition_none():
    table = MagicMock()
    key = {fake.bothify(): fake.bothify() for _ in range(2)}
    args = UpdateItemArgumentsFactory(condition_expression=None)

    execute_update_item(table, key, args)

    assert table.update_item.call_args == (
        (),
        {
            "Key": key,
            "UpdateExpression": args.update_expression,
            "ExpressionAttributeNames": args.attribute_names,
            "ExpressionAttributeValues": args.attribute_values,
        },
    )


@patch("pydantic_dynamo.utils.get_error_code")
def test_execute_update_conditional_check_failed(get_error_code):
    error_code = "ConditionalCheckFailedException"
    get_error_code.return_value = error_code
    table = MagicMock()
    key = {fake.bothify(): fake.bothify() for _ in range(2)}
    args = UpdateItemArgumentsFactory()

    db_ex = Exception(fake.bs())
    table.update_item.side_effect = db_ex
    with pytest.raises(RequestObjectStateError) as ex:
        execute_update_item(table, key, args)

    assert str(key) in str(ex)
    assert get_error_code.call_args == ((db_ex,), {})


@patch("pydantic_dynamo.utils.get_error_code")
def test_execute_update_some_other_exception(get_error_code):
    error_code = fake.bs()
    get_error_code.return_value = error_code
    table = MagicMock()
    key = {fake.bothify(): fake.bothify() for _ in range(2)}
    args = UpdateItemArgumentsFactory()

    db_ex = Exception(fake.bs())
    table.update_item.side_effect = db_ex
    with pytest.raises(Exception) as ex:
        execute_update_item(table, key, args)

    assert ex.value == db_ex


def test_clean_dict():
    nested_composed = ExampleFactory()
    composed = ExampleFactory()
    dict_input = {
        "nested": {
            "nested_dict": fake.pydict(),
            "nested_enum": _random_enum(),
            "nested_list_of_dict": [fake.pydict() for _ in range(3)],
            "nested_datetime": datetime.now(tz=timezone.utc),
            "nested_list_of_dt": [datetime.now(tz=timezone.utc) for _ in range(3)],
            "nested_list_of_date": [date.today() for _ in range(3)],
            "nested_list_of_time": [time() for _ in range(3)],
            "nested_composed": nested_composed,
        },
        "dict": fake.pydict(),
        "enum": _random_enum(),
        "list_of_dict": [fake.pydict() for _ in range(3)],
        "datetime": datetime.now(tz=timezone.utc),
        "list_of_dt": [datetime.now(tz=timezone.utc) for _ in range(3)],
        "list_of_date": [date.today() for _ in range(3)],
        "list_of_time": [time() for _ in range(3)],
        "composed": composed,
    }
    og = deepcopy(dict_input)
    cleaned = clean_dict(dict_input)
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
    nested_composed_expected = _example_to_expected(nested_composed)
    assert sorted(cleaned["nested"]["nested_composed"].pop("set_field")) == sorted(
        nested_composed_expected.pop("set_field")
    )
    assert cleaned["nested"]["nested_composed"] == nested_composed_expected
    assert cleaned["dict"].keys() == og["dict"].keys()
    assert cleaned["enum"] == og["enum"].value
    assert len(cleaned["list_of_dict"]) == len(og["list_of_dict"])
    assert cleaned["datetime"] == og["datetime"].isoformat()
    assert cleaned["list_of_dt"] == [dt.isoformat() for dt in og["list_of_dt"]]
    assert cleaned["list_of_date"] == [dt.isoformat() for dt in og["list_of_date"]]
    assert cleaned["list_of_time"] == [t.isoformat() for t in og["list_of_time"]]

    # Need to explicitly assert on set_field because it's not reliably sorted
    expected = _example_to_expected(composed)
    assert sorted(cleaned["composed"].pop("set_field")) == sorted(expected.pop("set_field"))
    assert cleaned["composed"] == expected


# dict_field only supports strings or instances of FieldModel
def _example_to_expected(example: Example) -> Dict:
    return {
        "dict_field": {
            k: v.dict() if isinstance(v, FieldModel) else v for k, v in example.dict_field.items()
        },
        "model_field": {
            "test_field": example.model_field.test_field,
            "failures": example.model_field.failures,
        },
        "list_field": [v.isoformat() if isinstance(v, datetime) else v for v in example.list_field],
        "set_field": [v.isoformat() if isinstance(v, datetime) else v for v in example.set_field],
        "date_field": example.date_field.isoformat(),
        "time_field": example.time_field.isoformat(),
        "datetime_field": example.datetime_field.isoformat(),
        "enum_field": example.enum_field.value,
        "int_field": example.int_field,
        "optional_field": example.optional_field,
    }


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
    some_example = ExampleFactory()
    incr_attr = fake.bs()
    incr_attr2 = fake.bs()
    incr = fake.pyint()
    current_version = fake.pyint()
    el1 = fake.bs()
    el2 = fake.bs()
    el3 = fake.bs()
    expiry = datetime.now(tz=timezone.utc)
    command = UpdateCommand(
        current_version=current_version,
        set_commands={
            "some_attr": some_string,
            "some_enum": some_enum,
            "some_object": some_example.dict(),
        },
        increment_attrs={incr_attr: incr, incr_attr2: 1},
        append_attrs={"some_list": [el1, el2], "some_list_2": [el3]},
        expiry=expiry,
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
        update_args.update_expression == "SET #att0 = :val0, #att1 = :val1, "
        "#att2.#att3 = :val2, #att2.#att4 = :val3, "
        "#att2.#att5 = :val4, #att2.#att6 = :val5, "
        "#att2.#att7 = :val6, #att2.#att8 = :val7, "
        "#att2.#att9 = :val8, #att2.#att10 = :val9, "
        "#att2.#att11 = :val10, #att2.#att12 = :val11, "
        "#att13 = :val12, #att14 = :val13, "
        "#att15 = if_not_exists(#att15, :zero) + :val14, "
        "#att16 = if_not_exists(#att16, :zero) + :val15, "
        "#att17 = if_not_exists(#att17, :zero) + :val16, "
        "#att18 = list_append(if_not_exists(#att18, :empty_list), :val17), "
        "#att19 = list_append(if_not_exists(#att19, :empty_list), :val18)"
    )
    assert update_args.attribute_names == {
        "#att0": "some_attr",
        "#att1": "some_enum",
        "#att2": "some_object",
        "#att3": "dict_field",
        "#att4": "model_field",
        "#att5": "list_field",
        "#att6": "set_field",
        "#att7": "date_field",
        "#att8": "time_field",
        "#att9": "datetime_field",
        "#att10": "enum_field",
        "#att11": "int_field",
        "#att12": "optional_field",
        "#att13": "_timestamp",
        "#att14": "_ttl",
        "#att15": incr_attr,
        "#att16": incr_attr2,
        "#att17": "_object_version",
        "#att18": "some_list",
        "#att19": "some_list_2",
    }
    assert sorted(update_args.attribute_values.pop(":val5")) == sorted(list(some_example.set_field))
    assert update_args.attribute_values == {
        ":zero": 0,
        ":empty_list": [],
        ":val0": some_string,
        ":val1": some_enum.value,
        ":val2": {k: v.dict() for k, v in some_example.dict_field.items()},
        ":val3": some_example.model_field.dict(),
        ":val4": some_example.list_field,
        ":val6": some_example.date_field.isoformat(),
        ":val7": some_example.time_field.isoformat(),
        ":val8": some_example.datetime_field.isoformat(),
        ":val9": some_example.enum_field.value,
        ":val10": some_example.int_field,
        ":val11": None,
        ":val12": now.isoformat(),
        ":val13": int(expiry.timestamp()),
        ":val14": incr,
        ":val15": 1,
        ":val16": 1,
        ":val17": [el1, el2],
        ":val18": [el3],
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


def test_validate_command_for_schema_happy_path():
    validate_command_for_schema(
        Example.schema(),
        UpdateCommand(
            set_commands={
                "dict_field": fake.pydict(),
                "model_field": FieldModelFactory().dict(),
                "set_field": set((fake.bothify() for _ in range(2))),
            },
            increment_attrs={"int_field": fake.pyint()},
            append_attrs={"list_field": [fake.bs()]},
        ),
    )


def test_validate_command_for_schema_invalid_set_command():
    command = UpdateCommandFactory()

    with pytest.raises(ValueError) as ex:
        validate_command_for_schema(FieldModel.schema(), command)

    exception_value = str(ex.value)
    assert "FieldModel" in exception_value
    for attr in command.set_commands.keys():
        assert attr in exception_value


def test_validate_command_for_schema_non_dict_field_to_dict():
    command = UpdateCommand(set_commands={"date_field": {fake.bs(): fake.bs()}})

    with pytest.raises(ValueError) as ex:
        validate_command_for_schema(Example.schema(), command)

    exception_value = str(ex.value)
    assert "Example" in exception_value
    assert "date_field" in exception_value


def test_validate_command_for_schema_invalid_nested_model_field():
    bs_key = fake.bs()
    command = UpdateCommand(set_commands={"model_field": {bs_key: fake.bs()}})

    with pytest.raises(ValueError) as ex:
        validate_command_for_schema(Example.schema(), command)

    exception_value = str(ex.value)
    assert "Example" in exception_value
    assert bs_key in exception_value


def test_validate_command_for_schema_invalid_incr_command():
    command = UpdateCommandFactory(set_commands={})

    with pytest.raises(ValueError) as ex:
        validate_command_for_schema(FieldModel.schema(), command)

    exception_value = str(ex.value)
    assert "FieldModel" in exception_value
    for attr in command.increment_attrs:
        assert attr in exception_value


def test_validate_command_for_schema_non_integer_incr_command():
    command = UpdateCommand(increment_attrs={"test_field": 1})

    with pytest.raises(ValueError) as ex:
        validate_command_for_schema(FieldModel.schema(), command)

    exception_value = str(ex.value)
    assert "FieldModel" in exception_value
    for attr in command.increment_attrs:
        assert attr in exception_value


def test_validate_filters_for_schema_invalid_filter():
    filters = FilterCommand(not_exists={fake.bs()})

    with pytest.raises(ValueError) as ex:
        validate_filters_for_schema(FieldModel.schema(), filters)

    exception_value = str(ex.value)
    assert "FieldModel" in exception_value
    for attr in filters.not_exists:
        assert attr in exception_value
