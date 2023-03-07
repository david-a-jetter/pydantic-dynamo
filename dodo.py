from doit.action import CmdAction


def python_version():
    return "python --version"


def poetry_version():
    return "poetry --version"


def poetry_env_info():
    return "poetry env info"


info = [CmdAction(python_version), CmdAction(poetry_version), CmdAction(poetry_env_info)]


def install_deps():
    return "poetry install"


def task_test():
    return {
        "actions": [
            *info,
            CmdAction(install_deps),
            CmdAction(
                "poetry run python -m pytest tests/ --cov=pydantic_dynamo --cov-report xml:coverage.xml"
            ),
            CmdAction("poetry run black pydantic_dynamo"),
            CmdAction("poetry run black tests"),
            CmdAction("poetry run mypy pydantic_dynamo tests"),
            CmdAction("poetry run flake8 pydantic_dynamo --ignore=E203,W503"),
            CmdAction("poetry run flake8 tests --ignore=E203,W503"),
        ],
        "verbosity": 2,
    }
