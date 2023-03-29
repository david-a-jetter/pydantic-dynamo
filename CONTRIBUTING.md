# Contributing Guide

At the moment, this project isn't well set up for external contributions. That said, 
the content below should assist in understanding how to build and test this library.

## TODOs

1. Adopt a code of conduct
2. Add continuous integration checks
3. Document style and structure philosophy
4. Automate package version publication

## Build and Test

### [Poetry](https://python-poetry.org/)

All external dependencies are managed through the excellent Poetry package manager. Poetry 
is also used to build and publish the package to PyPI.

Poetry can be [installed](https://python-poetry.org/docs/#installation) a number of ways. 

Once installed, run `poetry install` from the repository root directory to install both 
production and development packages.

### Build Tasks
The [dodo.py](./dodo.py) contains build task commands based on the [doit](https://pydoit.org/) 
build tool. There are three tasks that can be run. Assuming you do not have a machine-wide 
installation of doit, these must all be run prefixed with `poetry run` to ensure the 
[Poetry managed virtual environment](https://python-poetry.org/docs/managing-environments/) is 
being used, which will include the doit package.

`poetry run doit test` will run all `pytest` on all tests in the `./tests` directory, as well as
ensure [Black](https://github.com/psf/black), [mypy](https://www.mypy-lang.org/), and 
[flake8](https://flake8.pycqa.org/en/latest/) checks pass.

`poetry run doit build` will run the `test` task above, as well as 
[poetry build](https://python-poetry.org/docs/cli/#build) to generate the source and wheel
distributions.

`poetry run doit publish` will run the `build` task above, as well as attempt to publish the
generated distributions to [PyPI](https://pypi.org/project/pydantic-dynamo/). You probably
won't have access to this.

## Adding Functionality

The primary class is defined in [repository.py](./pydantic_dynamo/repository.py) and contains nearly
all functionality. 
