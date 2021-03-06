# Copyright 2020 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
#     or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

"""
This file contains the fixtures that are reusable by any tests within
this directory. You don't need to import the fixtures as pytest will
discover them automatically. More info here:
https://docs.pytest.org/en/latest/fixture.html
"""
from pathlib import Path
from typing import NamedTuple

from click.testing import CliRunner
from kedro import __version__ as kedro_version
from kedro.framework.startup import ProjectMetadata
from kedro.pipeline import Pipeline, node
from pytest import fixture


@fixture(name="cli_runner", scope="session")
def cli_runner():
    runner = CliRunner()
    with runner.isolated_filesystem():
        yield runner


@fixture(scope="session")
def metadata(cli_runner):  # pylint: disable=unused-argument
    # cwd() depends on ^ the isolated filesystem, created by CliRunner()
    project_path = Path.cwd()
    return ProjectMetadata(
        project_path / "pyproject.toml",
        "hello_world",
        "Hello world !!!",
        project_path,
        kedro_version,
        project_path / "src",
    )


def identity(arg):
    return arg


@fixture(scope="session")
def pipeline():
    return Pipeline(
        [node(identity, ["input"], ["output"]), node(identity, ["output"], ["final"])]
    )


@fixture(scope="session")
def context(pipeline):
    class MockContext(NamedTuple):
        pipelines: dict

    return MockContext(pipelines={"__default__": pipeline})


@fixture(scope="session")
def session(context):
    class MockSession:
        @staticmethod
        def load_context():
            return context

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, tb_):
            pass

    return MockSession()
