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
import os
from pathlib import Path

import pytest
from semver import VersionInfo

from kedro_airflow.plugin import commands


def a_function():  # pragma: no cover
    pass


@pytest.fixture
def dummy_context(mocker, tmp_path):
    context = mocker.MagicMock()
    context.project_path = tmp_path
    context.project_name = "Hello '-\u2603_-' world !!!"
    return context


def test_create_airflow_dag(cli_runner, tmp_path, mocker, dummy_context):
    project_context = {"context": dummy_context}
    mocker.patch("kedro_airflow.plugin.get_project_context", project_context.get)
    result = cli_runner.invoke(commands, ["airflow", "create"])
    assert result.exit_code == 0
    assert (tmp_path / "airflow_dags" / "hello_world_dag.py").exists()


def test_create_airflow_dag_kedro15(cli_runner, tmp_path, mocker, dummy_context):
    project_context = {"context": dummy_context}
    mocker.patch("kedro_airflow.plugin.KEDRO_VERSION", VersionInfo.parse("0.15.9"))
    mocker.patch("kedro_airflow.plugin.get_project_context", project_context.get)
    result = cli_runner.invoke(commands, ["airflow", "create"])
    assert result.exit_code == 0
    assert (tmp_path / "airflow_dags" / "hello_world_dag.py").exists()


def test_create_airflow_dag_kedro14(cli_runner, mocker):
    mocker.patch("kedro_airflow.plugin.KEDRO_VERSION", VersionInfo.parse("0.14.2"))
    project_context = {
        "project_path": Path.cwd(),
        "project_name": "Hello '-\u2603_-' world !!!",
    }
    mocker.patch("kedro_airflow.plugin.get_project_context", project_context.get)
    result = cli_runner.invoke(commands, ["airflow", "create"])
    assert result.exit_code == 0
    assert (Path.cwd() / "airflow_dags" / "hello_world_dag.py").exists()


def test_deploy_airflow_dag(cli_runner, tmp_path, mocker, dummy_context):
    project_context = {"context": dummy_context}
    mocker.patch("kedro_airflow.plugin.get_project_context", project_context.get)
    copy = mocker.patch("kedro_airflow.plugin.copy")
    mocker.patch.dict(os.environ, {"AIRFLOW_HOME": str(tmp_path)})
    result = cli_runner.invoke(commands, ["airflow", "deploy"])
    assert result.exit_code == 0
    copy.assert_called_once_with(
        str(tmp_path / "airflow_dags/hello_world_dag.py"), str(tmp_path / "dags")
    )
