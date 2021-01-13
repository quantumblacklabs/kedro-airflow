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


"""Behave step definitions for the cli_scenarios feature."""

import yaml
from behave import given, then, when

from features.steps.sh_run import run

OK_EXIT_CODE = 0


@given('I have initialized Airflow with home dir "{home_dir}"')
def init_airflow(context, home_dir):
    context.airflow_dir = context.temp_dir / home_dir
    context.env["AIRFLOW_HOME"] = str(context.airflow_dir)
    res = run([context.airflow, "initdb"], env=context.env)
    assert res.returncode == 0


@given("I have prepared an old data catalog")
def prepare_old_catalog(context):
    config = {
        "example_train_x": {
            "type": "PickleLocalDataSet",
            "filepath": "data/02_intermediate/example_train_x.pkl",
        },
        "example_train_y": {
            "type": "PickleLocalDataSet",
            "filepath": "data/02_intermediate/example_train_y.pkl",
        },
        "example_test_x": {
            "type": "PickleLocalDataSet",
            "filepath": "data/02_intermediate/example_test_x.pkl",
        },
        "example_test_y": {
            "type": "PickleLocalDataSet",
            "filepath": "data/02_intermediate/example_test_y.pkl",
        },
        "example_model": {
            "type": "PickleLocalDataSet",
            "filepath": "data/02_intermediate/example_model.pkl",
        },
        "example_predictions": {
            "type": "PickleLocalDataSet",
            "filepath": "data/02_intermediate/example_predictions.pkl",
        },
    }
    catalog_file = context.root_project_dir / "conf" / "local" / "catalog.yml"
    with catalog_file.open("w") as catalog_file:
        yaml.dump(config, catalog_file, default_flow_style=False)


@given("I have prepared a data catalog")
def prepare_catalog(context):
    config = {
        "example_train_x": {
            "type": "pickle.PickleDataSet",
            "filepath": "data/02_intermediate/example_train_x.pkl",
        },
        "example_train_y": {
            "type": "pickle.PickleDataSet",
            "filepath": "data/02_intermediate/example_train_y.pkl",
        },
        "example_test_x": {
            "type": "pickle.PickleDataSet",
            "filepath": "data/02_intermediate/example_test_x.pkl",
        },
        "example_test_y": {
            "type": "pickle.PickleDataSet",
            "filepath": "data/02_intermediate/example_test_y.pkl",
        },
        "example_model": {
            "type": "pickle.PickleDataSet",
            "filepath": "data/02_intermediate/example_model.pkl",
        },
        "example_predictions": {
            "type": "pickle.PickleDataSet",
            "filepath": "data/02_intermediate/example_predictions.pkl",
        },
    }
    catalog_file = context.root_project_dir / "conf" / "local" / "catalog.yml"
    with catalog_file.open("w") as catalog_file:
        yaml.dump(config, catalog_file, default_flow_style=False)


@given('I have installed kedro version "{version}"')
def install_kedro(context, version):
    """Execute Kedro command and check the status."""
    if version == "latest":
        cmd = [context.pip, "install", "-U", "kedro[pandas]"]
    else:
        cmd = [context.pip, "install", "kedro[pandas]=={}".format(version)]
    res = run(cmd, env=context.env)

    if res.returncode != OK_EXIT_CODE:
        print(res.stdout)
        print(res.stderr)
        assert False


@given("I have installed the kedro project package")
def install_project_package(context):
    """Install the packaged project."""
    cmd = [context.pip, "install", "-e", "src/"]
    res = run(cmd, env=context.env, cwd=str(context.root_project_dir))

    if res.returncode != OK_EXIT_CODE:
        print(res.stdout)
        print(res.stderr)
        assert False


@when('I execute the airflow command "{command}"')
def airflow_command(context, command):
    split_command = command.split()
    cmd = [context.airflow] + split_command
    context.result = run(cmd, env=context.env, cwd=str(context.root_project_dir))


@then('I should get a message including "{msg}"')
def check_message_printed(context, msg):
    """Check that specified message is printed to stdout (can be a segment)."""
    stdout = context.result.stdout
    assert msg in stdout, (
        "Expected the following message segment to be printed on stdout: "
        "{exp_msg},\nbut got {actual_msg}".format(exp_msg=msg, actual_msg=stdout)
    )


@given("I have prepared a config file")
def create_configuration_file(context):
    """Behave step to create a temporary config file
    (given the existing temp directory)
    and store it in the context.
    """
    context.config_file = context.temp_dir / "config.yml"
    context.project_name = "project-dummy"

    root_project_dir = context.temp_dir / context.project_name
    context.root_project_dir = root_project_dir
    config = {
        "project_name": context.project_name,
        "repo_name": context.project_name,
        "output_dir": str(context.temp_dir),
        "python_package": context.project_name.replace("-", "_"),
    }
    with context.config_file.open("w") as config_file:
        yaml.dump(config, config_file, default_flow_style=False)


@given("I have run a non-interactive kedro new")
def create_project_from_config_file(context):
    """Behave step to run kedro new
    given the config I previously created.
    """
    res = run(
        [
            context.kedro,
            "new",
            "-c",
            str(context.config_file),
            "--starter",
            "pandas-iris",
        ],
        env=context.env,
        cwd=str(context.temp_dir),
    )
    if res.returncode != OK_EXIT_CODE:
        print(res.stdout)
        print(res.stderr)
        assert False


@given('I have executed the kedro command "{command}"')
def exec_make_target_checked(context, command):
    """Execute Makefile target"""
    make_cmd = [context.kedro] + command.split()

    res = run(make_cmd, env=context.env, cwd=str(context.root_project_dir))

    if res.returncode != OK_EXIT_CODE:
        print(res.stdout)
        print(res.stderr)
        assert False


@then("I should get a successful exit code")
def check_status_code(context):
    if context.result.returncode != OK_EXIT_CODE:
        print(context.result.stdout)
        print(context.result.stderr)
        assert False, "Expected exit code {}" " but got {}".format(
            OK_EXIT_CODE, context.result.returncode
        )
