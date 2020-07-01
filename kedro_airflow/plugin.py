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
""" Kedro plugin for running a project with Airflow """

import os
from pathlib import Path
from shutil import copy

import click
import kedro
from click import secho
from jinja2 import Template
from semver import VersionInfo
from slugify import slugify

KEDRO_VERSION = VersionInfo.parse(kedro.__version__)

if KEDRO_VERSION.match(">=0.16.0"):
    from kedro.framework.cli import get_project_context
else:
    # pylint: disable=no-name-in-module,import-error
    from kedro.cli import get_project_context  # pragma: no cover


@click.group(name="Airflow")
def commands():
    """ Kedro plugin for running a project with Airflow """
    pass


@commands.group(name="airflow")
def airflow_commands():
    """Run project with Airflow"""
    pass


def _get_dag_filename():
    if KEDRO_VERSION.match(">=0.15.0"):
        context = get_project_context("context")
        project_path = context.project_path
        project_name = context.project_name
    else:
        project_path = get_project_context("project_path")
        project_name = get_project_context("project_name")
    dest_dir = project_path / "airflow_dags"
    return dest_dir / (slugify(project_name, separator="_") + "_dag.py")


@airflow_commands.command()
def create():
    """Create an Airflow DAG for a project"""

    src_file = Path(__file__).parent / "dag_template.py"
    dest_file = _get_dag_filename()
    dest_file.parent.mkdir(parents=True, exist_ok=True)
    template = Template(
        src_file.read_text(encoding="utf-8"), keep_trailing_newline=True
    )
    if KEDRO_VERSION.match(">=0.16.0"):
        kedro_version = 16
        context = get_project_context("context")
        project_path = context.project_path
        project_name = context.project_name
    elif KEDRO_VERSION.match(">=0.15.0") and KEDRO_VERSION.match("<0.16.0"):
        kedro_version = 15
        context = get_project_context("context")
        project_path = context.project_path
        project_name = context.project_name
    else:
        kedro_version = 14
        project_name = get_project_context("project_name")
        project_path = get_project_context("project_path")

    dest_file.write_text(
        template.render(
            project_name=project_name,
            project_path=project_path,
            kedro_version=kedro_version,
        ),
        encoding="utf-8",
    )

    secho("")
    secho("An Airflow DAG has been generated in:", fg="green")
    secho(str(dest_file))
    secho("This file should be copied to your Airflow DAG folder.", fg="yellow")
    secho(
        "The Airflow configuration can be customized by editing this file.", fg="green"
    )
    secho("")
    secho(
        "This file also contains the path to the config directory, this directory will need to "
        "be available to Airflow and any workers.",
        fg="yellow",
    )
    secho("")
    secho(
        "Additionally all data sets must have an entry in the data catalog.",
        fg="yellow",
    )
    secho(
        "And all local paths in both the data catalog and log config must be absolute paths.",
        fg="yellow",
    )
    secho("")


@airflow_commands.command()
def deploy():
    """Copy DAG to Airflow home"""
    airflow_home = Path(os.environ.get("AIRFLOW_HOME", "~/airflow"))
    dags_folder = airflow_home.expanduser().resolve() / "dags"
    dags_folder.mkdir(exist_ok=True, parents=True)
    dag_file = _get_dag_filename()
    secho("Copying {} to {}".format(str(dag_file), str(dags_folder / dag_file.name)))
    copy(str(dag_file), str(dags_folder))
