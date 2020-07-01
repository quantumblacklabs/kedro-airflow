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
import sys
from datetime import datetime, timedelta

from airflow import DAG
from slugify import slugify

from kedro_airflow.runner import AirflowRunner

{%- if kedro_version == 16 %}
from kedro.framework.context import load_context # isort:skip
{%- elif kedro_version == 15 %}
from kedro.context import load_context  # isort:skip
{%- else %}
from kedro.cli.cli import get_project_context  # isort:skip
{#- NOTE: kedro_cli needs to be importable for kedro<0.15.0 #}
# Make `kedro_cli` importable
sys.path.append("{{ project_path }}")
{%- endif %}

# Path to Kedro project directory
project_path = "{{ project_path }}"


# Default arguments for all the Airflow operators
default_args = {
    "owner": "kedro",
    "start_date": datetime(2015, 6, 1),
    "depends_on_past": True,
    "wait_for_downstream": True,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# Arguments for specific Airflow operators, keyed by Kedro node name
def operator_specific_arguments(task_id):
    return {}


# Injest Airflow's context, may modify the data catalog as necessary
# also a good place to init Spark
def process_context(data_catalog, **airflow_context):
    # you could put all the airflow context into the catalog as a new dataset
    for key in ["dag", "conf", "macros", "task", "task_instance", "ti", "var"]:
        del airflow_context[key]  # drop unpicklable things
    data_catalog.add_feed_dict({"airflow_context": airflow_context}, replace=True)

    # or add just the ones you need into Kedro parameters
    parameters = data_catalog.load("parameters")
    parameters["airflow_ds"] = airflow_context["ds"]
    data_catalog.save("parameters", parameters)

    return data_catalog


# Construct a DAG and then call into Kedro to have the operators constructed
dag = DAG(
    slugify("{{ project_name }}"),
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

{% if kedro_version == 14 %}
config = get_project_context('get_config')(project_path)
data_catalog = get_project_context('create_catalog')(config)
pipeline = get_project_context('create_pipeline')()
{%- else %}
_context = load_context(project_path)
data_catalog = _context.catalog
pipeline = _context.pipeline
{%- endif %}

runner = AirflowRunner(
    dag=dag,
    process_context=process_context,
    operator_arguments=operator_specific_arguments,
)

runner.run(pipeline, data_catalog)
