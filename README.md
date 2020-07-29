# Kedro-Airflow
| `develop`                                                                                                                                                                | `master`                                                                                                                                                               |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [![CircleCI](https://circleci.com/gh/quantumblacklabs/kedro-airflow/tree/develop.svg?style=shield)](https://circleci.com/gh/quantumblacklabs/kedro-airflow/tree/develop) | [![CircleCI](https://circleci.com/gh/quantumblacklabs/kedro-airflow/tree/master.svg?style=shield)](https://circleci.com/gh/quantumblacklabs/kedro-airflow/tree/master) |

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python Version](https://img.shields.io/badge/python-3.6%20%7C%203.7%20%7C%203.8-blue.svg)](https://pypi.org/project/kedro-airflow/)
[![PyPI Version](https://badge.fury.io/py/kedro-airflow.svg)](https://pypi.org/project/kedro-airflow/)
[![Code Style: Black](https://img.shields.io/badge/code%20style-black-black.svg)](https://github.com/ambv/black)

[Apache Airflow](https://github.com/apache/airflow) is a tool for orchestrating complex workflows and data processing pipelines. The Kedro-Airflow plugin can be used for:
- Rapid pipeline creation in the prototyping phase. You can write Python functions in Kedro without worrying about schedulers, daemons, services or having to recreate the Airflow DAG file.
- Automatic dependency resolution in Kedro. This allows you to bypass Airflow's need to specify the order of your tasks.
- Distributing Kedro tasks across many workers. You can also enable monitoring and scheduling of the tasks' runtimes.

## How do I install Kedro-Airflow?

`kedro-airflow` is a Python plugin. To install it:

```bash
pip install kedro-airflow
```

## How do I use Kedro-Airflow?

The Kedro-Airflow plugin adds a `kedro airflow create` CLI command that generates an Airflow DAG file in the `airflow_dags` folder of your project. At runtime, this file translates your Kedro pipeline into Airflow Python operators. This DAG object can be modified according to your needs and you can then deploy your project to Airflow by running `kedro airflow deploy`.

### Prerequisites

The following conditions must be true for Airflow to run your pipeline:
* Your project directory must be available to the Airflow runners in the directory listed at the top of the DAG file.
* Your source code must be on the Python path (by default the DAG file takes care of this).
* All datasets must be explicitly listed in `catalog.yml` and reachable for the Airflow workers. Kedro-Airflow does not support `MemoryDataSet` or datasets that require Spark.
* All local paths in configuration files (notably in `catalog.yml` and `logging.yml`) should be absolute paths and not relative paths.

### Process

1. Run `kedro airflow create` to generate a DAG file for your project.
2. If needed, customize the DAG file as described [below](https://github.com/quantumblacklabs/kedro-airflow/blob/master/README.md#customization).
3. Run `kedro airflow deploy` which will copy the DAG file from the `airflow_dags` folder in your Kedro project into the `dags` folder in the Airflow home directory.

> *Note:* The generated DAG file will be placed in `$AIRFLOW_HOME/dags/` when `kedro airflow deploy` is run, where `AIRFLOW_HOME` is an environment variable. If the environment variable is not defined, Kedro-Airflow will create `~/airflow` and `~/airflow/dags` (if required) and copy the DAG file into it.

## Customization

There are a number of items in the DAG file that you may want to customize including:
- Source location,
- Project location,
- DAG construction,
- Default operator arguments,
- Operator-specific arguments,
- And / or Airflow context and execution date.

The following sections guide you to the appropriate location within the file.

### Source location

The line `sys.path.append("/Users/<user-name>/new-kedro-project/src")` enables Python and Airflow to find your project source.

### Project location

The line `project_path = "/Users/<user-name>/new-kedro-project"` sets the location for your project directory. This is passed to your `get_config` method.

### DAG construction

The construction of the actual DAG object can be altered as needed. You can learn more about how to do this by going through the [Airflow tutorial](https://airflow.apache.org/tutorial.html).

### Default operator arguments

The default arguments for the Airflow operators are contained in the `default_args` dictionary.

### Operator-specific arguments

The `operator_specific_arguments` callback is called to retrieve any additional arguments specific to individual operators. It is passed the Airflow `task_id` and should return a dictionary of additional arguments. For example, to change the number of retries on node named `analysis` to 5 you may have:

```python
def operator_specific_arguments(task_id):
    if task_id == "analysis":
        return {"retries": 5}
    return {}
```
The easiest way to find the correct `task_id` is to use Airflow's `list_tasks` command.

### Airflow context and execution date

The `process_context` callback provides a hook for ingesting Airflow's Jinja context. It is called before every node, receives the context and catalog and must return a catalog. A common use of this is to pick up the execution date and either insert it into the catalog or modify the catalog based on it.

The list of default context variables is available in the Airflow [documentation](https://airflow.apache.org/code.html#default-variables).

## What licence do you use?

Kedro-Airflow is licensed under the [Apache 2.0](https://github.com/quantumblacklabs/kedro-airflow/blob/master/LICENSE.md) License.
