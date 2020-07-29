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



Feature: Airflow

  Background:
    Given I have initialized Airflow

  @no_python38
  Scenario: Print a list of tasks with Kedro 0.14.2
    Given I have installed kedro version "0.14.2"
    And I have prepared a config file
    And I have run a non-interactive kedro new
    And I have prepared an old data catalog
    And I have executed the kedro command "airflow create"
    And I have executed the kedro command "airflow deploy"
    When I execute the airflow command "list_tasks project-dummy"
    Then I should get a successful exit code
    And I should get a message including "report-accuracy-example-predictions-example-test-y-none"

  @no_python38
  Scenario: Run Airflow task locally with Kedro 0.14.2
    Given I have installed kedro version "0.14.2"
    And I have prepared a config file
    And I have run a non-interactive kedro new
    And I have prepared an old data catalog
    And I have executed the kedro command "airflow create"
    And I have executed the kedro command "airflow deploy"
    When I execute the airflow command "test project-dummy split-data-example-iris-data-parameters-example-test-x-example-test-y-example-train-x-example-train-y 2016-06-01T00:00:00+00:00"
    Then I should get a successful exit code
    And I should get a message including "Loading data from `parameters`"

  @no_python38
  Scenario: Print a list of tasks with Kedro 0.15.9
    Given I have installed kedro version "0.15.9"
    And I have prepared a config file
    And I have run a non-interactive kedro new
    And I have prepared a data catalog
    And I have executed the kedro command "airflow create"
    And I have executed the kedro command "airflow deploy"
    When I execute the airflow command "list_tasks project-dummy"
    Then I should get a successful exit code
    And I should get a message including "report-accuracy-example-predictions-example-test-y-none"

  @no_python38
  Scenario: Run Airflow task locally with Kedro 0.15.9
    Given I have installed kedro version "0.15.9"
    And I have prepared a config file
    And I have run a non-interactive kedro new
    And I have prepared a data catalog
    And I have executed the kedro command "airflow create"
    And I have executed the kedro command "airflow deploy"
    When I execute the airflow command "test project-dummy split-data-example-iris-data-params-example-test-data-ratio-example-test-x-example-test-y-example-train-x-example-train-y 2016-06-01T00:00:00+00:00"
    Then I should get a successful exit code
    And I should get a message including "Loading data from `parameters`"

  Scenario: Print a list of tasks with latest Kedro
    Given I have installed kedro version "latest"
    And I have prepared a config file
    And I have run a non-interactive kedro new
    And I have prepared a data catalog
    And I have executed the kedro command "airflow create"
    And I have executed the kedro command "airflow deploy"
    When I execute the airflow command "list_tasks project-dummy"
    Then I should get a successful exit code
    And I should get a message including "report-accuracy-example-predictions-example-test-y-none"

  Scenario: Run Airflow task locally with latest Kedro
    Given I have installed kedro version "latest"
    And I have prepared a config file
    And I have run a non-interactive kedro new
    And I have prepared a data catalog
    And I have executed the kedro command "airflow create"
    And I have executed the kedro command "airflow deploy"
    When I execute the airflow command "test project-dummy split-data-example-iris-data-params-example-test-data-ratio-example-test-x-example-test-y-example-train-x-example-train-y 2016-06-01T00:00:00+00:00"
    Then I should get a successful exit code
    And I should get a message including "Loading data from `parameters`"
