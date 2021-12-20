# !/usr/bin/python
#
# Copyright Phong Pham Hong <phongpham1805@gmail.com>
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
__author__ = 'phongphamhong'

import datetime
import json
import time

import six
from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow.exceptions import AirflowTaskTimeout, AirflowException
from airflow.models import BaseOperator
from airflow.models.dagrun import DagRun
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils import timezone
from airflow.utils.decorators import apply_defaults
from dmp.helper.log import logger


class SynTriggerDagRunOperator(BaseOperator):
    """
        Triggers a DAG run for a specified ``dag_id``

        :param trigger_dag_id: the dag_id to trigger (templated)
        :type trigger_dag_id: str
        :param python_callable: a reference to a python function that will be
            called while passing it the ``context`` object and a placeholder
            object ``obj`` for your callable to fill and return if you want
            a DagRun created. This ``obj`` object contains a ``run_id`` and
            ``payload`` attribute that you can modify in your function.
            The ``run_id`` should be a unique identifier for that DAG run, and
            the payload has to be a picklable object that will be made available
            to your tasks while executing that DAG run. Your function header
            should look like ``def foo(context, dag_run_obj):``
        :type python_callable: python callable
        :param is_async: run with mode async or sync
        :param execution_date: Execution date for the dag (templated)
        :type execution_date: str or datetime.datetime
        """
    template_fields = ('trigger_dag_id', 'execution_date')
    ui_color = '#ffefeb'

    @apply_defaults
    def __init__(
        self,
        trigger_dag_id,
        python_callable=None,
        execution_date=None,
        is_async: bool = False,
        trigger_from: str = 'nowhere',
        exec_date_from_parent: bool = True,
        *args, **kwargs):
        super(SynTriggerDagRunOperator, self).__init__(*args, **kwargs)
        self.python_callable = python_callable
        self.trigger_dag_id = trigger_dag_id
        self.is_async = is_async
        self.trigger_from = trigger_from
        self.exec_date_from_parent = exec_date_from_parent
        if isinstance(execution_date, datetime.datetime):
            self.execution_date = execution_date.isoformat()
        elif isinstance(execution_date, six.string_types):
            self.execution_date = execution_date
        elif execution_date is None:
            self.execution_date = execution_date
        else:
            raise TypeError(
                'Expected str or datetime.datetime type '
                'for execution_date. Got {}'.format(
                    type(execution_date)))

    def execute(self, context):
        from airflow.models.dagbag import DagBag
        from airflow import settings
        from sqlalchemy.exc import IntegrityError
        time_start = time.time()
        logger.info("Execution date is: %s" % self.execution_date)
        if self.execution_date is not None:
            run_id = 'trig__{}_{}'.format(self.execution_date, self.trigger_from)
            self.execution_date = timezone.parse(self.execution_date)
        else:
            run_id = 'trig__{}_{}'.format(timezone.utcnow().isoformat(), self.trigger_from)

        logger.info("Run_id: %s" % run_id)
        dro = TriggerDagRunOperator(run_id=run_id)
        if self.python_callable is not None:
            dro = self.python_callable(context, dro)
        exec_date = self.execution_date if not self.exec_date_from_parent else context[
            'execution_date']
        if dro:
            run_by_rid = True
            try:
                trigger_dag(dag_id=self.trigger_dag_id,
                            run_id=dro.run_id,
                            conf=json.dumps(dro.payload),
                            execution_date=exec_date,
                            replace_microseconds=False,
                            )
                dag_run = DagRun.find(dag_id=self.trigger_dag_id, run_id=run_id)[0]
            except IntegrityError as err:
                logger.error("Duplicate entry: %s-%s" % (self.trigger_dag_id, dro.run_id))
                logger.info("Start run clear dag to trigger")
                dbag = DagBag(settings.DAGS_FOLDER)
                clear = dbag.get_dag(self.trigger_dag_id)
                clear.clear(
                    start_date=exec_date,
                    end_date=exec_date,
                    only_failed=False,
                    only_running=False,
                    confirm_prompt=False,
                    reset_dag_runs=True,
                    include_subdags=False,
                    dry_run=False
                )
                run_by_rid = False
                dag_run = DagRun.find(dag_id=self.trigger_dag_id, execution_date=exec_date)[0]

            if self.is_async:
                logger.info("Run with mode: Async...")
                return True
            logger.info("Run with mode: Sync...")
            count = 0
            while dag_run is None or dag_run.get_state() == 'running':
                if count % 10 == 0:
                    logger.info("waiting for dag {%s} - {%s} running to complete..." % (self.trigger_dag_id, run_id))
                    if logger is not None:
                        logger.info(dag_run.get_state())
                count += 1
                time.sleep(10)
                if run_by_rid:
                    dag_run = DagRun.find(dag_id=self.trigger_dag_id, run_id=run_id)[0]
                else:
                    dag_run = DagRun.find(dag_id=self.trigger_dag_id, execution_date=exec_date)[0]
                time_run = time.time() - time_start
                if self.execution_timeout is not None and datetime.timedelta(seconds=time_run) > self.execution_timeout:
                    logger.error("Waiting dag {%s} - {%s} running: Timeout..." % (self.trigger_dag_id, run_id))
                    raise AirflowTaskTimeout
            if dag_run.get_state() == 'failed':
                logger.error("Trigger Dag {%s}: Failed!" % run_id)
                raise AirflowException
            return 0
        else:
            self.log.info("Criteria not met, moving on")
