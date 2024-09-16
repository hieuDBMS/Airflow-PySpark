import pytest
from airflow.models import DagBag


class TestDagValidation:
    LOAD_SECONDS_THRESHOLD = 2
    REQUIRED_EMAIL = "hieulata26102002@gmail.com"
    EXPECTED_NUMBER_OF_DAGS = 12

    def test_import_dags(self, dagbag):
        """
            Verify that Airflow is able to import all DAGs
            in the repo
            - check for typos
            - check for cycles
        """
        assert len(dagbag.import_errors) == 0, "DAG failures detected! Got: {}".format(
            dagbag.import_errors
        )

    def test_time_import_dags(self, dagbag):
        """
            Verify that DAGS load fast enough
            - check for loading time
        """
        stats = dagbag.dagbag_stats  # [FileLoadStat(file='/EPOS_practice/EPOS_practice.py', duration=datetime.timedelta(seconds=1, microseconds=65537), dag_num=1, task_num=12, dags="['EPOS_practice']"), FileLoadStat(file='/project_pyspark/stock_market.py', duration=datetime.timedelta(microseconds=50937), dag_num=1, task_num=6, dags="['stock_market']"), ... ]
        slow_dags = list(filter(lambda f: f.duration.total_seconds() > self.LOAD_SECONDS_THRESHOLD, stats))
        res = ', '.join(map(lambda f: f.file[1:], slow_dags))

        assert len(slow_dags) == 0, "The following DAGs take more than {0}s to load: {1}".format(
            self.LOAD_SECONDS_THRESHOLD,
            res
        )

    @pytest.mark.skip(reason="Not yet added to the DAGs")
    def test_default_args_email(self, dagbag):
        """
            Verify that DAGs have the required email
            - Check email
        """
        no_email_dags = []
        for dag_id, dag in dagbag.dags.items():
            emails = dag.default_args.get('email', [])
            if len(emails) == 0:
                no_email_dags.append(dag_id)

        assert len(no_email_dags) == 0, "The email {0} for sending alerts is missing from the DAG {1}".format(
            self.REQUIRED_EMAIL,
            no_email_dags
        )

    @pytest.mark.skip(reason="Not yet added to the DAGs")
    def test_default_args_retries(self, dagbag):
        """
            Verify that DAGs have the required number of retries
            - Check retries
        """
        no_retries_dags = []
        for dag_id, dag in dagbag.dags.items():
            retries = dag.default_args.get('retries', None)
            if retries is None:
                no_retries_dags.append(dag_id)

        assert len(no_retries_dags) == 0, "You must specify a number of retries in the DAG: {0}".format(
            no_retries_dags
        )

    @pytest.mark.skip(reason="Not yet added to the DAGs")
    def test_default_args_retry_delay(self, dagbag):
        """
            Verify that DAGs have the required retry_delay expressed in seconds
            - Check retry_delay
        """
        no_retry_delay_dags = []
        for dag_id, dag in dagbag.dags.items():
            retries = dag.default_args.get('retry_delay', None)
            if retries is None:
                no_retry_delay_dags.append(dag_id)

        assert len(no_retry_delay_dags) == 0, "You must specify a retry delay (seconds) in the DAG: {0}".format(
            no_retry_delay_dags
        )

    def test_number_of_dags(self, dagbag):
        """
            Verify that there is the right number of DAGs in the dag folder
            - Check number of dags
        """
        stats = dagbag.dagbag_stats
        dag_num = sum([dag.dag_num for dag in stats])
        assert dag_num == self.EXPECTED_NUMBER_OF_DAGS, "Wrong number of dags, {0} expected got {1} (Can be due to cycles!)".format(
            self.EXPECTED_NUMBER_OF_DAGS,
            dag_num
        )