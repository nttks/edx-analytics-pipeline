"""Collect information about payments from third-party sources for financial reporting."""

import csv
import datetime
import logging
import requests
import luigi

from edx.analytics.tasks.url import get_target_from_url
from edx.analytics.tasks.url import url_path_join
from edx.analytics.tasks.util.hive import HivePartition, WarehouseMixin
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin

# Tell urllib3 to switch the ssl backend to PyOpenSSL.
# see https://urllib3.readthedocs.org/en/latest/security.html#pyopenssl
import urllib3.contrib.pyopenssl
urllib3.contrib.pyopenssl.inject_into_urllib3()

log = logging.getLogger(__name__)


class PullFromCybersourceTaskMixin(OverwriteOutputMixin, WarehouseMixin):
    """Define common parameters for Cybersource pull and downstream tasks."""

    host = luigi.Parameter(
        default_from_config={'section': 'cybersource', 'name': 'host'}
    )
    merchant_id = luigi.Parameter(
        default_from_config={'section': 'cybersource', 'name': 'merchant_id'}
    )
    username = luigi.Parameter(
        default_from_config={'section': 'cybersource', 'name': 'username'}
    )
    # Making this 'insignificant' means it won't be echoed in log files.
    password = luigi.Parameter(
        default_from_config={'section': 'cybersource', 'name': 'password'},
        significant=False,
    )


class DailyPullFromCybersourceTask(PullFromCybersourceTaskMixin, luigi.Task):
    """
    A task that reads out of a remote Cybersource account and writes to a file.

    A complication is that this needs to be performed with more than one account
    (or merchant_id), with potentially different credentials.  If possible, create
    the same credentials (username, password) for each account.

    Pulls are made for only a single day.  This is what Cybersource
    supports for these reports, and it allows runs to performed
    incrementally on a daily tempo.

    """
    run_date = luigi.DateParameter(default=datetime.date.today())

    # This is the table that we had been using for gathering and
    # storing historical Cybersource data.  It adds one additional
    # column over the 'PaymentBatchDetailReport' format.
    REPORT_NAME = 'PaymentSubmissionDetailReport'
    REPORT_FORMAT = 'csv'

    def requires(self):
        pass

    def run(self):
        self.remove_output_on_overwrite()
        auth = (self.username, self.password)
        response = requests.get(self.query_url, auth=auth)
        if response.status_code != requests.codes.ok:  # pylint: disable=no-member
            msg = "Encountered status {} on request to Cybersource for {}".format(response.status_code, self.run_date)
            raise Exception(msg)

        with self.output().open('w') as output_file:
            output_file.write(response.content)

    def output(self):
        """Output is in the form {warehouse_path}/cybersource/{CCYY-mm}/cybersource_{merchant}_{CCYYmmdd}.csv"""
        month_year_string = self.run_date.strftime('%Y-%m')  # pylint: disable=no-member
        date_string = self.run_date.strftime('%Y%m%d')  # pylint: disable=no-member
        filename = "cybersource_{merchant_id}_{date_string}.{report_format}".format(
            merchant_id=self.merchant_id,
            date_string=date_string,
            report_format=self.REPORT_FORMAT,
        )
        url_with_filename = url_path_join(self.warehouse_path, "cybersource", month_year_string, filename)
        return get_target_from_url(url_with_filename)

    @property
    def query_url(self):
        """Generate the url to download a report from a Cybersource account."""
        slashified_date = self.run_date.strftime('%Y/%m/%d')  # pylint: disable=no-member
        url = 'https://{host}/DownloadReport/{date}/{merchant_id}/{report_name}.{report_format}'.format(
            host=self.host,
            date=slashified_date,
            merchant_id=self.merchant_id,
            report_name=self.REPORT_NAME,
            report_format=self.REPORT_FORMAT
        )
        return url


class DailyProcessFromCybersourceTask(PullFromCybersourceTaskMixin, luigi.Task):
    """
    A task that reads a local file generated from a daily Cybersource pull, and writes to a TSV file.

    The output file should be readable by Hive, and be in a common format across
    other payment accounts.

    """
    run_date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        args = {
            'run_date': self.run_date,
            'host': self.host,
            'merchant_id': self.merchant_id,
            'username': self.username,
            'password': self.password,
            'warehouse_path': self.warehouse_path,
            'overwrite': self.overwrite,
        }
        return DailyPullFromCybersourceTask(**args)

    def run(self):
        # Read from input and reformat for output.
        self.remove_output_on_overwrite()
        with self.input().open('r') as input_file:
            # Skip the first line, which provides information about the source
            # of the file.  The second line should define the column headings.
            _download_header = input_file.readline()
            reader = csv.DictReader(input_file, delimiter=',')
            with self.output().open('w') as output_file:
                for row in reader:
                    result = [
                        'cybersource',
                        row['merchant_id'],
                        row['batch_date'],
                        row['merchant_ref_number'],  # should equal order_id
                        row['currency'],
                        row['amount'],
                        row['transaction_type'],
                        row['payment_method'],
                    ]
                    output_file.write('\t'.join(result))
                    output_file.write('\n')

    def output(self):
        """
        Output is set up so it can be read in as a Hive table with partitions.

        The form is {warehouse_path}/payments/dt={CCYY-mm-dd}/cybersource_{merchant}.tsv
        """
        date_string = self.run_date.strftime('%Y-%m-%d')  # pylint: disable=no-member
        partition_path_spec = HivePartition('dt', date_string).path_spec
        filename = "cybersource_{}.tsv".format(self.merchant_id)
        url_with_filename = url_path_join(self.warehouse_path, "payments", partition_path_spec, filename)
        return get_target_from_url(url_with_filename)


class IntervalPullFromCybersourceTask(PullFromCybersourceTaskMixin, luigi.Task):
    """Determines a set of dates to pull, and requires them."""

    interval = luigi.DateIntervalParameter()

    required_tasks = None

    def _get_required_tasks(self):
        """Internal method to actually calculate required tasks once."""
        start_date = self.interval.date_a  # pylint: disable=no-member
        end_date = self.interval.date_b  # pylint: disable=no-member
        args = {
            'host': self.host,
            'merchant_id': self.merchant_id,
            'username': self.username,
            'password': self.password,
            'warehouse_path': self.warehouse_path,
            'overwrite': self.overwrite,
        }

        current_date = start_date
        while current_date < end_date:
            args['run_date'] = current_date
            task = DailyProcessFromCybersourceTask(**args)
            # To cut down on the number of tasks that Luigi has to deal with,
            # screen out here the tasks that are already complete.
            if not task.complete():
                yield task
            current_date += datetime.timedelta(days=1)

    def requires(self):
        if not self.required_tasks:
            self.required_tasks = [task for task in self._get_required_tasks()]

        return self.required_tasks

    def output(self):
        return [task.output() for task in self.requires()]