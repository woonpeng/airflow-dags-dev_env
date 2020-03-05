# coding=utf-8
"""
Utility functions for cron configuration
"""
from datetime import datetime

from croniter import croniter


def compute_days_between_runs(cron_config):
    """Determines the interval of the cron configuration. Can only be daily or
    weekly.

    :param cron_config: cron configuration for running the airflow job
    :type cron_config: str
    :return: 1 (daily), 7 (weekly)
    :rtype: int
    """

    base_dttm = datetime(2017, 11, 1, 0, 0)
    dttm_iter = croniter(cron_config, base_dttm)
    dttm1 = dttm_iter.get_next(datetime)
    dttm2 = dttm_iter.get_next(datetime)
    num_days_between_scheduled_runs = (
        (dttm2 - dttm1).total_seconds() / (24.0 * 60.0 * 60.0)
    )

    if num_days_between_scheduled_runs == 1.0 and cron_config[-5:] == '* * *':
        return 1
    elif num_days_between_scheduled_runs == 7.0 and cron_config[-5:-2] == '* *':
        return 7
    else:
        raise ValueError('Only allow airflow to run daily or weekly')
