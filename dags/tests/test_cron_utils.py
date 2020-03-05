"""Unit testing of cron_config_tools module."""
import pytest

from fncore.utils.cron_config_tools import compute_days_between_runs


def test_cron_config_util():
    """Unit test module for determining the number of days between runs."""
    assert compute_days_between_runs("0 20 * * *") == 1
    assert compute_days_between_runs("0 20 * * 7") == 7
    with pytest.raises(ValueError,
                       message='Only allow airflow to run daily or weekly'):
        compute_days_between_runs("* 20 * * *")
