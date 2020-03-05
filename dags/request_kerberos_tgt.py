"""
DAG to request and renew Kerberos ticket-granting tickets
"""

# pylint: disable=invalid-name

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

pipeline_args = {
    'owner': 'operations',
    'depends_on_past': False,
    'start_date': datetime(2017, 6, 1),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=10),
    'retry_exponential_backoff': True,
}

dag = DAG('request_kerberos_tgt',
          default_args=pipeline_args,
          schedule_interval="@hourly",
          catchup=False)

renew_token_operator = BashOperator(
    task_id='renew_or_request_kerberos_tgt',
    bash_command='kinit -R || kinit -k -t /etc/krb5.keytab "$KERBEROS_PRINCIPAL" || true',
    execution_timeout=timedelta(minutes=5),
    dag=dag
)
