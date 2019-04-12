"""
Kochava installs attributed to Facebook need to be parsed in order to get campaign level granularity.
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from collections import defaultdict
import csv
from datetime import datetime, timedelta
import io
import logging
from urllib import parse
import pandas as pd

from gsn_common.hooks.vertica_hook import VerticaHook
from gsn_common.operators.vertica_operator import VerticaOperator

keys_to_parse_from_install_original_requests = [
    "campaign_id",
    "campaign_name",
    "creative_id",
    "creative_name",
    "campaign_group_id",
    "campaign_group_name",
]

columns_to_rename_from_original_request_to_bingo_installs = {
    "campaign_group_id": "adset_id",
    "campaign_group_name": "adset_name",
}


select_stmt = """
SELECT id,
       original_request
  FROM bash.bingo_installs
 WHERE installed_at > '{min_date}'
   AND (source_name = 'FACEBOOK'
        OR source_name = 'ADQUANT')
   AND adset_name IS NULL
;
"""

update_stmt = """
UPDATE /*+direct*/ bash.bingo_installs bi
SET
  campaign_id   = fb.campaign_id,
  campaign_name = fb.campaign_name,
  creative_id   = fb.creative_id,
  creative_name = fb.creative_name,
  adset_id      = fb.adset_id,
  adset_name    = fb.adset_name
FROM bash.bingo_installs_facebook_ods fb
WHERE bi.id = fb.id;
"""


def parse_broken_json(jsons, key):
    """Parse a key out from a broken json string"""
    if key not in jsons:
        return None

    start_idx = jsons.find(key)
    end_idx = jsons[start_idx:].find('",')
    value = jsons[start_idx:start_idx + end_idx + 1].split(":")[1]
    return value.strip('"')


def iterate_over_original_requests_for_json_parsing(original_request_series):
    """Iterate over original requests"""
    new_fields = defaultdict(lambda: [])

    for original_request in original_request_series:
        for key in keys_to_parse_from_install_original_requests:

            try:
                new_fields[key].append(parse_broken_json(original_request, key))
            except:
                new_fields[key].append(None)

    return pd.DataFrame(new_fields).rename(columns=columns_to_rename_from_original_request_to_bingo_installs)


def copy_dataframe_to_table(connection, df, table_name="bash.bingo_installs_facebook_ods"):
    """Use Vertica COPY LOCAL to bulk load the data in the dataframe `df` into `loading_table`.
    The columns of `df` must match the columns of `loading_table`.
    """
    cursor = connection.cursor()
    cursor.execute("TRUNCATE TABLE {};".format(table_name))
    csv_buf = io.StringIO()
    df.to_csv(csv_buf, header=False, index=False, encoding='utf-8', quoting=csv.QUOTE_MINIMAL, escapechar='\\', doublequote=False)
    csv_buf.seek(0)
    copy_query = """
    COPY {table_name} (
        {column_string}
    ) FROM stdin DELIMITER ',' ENCLOSED BY '"' ESCAPE AS '\\' ABORT ON ERROR;
    """.format(table_name=table_name, column_string=", ".join(df.columns))
    logging.info(copy_query)
    cursor.copy(copy_query, csv_buf.getvalue())
    return len(df)


def update_kochava_facebook_campaigns(vertica_conn_id="vertica_mbo_backup", look_back=timedelta(90), **context):
    """Top level function for parsing fields of kochava campaign"""
    con = VerticaHook(vertica_conn_id=vertica_conn_id, driver_type="vertica_python").get_conn()
    compiled_select_stmt = select_stmt.format(min_date=(context["execution_date"] - look_back).isoformat())
    logging.info(compiled_select_stmt)
    df = pd.read_sql(compiled_select_stmt, con)

    # original request is a JSON string from Kochava describing a facebook ad campaign
    # this string is getting truncated somewhere so we need to do some special parsing

    original_request_series = df["original_request"]
    output_df = iterate_over_original_requests_for_json_parsing(original_request_series)
    output_df["id"] = df["id"]
    logging.info("Copied {} rows into bash.bingo_installs_facebook".format(
        copy_dataframe_to_table(con, output_df, table_name="bash.bingo_installs_facebook_ods")))

    # bingo_installs is updated using the temporary table created in the step above
    logging.info(update_stmt)
    cur = con.cursor()
    cur.execute(update_stmt)
    con.commit()


default_args = {
    'owner': 'bingo',
    'start_date': datetime(2015, 1, 21),
    'email': ['etl@gsngames.com',
              'data-team-standard@gsngames.pagerduty.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='bash_ua_facebook',
    default_args=default_args,
    schedule_interval=timedelta(minutes=10),
    catchup=False,
)

update_kochava_report_ios = PythonOperator(
    task_id='t_update_bingo_installs_facebook',
    python_callable=update_kochava_facebook_campaigns,
    provide_context=True,
    dag=dag,
)

t_separate_internal_traffic_from_yellowhead = VerticaOperator(
    task_id='t_separate_internal_traffic_from_yellowhead',
    vertica_conn_id='vertica_mbo_backup',
    sql='sql/t_separate_internal_traffic_from_yellowhead.sql',
    dag=dag,
)

update_kochava_report_ios >> t_separate_internal_traffic_from_yellowhead

t_fix_yh_campaign = VerticaOperator(
    task_id='t_fix_yh_campaign',
    vertica_conn_id='vertica_mbo_backup',
    sql='sql/t_fix_yh_campaign.sql',
    dag=dag,
)

t_separate_internal_traffic_from_yellowhead >> t_fix_yh_campaign
