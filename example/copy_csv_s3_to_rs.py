"""S3에 저장되어있는 csv 파일을 Redshift 로 COPY하는 Python Shell 이며
csv내 비어있는 값을 공백('')값으로 채우고 첫째 행은 COPY하지 않습니다.
보안은 전혀 신경쓰지않은 연습용 스크립트
"""

import pg
import sys
import boto3
import json
import datetime

rs_endpoint = 'my-redshift.abcdefg1234.ap-northeast-2.redshift.amazonaws.com'
rs_username = 'rsusername'
rs_password = 'rspassword'
rs_port = '5439'
rs_db_name = 'rsdb'

bucket_name = 'my-rs-data'
object_name = '/my_redshift_table/file.csv'

target_db_schema = 'my_redshift_schema'
target_table_name = 'my_redshift_table'

iam_role_arn = 'arn:aws:iam::123456789:role/my-redshift-role'

try:
    rs_conn_string = "host=%s port=%s dbname=%s user=%s password=%s" % \
                     (rs_endpoint, rs_port, rs_db_name, rs_username, rs_password)
    rs_conn = pg.connect(dbname=rs_conn_string)
    rs_conn.query("set statement_timeout = 36000000")

    statement = """
            COPY {target_db_schema}.{target_table_name}
            FROM 's3://{bucket_name}/{object_name}'
            IAM_ROLE {iam_role_arn}
            delimiter ',' CSV
            FILLRECORD
            IGNOREHEADER 1;
    """.strip('\n').replace('\n', ' ').format(target_db_schema=target_db_schema,
                                              target_table_name=target_table_name,
                                              bucket_name=bucket_name,
                                              object_name=object_name,
                                              iam_role_arn=iam_role_arn)

    print("Execute Query : " + statement)
    res = rs_conn.query(statement)
    print("Complete procedure.")

    if rs_conn:
        rs_conn.close()
        print("Connection is Closed.")
except Exception as e:
    print(e)
    raise