"""S3에 저장되어있는 csv 파일을 Redshift 로 COPY하는 Python Shell 이며
csv내 비어있는 값을 공백('')값으로 채우고 첫째 행은 COPY하지 않습니다.
DB 접속정보 및 Bucket 정보는 KMS로 암호화 하였으며 boto3를 이용하여 값을 불러오고 사용합니다.
KMS 값 중 bucket_name / object_name / target_db_schema / target_table_name은
각 src / target 마다 key 명을 지정 해야합니다. (고정 값이 아님)
"""
import pg
import sys
import boto3
import json
from datetime import datetime

secret_id = 'my-secret-id'
kms_client = boto3.client('secretsmanager', region_name='ap-northeast-2')
secret_value_response = kms_client.get_secret_value(SecretId=secret_id)

secret_str = secret_value_response["SecretString"]
secret_values = json.loads(secret_str)

rs_username = secret_values.get('rs_username')
rs_password = secret_values.get('rs_password')
rs_endpoint = secret_values.get('rs_endpoint')
rs_port = secret_values.get('rs_port')
rs_db_name = secret_values.get('rs_db_name')

bucket_name = secret_values.get('bucket_name')
object_name = secret_values.get('object_name')

target_db_schema = secret_values.get('target_db_schema')
target_table_name = secret_values.get('target_table_name')

iam_role_arn = secret_values.get('rs_iam_role_arn')


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