"""Amazon RDS(MySQL)에 저장되어있는 테이블을 읽어 Parquet 포맷으로 S3에 저장하는 Job
AWS Glue Pyspark 으로 개발하였으며
보안은 전혀 신경쓰지않은 연습용 스크립트
"""

import sys
import datetime as dt
import boto3

from pyspark.context import SparkContext
from pyspark.sql import SparkSession

from awsglue.context import GlueContext
from awsglue.job import Job

from awsglue.transforms.apply_mapping import ApplyMapping
from awsglue.transforms.drop_nulls import DropNullFields
from awsglue.transforms.resolve_choice import ResolveChoice
from awsglue.utils import getResolvedOptions

from awsglue.dynamicframe import DynamicFrame



def getDataFrameFromRDS():
    jdbc_driver_name = "com.mysql.jdbc.Driver"
    rds_endpoint = "jdbc:mysql://my-rds.abcdefg1234.ap-northeast-2.rds.amazonaws.com:3306/mydb"
    rds_username = "rdsusername"
    rds_password = "rdspassword"

    my_rds_tables_query = """(select id, name from my_rds_table) as temp""".strip('\n').replace('\n', ' ')

    df = glueContext.read.format("jdbc") \
        .option("driver", jdbc_driver_name) \
        .option("url", rds_endpoint) \
        .option("dbtable", my_rds_tables_query) \
        .option("user", rds_username) \
        .option("password", rds_password) \
        .load()

    return df


def writeParquetS3_with_DynamicFrame(df):
    bucket_name = "my-parquet-data"
    object_name = "my-target-table"
    target_path = "s3://" + bucket_name + "/" + object_name

    dynamic_frame = DynamicFrame.fromDF(df, glueContext, 'dynamic_frame')
    apply_mapping = ApplyMapping.apply(frame=dynamic_frame,
                                       mappings=[("id", "integer", "id", "integer"),
                                                 ("name", "string", "name", "string")],
                                       transformation_ctx="apply_mapping")

    resolve_choice = ResolveChoice.apply(frame=apply_mapping, choice="make_struct", transformation_ctx="resolvechoice")
    dropnull_fields = DropNullFields.apply(frame=resolve_choice, transformation_ctx="dropnullfields")

    datasink = glueContext.write_dynamic_frame.from_options(frame=dropnull_fields,
                                                            connection_type="s3",
                                                            connection_options={"path": target_path},
                                                            format="parquet",
                                                            transformation_ctx="datasink4")


if __name__ == '__main__':

    args = getResolvedOptions(sys.argv, ['JOB_NAME'])

    sc = SparkContext()

    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    df = getDataFrameFromRDS()
    writeParquetS3_with_DynamicFrame(df)

    job.commit()






