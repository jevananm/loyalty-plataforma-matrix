import sys
from pyspark.sql.functions import substring, to_timestamp
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import zipfile
import pandas as pd

import boto3



## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


args = {
    'DB_HOST':'matrix.cdbvcj1zx66s.us-east-1.redshift.amazonaws.com',
    'DB_PORT':'5439',
    'DB_NAME':'matrix',
    'DB_USER':'awsuser',
    'DB_PWD':'M4tr1x2022._',
    'TABLE_TEMPORAL':'s3://ue1stgdesaas3mat003/temporal',
    'IAM_REDSHIFT':'arn:aws:iam::198328445529:role/service-role/AmazonRedshift-CommandsAccessRole-20221129T004338',
}


config_table = {
    "table_name": "m_tarjeta",
    "bucket_name": "ue1stgdesaas3ftp001",
    "path": "RAW-SFTP/F685",
    "filename":"F685G.zip",
    "data": [
        {
            "order": 1,
            "type": "VARCHAR(100)",
            "size": 19,
            "name": "cod_tarjeta"
        },
        {
            "order": 2,
            "type": "VARCHAR(100)",
            "size": 1,
            "name": "cod_estadotarjeta"
        },
        {
            "order": 3,
            "type": "VARCHAR(100)",
            "size": 20,
            "name": "des_estadotarjeta"
        },
        {
            "order": 4,
            "type": "VARCHAR(100)",
            "size": 4,
            "name": "tip_tarjeta"
        },
        {
            "order": 5,
            "type": "VARCHAR(100)",
            "size": 20,
            "name": "des_tiptarjeta"
        },
        {
            "order": 6,
            "type": "VARCHAR(100)",
            "size": 10,
            "name": "cod_personath"
        },
        {
            "order": 7,
            "type": "VARCHAR(100)",
            "size": 10,
            "name": "cod_persona"
        },
        {
            "order": 8,
            "type": "VARCHAR(100)",
            "size": 3,
            "name": "cod_cuenta"
        },
        {
            "order": 9,
            "type": "TIMESTAMP",
            "size": 8,
            "name": "fec_solicitud"
        },
        {
            "order": 10,
            "type": "TIMESTAMP",
            "size": 8,
            "name": "fec_afiliacion"
        },
        {
            "order": 11,
            "type": "VARCHAR(100)",
            "size": 4,
            "name": "des_afiliacioncadena"
        },
        {
            "order": 12,
            "type": "VARCHAR(100)",
            "size": 4,
            "name": "des_afiliaciontienda"
        },
        {
            "order": 13,
            "type": "VARCHAR(100)",
            "size": 1,
            "name": "tip_tarjetafinanciera"
        },
        {
            "order": 14,
            "type": "VARCHAR(100)",
            "size": 15,
            "name": "des_afiliacion"
        }
    ]
}



if config_table["filename"].lower().endswith('zip'):
    client_s3 = boto3.client('s3')
    resource_s3 = boto3.resource('s3')

    bucket_name = "ue1stgdesaas3ftp001"
    bucket = resource_s3.Bucket(bucket_name)

    path_s3_source = f'{config_table["path"]}/{config_table["filename"]}'
    #Download Zip
    file_dest_on_memory = f'/tmp/{config_table["filename"]}'
    with open(file_dest_on_memory, 'wb') as f:
        client_s3.download_fileobj(bucket_name, 
                                    path_s3_source, f)
                    
    # Read the excel sheet to pandas dataframe

    df = pd.read_csv(file_dest_on_memory, names=['value'], header=None,compression='zip', sep=';')
    #print(pdf.head(5))
    df1=spark.createDataFrame(df)
else:
    path = f's3://{config_table["bucket_name"]}/{config_table["path"]}/{config_table["filename"]}'
    df1 = spark.read.option("encoding", "ISO-8859-1").text(path)

fields = config_table["data"]


init = 1
for field in fields:
    if field['type'] == 'TIMESTAMP':
        df1 = df1.withColumn(field['name'], to_timestamp(substring('value', init, field['size']),'yyyyMMdd'))
    else:
        df1 = df1.withColumn(field['name'], substring('value', init, field['size']))
    init = init + field['size']

df1 = df1.drop("value")
#print(df1.show(5))

#REDSHIFT
db_host = args['DB_HOST']
db_port = args['DB_PORT']
db_name = args['DB_NAME']
db_user = args['DB_USER']
db_pwd = args['DB_PWD']


table_temporal = args['TABLE_TEMPORAL']
iam_redshift = args['IAM_REDSHIFT']



# datos redshift
url_redshift = "jdbc:redshift://{0}:{1}/{2}?user={3}&password={4}".format(db_host, db_port, db_name, db_user, db_pwd)
schema_redshift = "trusted"
name_table = config_table["table_name"]
table_redshift = "{0}.{1}".format(schema_redshift, name_table)

df1.write \
                        .format("com.databricks.spark.redshift") \
                        .option("url", url_redshift) \
                        .option("dbtable", table_redshift) \
                        .option("tempdir", table_temporal) \
                        .option("aws_iam_role", iam_redshift) \
                        .mode("append") \
                        .save()
job.commit()