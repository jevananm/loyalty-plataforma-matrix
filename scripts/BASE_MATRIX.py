import sys
from pyspark.sql.functions import substring, to_timestamp, when
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark import SparkConf
from awsglue.job import Job
import zipfile
import pandas as pd

from botocore.exceptions import ClientError
import boto3
import json


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','CONFIG_TABLE'])

myconfig=SparkConf().set('spark.rpc.message.maxSize','1024').set('spark.driver.memory','9g') #256
# spark.driver.memory
#SparkConf can be directly used with its .set  property
sc = SparkContext(conf=myconfig)
#sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)



def_table = args['CONFIG_TABLE']
config_table = json.loads(def_table)

job.init(args['JOB_NAME']+'-'+config_table['table_name'], args)


secret_name = "/PROD/MATRIX"
region_name = "us-east-1"

# Create a Secrets Manager client
session = boto3.session.Session()
client = session.client(
    service_name='secretsmanager',
    region_name=region_name
)

try:
    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
except ClientError as e:
    # For a list of exceptions thrown, see
    # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    print(e)
    raise e

# Decrypts secret using the associated KMS key.
secret = get_secret_value_response['SecretString']
config_main = json.loads(secret)
bucket_name = "ue1stgdesaas3ftp001"

if type(config_table["filename"]) == type([]):
    pass
else:
    config_table["filename"] = [config_table["filename"]]

if config_table["filename"][0].lower().endswith('zip'):
    client_s3 = boto3.client('s3')
    resource_s3 = boto3.resource('s3')

    bucket = resource_s3.Bucket(bucket_name)
    df = pd.DataFrame()
    for n in config_table["filename"]:
        path_s3_source = f'{config_table["path"]}/{n}'
        #Download Zip
        file_dest_on_memory = f'/tmp/{n}'
        with open(file_dest_on_memory, 'wb') as f:
            client_s3.download_fileobj(bucket_name, 
                                        path_s3_source, f)
                        
        # Read the excel sheet to pandas dataframe

        df_tmp = pd.read_csv(file_dest_on_memory, names=['value'], header=None,compression='zip', sep=';',encoding = 'ISO-8859-1')
        data = [df, df_tmp]
        df = pd.concat(data)

        print(df.head(5))
    df1=spark.createDataFrame(df)
else:
    path = f's3://{config_table["bucket_name"]}/{config_table["path"]}/{config_table["filename"][0]}'
    df1 = spark.read.option("encoding", "ISO-8859-1").text(path)

fields = config_table["data"]


init = 1
for field in fields:
    if field['type'] == 'TIMESTAMP':
        df1 = df1.withColumn(field['name'].lower(), to_timestamp(substring('value', init, field['size']),'yyyyMMdd'))
    elif field['type'] == 'BOOLEAN': 
        name_tmp  = field['name'].lower()+ "_tmp"
        df1 = df1.withColumn(name_tmp, to_timestamp(substring('value', init, field['size']),'yyyyMMdd'))
        df1 = df1.withColumn(field['name'].lower(), when(df1[name_tmp] == "1",True).when(df1[name_tmp]  == "0",False))#.otherwise(df.gender))
        df1 = df1.drop(name_tmp)
    elif field['type'] == 'INTEGER' or field['type'] == 'BIGINT': 
        df1 = df1.withColumn(field['name'].lower(), substring('value', init, field['size']).cast("Integer"))
    elif 'DECIMAL' in field['type'] : 
        df1 = df1.withColumn(field['name'].lower(), substring('value', init, field['size']).cast("double"))
    else:
        df1 = df1.withColumn(field['name'].lower(), substring('value', init, field['size']))
    init = init + field['size']

df1 = df1.drop("value")
#print(df1.show(5))

#REDSHIFT
db_host = config_main['DB_HOST']
db_port = config_main['DB_PORT']
db_name = config_main['DB_NAME']
db_user = config_main['DB_USER']
db_pwd = config_main['DB_PWD']


table_temporal = config_main['TABLE_TEMPORAL']
iam_redshift = config_main['IAM_REDSHIFT']



# datos redshift
url_redshift = "jdbc:redshift://{0}:{1}/{2}?user={3}&password={4}".format(db_host, db_port, db_name, db_user, db_pwd)
schema_redshift = "trusted"
name_table = config_table["table_name"].lower()
table_redshift = "{0}.{1}".format(schema_redshift, name_table)

pre_actions = "TRUNCATE TABLE {0};".format(table_redshift)

df1.write \
                        .format("com.databricks.spark.redshift") \
                        .option("url", url_redshift) \
                        .option("preactions", pre_actions) \
                        .option("dbtable", table_redshift) \
                        .option("tempdir", table_temporal) \
                        .option("aws_iam_role", iam_redshift) \
                        .mode("append") \
                        .save()
#job.commit()

