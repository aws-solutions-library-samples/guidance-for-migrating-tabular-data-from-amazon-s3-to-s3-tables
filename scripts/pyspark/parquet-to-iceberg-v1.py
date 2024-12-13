import sys
import logging
from pyspark import SparkConf
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Setup Logging
logger = logging.getLogger(__name__)
logger.setLevel('INFO')

# Set Required Sys and Args parameters
catalog_name = "glue-catalog"
database_name = "my_iceberg_sql"
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket_name', 'prefix'])
bucket_name = args['bucket_name']
prefix = args['prefix']
warehouse_path = f"s3://{bucket_name}/iceberg-data"
table_name = "my_icebergtable"

# Setup Environment Configuration
conf = SparkConf() \
    .set("spark.sql.warehouse.dir", warehouse_path) \
    .set(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
    .set(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path) \
    .set(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .set(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
# sc = SparkContext.getOrCreate()

# Initialize Spark
spark = glueContext.spark_session

# Job Entry with try error catch
try:
    # ETL Code
    # Define Variables
    input_path = f"s3://{bucket_name}/{prefix}/"
    output_path = f"s3://{bucket_name}/csv-output/"
    df = spark.read.parquet(input_path)

    # Create a Temporary View
    df.createOrReplaceTempView(f"tmp_{table_name}")

    # Run a SQL query to display the data
    result = spark.sql(f"SELECT * FROM tmp_{table_name} LIMIT 10")

    # Show the results

    # Display results in Cloudwatch Logs
    result.show()

    # Create a new Database
    query = f"""
    CREATE DATABASE IF NOT EXISTS {database_name}
    """
    spark.sql(query)

    # CTAS for Iceberg Table
    logger.info(f"Creating and Iceberg Table at {warehouse_path} Started")
    query = f"""
    CREATE TABLE `{catalog_name}`.`{database_name}`.`{table_name}`
    USING iceberg
    AS SELECT * FROM tmp_{table_name}
    """
    spark.sql(query)
    # spark.sql(f"""SELECT * FROM {catalog_name}.{database_name}.{table_name}""").show()


except Exception as e:
    logger.error(e, exc_info=True)
    sys.exit(1)

else:
    # Finalize Job
    logger.info("Successful Job completion")
    job.commit()
