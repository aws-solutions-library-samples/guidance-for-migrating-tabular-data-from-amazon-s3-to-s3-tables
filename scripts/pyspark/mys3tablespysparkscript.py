
import sys
import argparse
from pyspark.sql import SparkSession
from pyspark import SparkConf
import logging

# Setup Logging
logger = logging.getLogger(__name__)
logger.setLevel('INFO')

# Import Sys Arguments
parser = argparse.ArgumentParser()
parser.add_argument('--data_migration_type', help="Data Migration type new or insert/update.")
parser.add_argument('--data_source_bucket', help="Source data S3 bucket name.")
parser.add_argument('--data_source_db', help="Source data Glue Database name.")
parser.add_argument('--data_source_tbl', help="Source data Glue Table name.")
parser.add_argument('--data_destination_s3tables_arn', help="Destination S3 Table ARN.")
parser.add_argument('--data_destination_catalog', help="Destination S3 Tables Namespace/Database.")
parser.add_argument('--data_destination_s3tables_namespace', help="Destination S3 Tables Namespace/Database.")
parser.add_argument('--data_destination_s3tables_tbl', help="Destination S3 Tables Table name .")
parser.add_argument('--data_destination_s3tables_partitions', help="Destination S3 Tables Table Partitions .")
parser.add_argument('--data_source_tbl_partition_name', help="Source Data Table Partition Name .")
parser.add_argument('--data_source_tbl_partition_value', help="Source Data Table Partition Value .")

# Initiate ARGS
args = parser.parse_args()

# Now define the variables
data_migration_type = args.data_migration_type
data_source_bucket = args.data_source_bucket
data_source_db = args.data_source_db
data_source_tbl = args.data_source_tbl
data_destination_catalog = args.data_destination_catalog
data_destination_s3tables_arn = args.data_destination_s3tables_arn
data_destination_s3tables_namespace = args.data_destination_s3tables_namespace
data_destination_s3tables_tbl = args.data_destination_s3tables_tbl
data_destination_s3tables_partitions = args.data_destination_s3tables_partitions
data_source_tbl_partition_name = args.data_source_tbl_partition_name
data_source_tbl_partition_value = args.data_source_tbl_partition_value

# Create Spark Configuration Set
conf = SparkConf()     .set("spark.sql.catalogImplementation", "hive")     .set(f"spark.sql.catalog.{data_destination_catalog}", "org.apache.iceberg.spark.SparkCatalog")     .set(f"spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")     .set(f"spark.sql.catalog.{data_destination_catalog}.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog")     .set(f"spark.sql.catalog.{data_destination_catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")     .set(f"spark.sql.catalog.{data_destination_catalog}.warehouse", data_destination_s3tables_arn)     .set("spark.executor.memory", "8G")     .set("spark.executor.cores", "4")     .set("spark.driver.memory", "8G") 


# Initiate PySpark Session
spark = SparkSession.builder.appName("MyMigrationApp").config(conf=conf).getOrCreate()


# Function for performing INSERT/UPDATE into an existing destination Database/Table
def insert_update_action(catalog, src_db, src_tbl, dst_db, dst_tbl):
    """
    Use INSERT/UPDATE to load data from source to S3 Tables Bucket
    :param:
    """

    try:
        # Do a CTAS to migrate table data from source Hive to a new Iceberg Table
        sql_query_insert = f"""
        INSERT INTO
        `{catalog}`.`{dst_db}`.`{dst_tbl}`
        AS SELECT * FROM `{src_db}`.`{src_tbl}` 
        """
        # Run the INSERT INTO SQL query
        spark_sql_query_insert = spark.sql(sql_query_insert)
    except Exception as e:
        logger.error(e, exc_info=True)
        raise e


def create_namespace(catalog, dst_db):
        # Create the Namespace first
        sql_query_namespace = f"""
        CREATE NAMESPACE IF NOT EXISTS
        `{catalog}`.`{dst_db}`
        """        
        # Now run the query
        spark_sql_query_namespace = spark.sql(sql_query_namespace)       



# Function for performing CTAS - CREATE TABLE AS SELECT into a new destination Database/Table - creates a new DB/Table
def ctas_action(catalog, src_db, src_tbl, dst_db, dst_tbl):
    """
    Use CTAS to load data from source to S3 Tables Bucket
    :param:
    """
    print(f"Echo parameters catalog={catalog}, src_db={src_db}, src_tbl={src_tbl}, dst_db={dst_db}, dst_tbl={dst_tbl}")
    # We need to create the namespace/database first, so calling the namespace function
    print(f"Creating the namespace {dst_db} first if it does not already exist....")
    create_namespace(catalog, dst_db)
    print(f"Creating the namespace {dst_db} is successful proceeding to CTAS, please hold...")
    try:
        # Do a CTAS to migrate table data from source Hive to a new Iceberg Table
        sql_query_d = f"""
        CREATE TABLE IF NOT EXISTS
        `{catalog}`.`{dst_db}`.`{dst_tbl}`
        USING iceberg
        AS SELECT * FROM `{src_db}`.`{src_tbl}` 
        """
        # Run the CTAS SQL query
        spark_sql_query_d = spark.sql(sql_query_d)
    except Exception as e:
        logger.error(e, exc_info=True)
        raise e
    else:
        print(f"Create Table as Select (CTAS) completed....")


# Function for performing a querying on a Table
def query_table_data(catalog, db, tbl):
    """
    Check that we can access the Table data
    :param:
    """
    # Handle query with or without catalog name provided
    if catalog:
        sql_query_data = f"""SELECT * 
        FROM `{catalog}`.`{db}`.`{tbl}`
        limit 10
        """
    else:
        sql_query_data = f"""SELECT * 
        FROM `{db}`.`{tbl}`
        limit 10
        """

    try:
        # Run Spark SQL Query
        spark_sql_query_data = spark.sql(sql_query_data)
    except Exception as e:
        logger.error(e, exc_info=True)
        raise e
    else:
        return spark_sql_query_data.show()


# Function for retrieving the existing table DDL (Data Definition Language)
def generate_table_ddl(catalog, db, tbl):
    """
    Check that we can access the source Glue Table data
    :param:
    """
    # Handle query with or without catalog name provided
    if catalog:
        sql_query_ddl = f"""SHOW CREATE TABLE `{catalog}`.`{db}`.`{tbl}`"""
    else:
        sql_query_ddl = f"""SHOW CREATE TABLE `{db}`.`{tbl}`"""
    # check if I can access the existing Glue Tables
    try:
        # Run Spark SQL Query
        spark_sql_query_ddl = spark.sql(sql_query_ddl)
    except Exception as e:
        logger.error(e, exc_info=True)
        raise e
    else:
        # spark_sql_query_ddl.show()
        return spark_sql_query_ddl.show(truncate=False)


# Main workflow Function, calls other functions as needed
def initiate_workflow():
    """
    Initiate Migration Workflow

    """
    try:
        # First let's query the source table
        query_table_data(None, data_source_db, data_source_tbl)
        # Next let's retrieve the source Table DDL
        # my_source_table_ddl = generate_table_ddl(None, data_source_db, data_source_tbl)
        generate_table_ddl(None, data_source_db, data_source_tbl)
        # Depending on the Migration Type, let's choose the CTAS or Insert into/Update
        if data_migration_type == 'New-Migration':
            logger.info(f"We are performing a new migration, so will use CTAS to create a new table and load data")
            ctas_action(data_destination_catalog, data_source_db, data_source_tbl, data_destination_s3tables_namespace,
                        data_destination_s3tables_tbl
                        )

        elif data_migration_type == 'Update-Insert':
            logger.info(f"We are performing an INSERT INTO/UPDATE, the destination table already exists")
            insert_update_action(data_destination_catalog, data_source_db, data_source_tbl,
                                data_destination_s3tables_namespace,
                                data_destination_s3tables_tbl
                                )
        # Now we are done with CTAS or INSERT, let's perform some verifications on the destination Table
        # Let's query the destination table
        print(f"Let do a test query of the destination table {data_destination_s3tables_namespace}.{data_destination_s3tables_tbl} to see if we can perform a successful query")
        query_table_data(data_destination_catalog, data_destination_s3tables_namespace, data_destination_s3tables_tbl)
        # Retrieve the destination Table DDL too:
        print(f"Now retrieve the Create Table DDL of the destination table {data_destination_s3tables_namespace}.{data_destination_s3tables_tbl} too...")
        # my_destination_table_ddl = generate_table_ddl(data_destination_catalog, data_destination_s3tables_namespace, data_destination_s3tables_tbl)
        generate_table_ddl(data_destination_catalog, data_destination_s3tables_namespace, data_destination_s3tables_tbl)
        """ Migration and verification was successful!"""

    except Exception as e:
        logger.error(e, exc_info=True)
        spark.stop()
        sys.exit(1)
    else:
        # Finalize Job
        logger.info("Successful Job completion")
        spark.stop()


if __name__ == "__main__":
    # Start the Main Task
    initiate_workflow()

