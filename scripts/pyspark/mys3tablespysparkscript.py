
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
parser.add_argument('--data_source_type', help="Source data Glue Table Type.")
parser.add_argument('--data_source_catalog', help="Source DB/TableCatalog.")
parser.add_argument('--data_destination_s3tables_arn', help="Destination S3 Table ARN.")
parser.add_argument('--data_destination_catalog', help="Destination S3 Tables Catalog.")
parser.add_argument('--data_destination_s3tables_namespace', help="Destination S3 Tables Namespace/Database.")
parser.add_argument('--data_destination_s3tables_tbl', help="Destination S3 Tables Table name .")
parser.add_argument('--data_destination_s3tables_partitions', help="Destination S3 Tables Table Partitions .")


# Initiate ARGS
args = parser.parse_args()

# Now define the variables
data_migration_type = args.data_migration_type
data_source_bucket = args.data_source_bucket
data_source_db = args.data_source_db
data_source_tbl = args.data_source_tbl
data_source_type = args.data_source_type
data_source_catalog = args.data_source_catalog
data_destination_catalog = args.data_destination_catalog
data_destination_s3tables_arn = args.data_destination_s3tables_arn
data_destination_s3tables_namespace = args.data_destination_s3tables_namespace
data_destination_s3tables_tbl = args.data_destination_s3tables_tbl
data_destination_s3tables_partitions = args.data_destination_s3tables_partitions

# Create Spark Configuration Set
conf = SparkConf()     .set("spark.sql.catalogImplementation", "hive")     .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")     .set(f"spark.sql.catalog.{data_destination_catalog}", "org.apache.iceberg.spark.SparkCatalog")     .set(f"spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")     .set(f"spark.sql.catalog.{data_destination_catalog}.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog")     .set(f"spark.sql.catalog.{data_destination_catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")     .set(f"spark.sql.catalog.{data_destination_catalog}.warehouse", data_destination_s3tables_arn)     .set(f"spark.sql.catalog.{data_source_catalog}", "org.apache.iceberg.spark.SparkCatalog")     .set(f"spark.sql.catalog.{data_source_catalog}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")     .set(f"spark.sql.catalog.{data_source_catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")                 



# Initiate PySpark Session
spark = SparkSession.builder.appName("MyMigrationApp").config(conf=conf).getOrCreate()

# Function for creating a New NameSpace in Amazon S3 Table Bucket
def create_namespace(catalog, dst_db): 
    # Create a NameSpace in S3 Table Buckets first
    try:
        # Create the Namespace first
        sql_query_namespace = f"""
        CREATE NAMESPACE IF NOT EXISTS
        `{catalog}`.`{dst_db}`
        """        
        # Now run the query
        spark_sql_query_namespace = spark.sql(sql_query_namespace)                    
    except Exception as e:
        print(e)
        raise e                       


# Function for performing INSERT/UPDATE into an existing destination Database/Table
def insert_update_action(src_catalog, catalog, src_db, src_tbl, dst_db, dst_tbl):
    """
    Use INSERT/UPDATE to load data from source to S3 Tables Bucket
    :param:
    """

    try:
        # Do an INSERT INTO to migrate table data from source to S3 Tables Bucket
        sql_query_insert = ''
        # Let's start the INSERT INTO action FOR the earlier CTAS 
        print(f"Initiating INSERT INTO worklow from {src_catalog}.{src_db}.{src_tbl} into {dst_db}.{dst_tbl} please hold...")
        # Handle query with or without catalog name provided
        if src_catalog:
            sql_query_insert = f"""
            INSERT INTO
            `{catalog}`.`{dst_db}`.`{dst_tbl}`
            SELECT * FROM `{src_catalog}`.`{src_db}`.`{src_tbl}`
            """ 
        else:
            sql_query_insert = f"""
            INSERT INTO
            `{catalog}`.`{dst_db}`.`{dst_tbl}`
            SELECT * FROM `{src_db}`.`{src_tbl}`
            """                     

        # Run the INSERT INTO SQL query
        spark_sql_query_insert = spark.sql(sql_query_insert)
    except Exception as e:
        print(e)
        raise e
    else:
        print(f"INSERT INTO worklow from {src_db}.{src_tbl} into {dst_db}.{dst_tbl} completed!")



# Function for performing CTAS - CREATE TABLE AS SELECT into a new destination Database/Table - creates a new DB/Table
def ctas_action(src_catalog, catalog, src_db, src_tbl, dst_db, dst_tbl, dst_partitions):
    """
    Use CTAS to load data from source to S3 Tables Bucket
    :param:
    """
    print(f"Echo parameters src_catalog={src_catalog}, catalog={catalog}, src_db={src_db}, src_tbl={src_tbl}, dst_db={dst_db}, dst_tbl={dst_tbl}")
    # We need to create the namespace/database first, so calling the namespace function
    print(f"Creating the namespace {dst_db} first if it does not already exist....")
    create_namespace(catalog, dst_db)
    print(f"Creating the namespace {dst_db} is successful proceeding to CTAS, please hold...")

    try:
        # Do a CTAS to migrate table data from source Table to S3 Tables Bucket
        # If destination partition is provided, them include partition info in CTAS query
        # We are not loading data now, just creating an empty table
        sql_query_d = ''
        # Check the provided partition name and value for the destination Table
        if dst_partitions:
            if dst_partitions == "NotApplicable":
                # Handle query with or without catalog name provided
                if src_catalog:
                    sql_query_d = f"""
                    CREATE TABLE IF NOT EXISTS
                    `{catalog}`.`{dst_db}`.`{dst_tbl}`
                    USING iceberg
                    AS SELECT * FROM `{src_catalog}`.`{src_db}`.`{src_tbl}` 
                    LIMIT 0
                    """
                else: 
                    sql_query_d = f"""
                    CREATE TABLE IF NOT EXISTS
                    `{catalog}`.`{dst_db}`.`{dst_tbl}`
                    USING iceberg
                    AS SELECT * FROM `{src_db}`.`{src_tbl}` 
                    LIMIT 0
                    """                               
            else:
                # Handle query with or without catalog name provided                        
                if src_catalog: 
                    sql_query_d = f"""
                    CREATE TABLE IF NOT EXISTS
                    `{catalog}`.`{dst_db}`.`{dst_tbl}`
                    USING iceberg
                    PARTITIONED BY {dst_partitions}
                    AS SELECT * FROM `{src_catalog}`.`{src_db}`.`{src_tbl}`
                    LIMIT 0
                    """
                else:
                    sql_query_d = f"""
                    CREATE TABLE IF NOT EXISTS
                    `{catalog}`.`{dst_db}`.`{dst_tbl}`
                    USING iceberg
                    PARTITIONED BY {dst_partitions}
                    AS SELECT * FROM `{src_db}`.`{src_tbl}`
                    LIMIT 0
                    """                                

        # Run the CTAS SQL query
        spark_sql_query_d = spark.sql(sql_query_d)
    except Exception as e:
        print(e)
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
        print(e)
        raise e
    else:
        return spark_sql_query_data


# Main workflow Function, calls other functions as needed
def initiate_workflow():
    """
    Initiate Migration Workflow

    """
    try:
        # First let's query the source table
        print(f"Let do a test query of the source table {data_source_db}.{data_source_tbl} to see if we can perform a successful query")
        if data_source_type == 'Standard':
            query_table_data(None, data_source_db, data_source_tbl)
        elif data_source_type == 'Iceberg':    
            query_table_data(data_source_catalog, data_source_db, data_source_tbl)
        print(f"Test query of the source table {data_source_db}.{data_source_tbl} is successful proceeding to main task")
        # Choose the CTAS option to create new Amazon S3 Table Bucket destination NameSpace and Table
        if data_migration_type == 'New-Migration':
            print(f"We are performing a new migration, so will use CTAS to create a new table and load data")
            if data_source_type == 'Iceberg':
                print(f"Source Table type is Hive....")
                ctas_action(data_source_catalog, data_destination_catalog, data_source_db, data_source_tbl, data_destination_s3tables_namespace,
                            data_destination_s3tables_tbl, data_destination_s3tables_partitions
                            )
                # Now that we have successfully created the destination table, let's perform an INSERT INTO
                insert_update_action(data_source_catalog, data_destination_catalog, data_source_db, data_source_tbl,
                                    data_destination_s3tables_namespace, data_destination_s3tables_tbl)
            elif data_source_type == 'Standard':
                ctas_action(None, data_destination_catalog, data_source_db, data_source_tbl, data_destination_s3tables_namespace,
                            data_destination_s3tables_tbl, data_destination_s3tables_partitions
                            )
                # Now that we have successfully created the destination table, let's perform an INSERT INTO
                insert_update_action(None, data_destination_catalog, data_source_db, data_source_tbl,
                                    data_destination_s3tables_namespace, data_destination_s3tables_tbl)                                                                                      

        # Now we are done with CTAS and INSERT INTO, let's perform some verifications on the destination Table
        # Let's query the destination table
        print(f"Let do a test query of the destination table {data_destination_s3tables_namespace}.{data_destination_s3tables_tbl} to see if we can perform a successful query")
        query_table_data(data_destination_catalog, data_destination_s3tables_namespace, data_destination_s3tables_tbl)
        print(f"Test query of the destination table {data_destination_s3tables_namespace}.{data_destination_s3tables_tbl} is successful!! ")
        """ Migration and verification was successful!"""

    except Exception as e:
        print(e)
        sys.exit(1)
    else:
        # Finalize Job
        print("Successful Job completion")



if __name__ == "__main__":
    # Start the Main Task
    initiate_workflow()
