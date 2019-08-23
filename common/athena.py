from common.aws_resource_base import ResourceBase
from botocore.exceptions import ClientError
import logging
import time

logger = logging.getLogger(__name__)


class Athena(ResourceBase):
    """  This class is used to interact with AWS Athena  """

    def __init__(self):
        super().__init__()

    def create_output_config(self, tag, datatype):
        """ Returns an output configuration used for the main query """
        return "s3://{}/{}/".format(tag, datatype)

    def __main_query(self, query, tag, database):
        """ Main function used to create an Athena table and query data from an Athena table """

        logger.info("Calling main_query")
        logger.info("Here is the Athena query that is about to run:\n\n\n\n{}\n\n\n".format(query))
        self.athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={"OutputLocation":'s3://{}-results/'.format(tag)},
            WorkGroup=tag
        )
        logger.info("Finished calling main_query")

    def create_database(self, tag, database):
        """ Creates an Amazon Athena database within a Amazon S3 bucket """

        def create_database_query(query, tag):
            """ Main function used to create an Athena database"""

            logger.info("Calling main_query")
            self.athena_client.start_query_execution(
                QueryString=query,
                ResultConfiguration={"OutputLocation": "s3://{}/".format(tag)}
            )
            logger.info("Finished calling main_query")

        try:
            logger.info("About to create an Athena database!")
            create_database_query("CREATE DATABASE IF NOT EXISTS %s;" % database, tag)
            logger.info("Created database succesfully!\n")
        except ClientError:
            logger.info("There was an error in creating the Athena database!\n")


    def __create_work_group_config(self, tag):
        return {
            'ResultConfiguration': {'OutputLocation': 's3://{}-results/'.format(tag)}, 'EnforceWorkGroupConfiguration': True
        }

    def create_work_group(self, tag):
        try:
            logger.info("About to create the work group")
            # self.athena_client.create_work_group(Name=tag, Configuration=self.__create_work_group_config(tag))
            self.athena_client.create_work_group(Name=tag)
            logger.info("Successfully created the work group\n")
        except ClientError:
            logger.info("Work group already created!\n")

    def delete_work_group(self, tag):
        try:
            logger.info("About to delete the work group")
            self.athena_client.delete_work_group(WorkGroup=tag, RecursiveDeleteOption=True)
            logger.info("Successfully deleted the work group\n")
        except ClientError:
            logger.info("Work group already deleted!\n")

    def list_querys(self, tag):
        try:
            return self.athena_client.list_query_executions(WorkGroup=tag)
        except ClientError:
            logger.info("There was an error in listing the queries!")

    def check_query_status(self, id):
        status = self.athena_client.get_query_execution(QueryExecutionId=id)
        while status['QueryExecution']['Status']['State'] != 'SUCCEEDED':
            status = self.athena_client.get_query_execution(QueryExecutionId=id)
            state = status['QueryExecution']['Status']['State']
            logger.info(state)
            if state == 'FAILED':
                raise Exception("There was an error in querying")
            time.sleep(2)
            logger.info("Query is still running")
            continue
        return True

    def show_query_failed(self, tag):
        query = self.list_querys(tag)["QueryExecutionIds"][0]
        logger.info("Going to check if query has finished")
        status = self.athena_client.get_query_execution(QueryExecutionId=query)
        while status['QueryExecution']['Status']['State'] != 'SUCCEEDED':
            status = self.athena_client.get_query_execution(QueryExecutionId=query)
            state = status['QueryExecution']['Status']['State']
            logger.info(state)
            if state == 'FAILED':
                logger.info("Query has failed! This is because there is no metadata in the Glue Catalog\n")
                return False
            time.sleep(2)
            logger.info("Query is still running")
            continue
        return True

    def get_query(self, id):
        try:
            return self.athena_client.get_query_results(QueryExecutionId=id)
        except ClientError:
            logger.info("There is an error in getting the query results!")

    def read_data(self, tag):
        """ Returns the most recently executed query by Athena """
        query = self.list_querys(tag)["QueryExecutionIds"][0]
        # logger.info("Here is the id of the query that filed: {}!!".format(query))
        logger.info("Going to check if query has finished")
        if self.check_query_status(query):
            logger.info("Query has finished running\n")
            return self.get_query(query)['ResultSet']['Rows']

    def create_athena_csv_table(self, tag, database, table):
        """ Creates an Amazon Athena table within an Athena database """

        query = """CREATE EXTERNAL TABLE %s.%s(
        first_name STRING,
        last_name STRING,
        year_born INT,
        age INT,
        gender STRING
        ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES (
        'separatorChar' = ',',
        'quoteChar' = '"' ,
        "skip.header.line.count"="1"
        )
        STORED AS TEXTFILE
        LOCATION "%s" """ % (database, table, self.create_output_config(tag, 'csv'))

        try:
            logger.info("About to create an Athena table!")
            logger.info("Here is an example query to manually create a table through an Athena Query:\n {}\n".format(query))
            self.__main_query(query, tag, database)
            logger.info("Created Athena table created succesfully!\n")
        except ClientError:
            logger.info("There was an error! Athena table not created successfully\n")

    def create_athena_json_table(self, tag, database, table):
        """ Creates an Amazon Athena table within an Athena database """

        query = """CREATE EXTERNAL TABLE %s.%s(
        tweet string,
        name string
        ) ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
        STORED AS TEXTFILE
        LOCATION "%s" """ % (database, table, self.create_output_config(tag, 'json'))

        try:
            logger.info("About to create an Athena table!")
            self.__main_query(query, tag, database)
            logger.info("Created Athena table created succesfully!\n")
        except ClientError:
            logger.info("There was an error! Athena table not created successfully\n")

    def create_new_table_as(self, tag, database,table):
        """ Create a new Athena table based on a previous database """

        query = """CREATE TABLE prac 
        AS SELECT age FROM %s.%s; """ % (database, table)
        self.__main_query(query, tag, database)

    def delete_database(self, tag, database):
        """ Deletes an Amazon Athena database """

        query = """DROP DATABASE %s CASCADE""" % database
        try:
            logger.info("About to delete the database and all of its contents")
            self.__main_query(query, tag, database)
            logger.info("Successfully deleted the database and all of its contents!\n")
        except ClientError:
            logger.info("There was an error! Can't delete the database\n")

    def delete_table(self, tag, database, table):
        """ Deletes an Amazon Athena table"""

        query = """DROP TABLE %s""" % table
        try:
            logger.info("About to delete the Athena table")
            self.__main_query(query, tag, database)
            logger.info("Successfully deleted the table!\n")
        except ClientError:
            logger.info("There exists an error! Can't delete the table\n")

    def show_json_extract_fxn(self, tag, database):
        query_1 = """ WITH dataset AS (
              SELECT * FROM(VALUES
              (JSON '{"first_name": "Bob",
                       "last_name": "Wilkerson",
                       "year_born": 1943,
                       "age": 27}'),
               (JSON
                       '{"first_name": "Rob",
                       "last_name": "Dranson",
                       "year_born": 1941,
                       "age": 25}')
                ) AS  t (blob)
            )

            SELECT
              json_extract_scalar(blob, '$.age') AS age
              FROM
              dataset """
        try:
            logger.info("\nHere is an example of using Presto json functions within Athena to query complex json data:\n {}".format(query_1))
            self.__main_query(query_1, tag, database)
            logger.info("Example ran properly!\n")
        except ClientError:
            logger.info("There was an issue!\n")

    def run_example_explicit_helper(self, tag, database, table):
        query = """CREATE OR REPLACE VIEW explicit_helper AS 
        SELECT peopleinfo.first_name, peopleinfo.last_name, peopleinfo.gender
        FROM %s.%s 
        CROSS JOIN UNNEST (people) 
        AS t(peopleinfo); """ % (database, table)
        try:
            logger.info("About to create a table with just the first name, last name, and gender fields")
            self.__main_query(query, tag, database)
            logger.info("Successfully created a table with just the first name, last name, and gender fields!\n")
        except ClientError:
            logger.info("There was an issue in creating a table with just the first name, "
                        "last name, and gender fields!\n")

    def run_example_explicit(self, tag, database, table):
        query = "SELECT * FROM %s.%s;" % (database, "explicit_helper")
        try:
            logger.info("About to run a query that reads from the table "
                        "that just has the first name, last name, and gender fields")
            self.__main_query(query, tag, database)
            logger.info("Successfully ran a query that reads from the table "
                        "that just has the first name, last name, and gender fields!\n")
        except ClientError:
            logger.info("There was an issue in running a query that reads from the table "
                        "that just has the first name, last name, and gender fields!\n")

    def run_example_generic_helper_1(self, tag, database):
        query = """CREATE EXTERNAL TABLE test_table_bucktest(people string) 
        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
        LOCATION 's3://dlp-bucktest/json/' """
        try:
            logger.info("About to create generic metadata for our complex json data")
            self.__main_query(query, tag, database)
            logger.info("Successfully created the generic metadata for our complex json data!\n")
        except ClientError:
            logger.info("There was an issue in creating he generic metadata for our complex json data!\n")

    def run_example_generic_helper_2(self, tag, database):
        query = """ CREATE OR REPLACE VIEW casted_test_table_bucktest AS
        SELECT CAST(json_parse(people) AS ARRAY(MAP(VARCHAR, VARCHAR))) peopleinfo
        from test_table_bucktest;"""
        try:
            logger.info("About to create a table that casts the data within our generic metadata to an Athena format")
            self.__main_query(query, tag, database)
            logger.info("Successfully created a table that casts the"
                        " data within our generic metadata to an Athena format!\n")
        except ClientError:
            logger.info("There was an issue in creating a table table that casts the data"
                        " within our generic metadata to an Athena format\n")

    def run_example_generic(self, tag, database):
        query = """SELECT 
        element_at(people, 'first_name') first_name,
        element_at(people, 'last_name') last_name,
        element_at(people, 'gender') gender
        FROM
        casted_test_table_bucktest
        CROSS JOIN
        UNNEST(peopleinfo) AS T (people) """
        try:
            logger.info("About to run a query that reads from the table "
                        "that just has the first name, last name, and gender fields")
            self.__main_query(query, tag, database)
            logger.info("Successfully ran a query that reads from the table "
                        "that just has the first name, last name, and gender fields!\n")
        except ClientError:
            logger.info("There was an issue in running a query that reads from the table "
                        "that just has the first name, last name, and gender fields!\n")

    def run_example_1(self, tag, database, table):
        """ This query reads all the table within an Athena database """

        query = "SELECT * FROM %s.%s;" % (database, table)
        try:
            logger.info("About to run the query that reads all the data")
            self.__main_query(query, tag, database)
            logger.info("Example ran properly!\n")
        except ClientError:
            logger.info("There was an issue!\n")


