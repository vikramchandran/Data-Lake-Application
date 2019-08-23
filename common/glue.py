from common.aws_resource_base import ResourceBase
import time
from botocore.exceptions import ClientError
import logging


logger = logging.getLogger(__name__)


class Glue(ResourceBase):
    """  This class is used to interact with AWS Glue  """

    def __init__(self):
        super().__init__()

    def __create_updatedname_table(self, database, table, filetype):
        """ Creates a new table with the correct name within Glue Catalog """

        response = self.glue_client.get_table(DatabaseName=database, Name=filetype)
        table_input = response["Table"]
        table_input["Name"] = table
        table_input.pop("CreatedBy")
        table_input.pop("CreateTime")
        table_input.pop("UpdateTime")
        table_input.pop("DatabaseName")
        try:
            self.glue_client.create_table(DatabaseName=database, TableInput=table_input)
        except ClientError:
            logger.info("Table already exists!\n")

    def __delete_table(self, database, filetype):
        """ Deletes a table within Glue Catalog"""
        try:
            self.glue_client.delete_table(DatabaseName=database, Name=filetype)
        except ClientError:
            logger.info("{} table doesn't exist yet!\n".format(filetype))

    def change_table_name(self, database, table, filetype):
        """ Changes the name of the table within Glue Catalog"""
        logger.info("About to change the crawler table name")
        self.__create_updatedname_table(database, table, filetype)
        self.__delete_table(database, filetype)
        logger.info("Changed crawler table name successfully!\n")

    def delete_table_wipeout(self, table, database):
        """ Deletes a table within Glue Catalog"""
        try:
            logger.info("About to delete the table within Glue Catalog")
            self.glue_client.delete_table(DatabaseName=database, Name=table)
            logger.info("Successfully the table within Glue Catalog\n")
        except ClientError:
            logger.info("Table was already deleted!!\n")

    def __s3_target(self, tag, filetype):
        return {'S3Targets': [
                                {
                                    'Path': 's3://{}/{}/'.format(tag, filetype)
                                }
                            ]
                }

    def get_crawler_name(self, database):
        return '{}_crawler'.format(database)

    def create_custom_jsonpath(self):
        return '$.people[*]'

    def get_classifier_name(self, tag):
        return "{}-classifier".format(tag)

    def __generate_json_classifier(self, tag, jsonpath):
        return {
        'Name': self.get_classifier_name(tag),
        'JsonPath': jsonpath
    }

    def create_classifier(self, tag, jsonpath):
        try:
            logger.info("About to create the classifier!")
            self.glue_client.create_classifier(JsonClassifier=self.__generate_json_classifier(tag, jsonpath))
            logger.info("Classifier successfully created!\n")
        except ClientError:
            logger.info("Classifier was already created!\n")

    def delete_classifier(self, tag):
        """ Deletes an Athena database within AWS """
        try:
            self.glue_client.get_classifier(Name=self.get_classifier_name(tag))
            try:
                logger.info("About to delete a Glue classifier")
                self.glue_client.delete_classifier(Name=self.get_classifier_name(tag))
                logger.info("Successfully deleted a Glue classifier\n")
            except:
                logger.info("There was an issue in deleting the classifier\n")
        except:
            logger.info("Classifier doesn't exist yet!\n")

    def create_database(self, database):
        """ Creates an Athena database within AWS """

        try:
            self.glue_client.get_database(Name=database)
            logger.info("The database already exists!\n")
        except ClientError:
            logger.info("About to create an Athena database")
            self.glue_client.create_database(DatabaseInput={'Name': database})
            logger.info("Successfully created an Athena database\n")

    def delete_database(self, database):
        """ Deletes an Athena database within AWS """
        try:
            self.glue_client.get_database(Name=database)
            try:
                logger.info("About to delete an Athena database")
                self.glue_client.delete_database(Name=database)
                logger.info("Successfully deleted an Athena database\n")
            except:
                logger.info("There was an issue in deleting the database\n")
        except:
            logger.info("Database doesn't exist yet!\n")

    def delete_crawler(self, database):
        """ Deletes an Glue crawler within AWS """
        try:
            self.glue_client.get_crawler(Name=self.get_crawler_name(database))
            try:
                logger.info("About to delete the Glue Crawler")
                self.glue_client.delete_crawler(Name=self.get_crawler_name(database))
                logger.info("Successfully deleted the Glue Crawler\n")
            except ClientError:
                logger.info("There was an error in deleting the Crawler!\n")
        except ClientError:
            logger.info("Crawler doesn't exist yet!\n")

    def create_crawler(self, tag, database, filetype):
        """ Creates a Crawler to populate the AWS Glue Catalog """
        try:
            self.glue_client.get_crawler(Name=self.get_crawler_name(database))
            logger.info("Glue Crawler already exists!\n")
        except ClientError:
            logger.info("About to create a Glue Crawler")
            self.glue_client.create_crawler(Name=self.get_crawler_name(database), Role="AWSGlueServiceRole-Pegasus",
                                            DatabaseName=database, Targets=self.__s3_target(tag, filetype))
            logger.info("Successfully created a Glue Crawler\n")

    def create_custom_crawler(self, tag, database, filetype):
        """ Creates a Crawler to populate the AWS Glue Catalog """
        try:
            self.glue_client.get_crawler(Name=self.get_crawler_name(database))
            logger.info("Glue crawler with a custom classifier already exists!\n")
        except ClientError:
            logger.info("About to create a Glue Crawler with a custom classifier")
            self.glue_client.create_crawler(Name=self.get_crawler_name(database), Role="AWSGlueServiceRole-Pegasus",
                                            DatabaseName=database, Targets=self.__s3_target(tag, filetype),
                                            Classifiers=[self.get_classifier_name(tag)])
            logger.info("Successfully created a Glue Crawler with a custom classifier\n")

    def start_crawler(self, database):
        """ Starts a Crawler  """
        logger.info("About to start a Glue Crawler")
        self.glue_client.start_crawler(Name=self.get_crawler_name(database))
        logger.info("Successfully started a Glue Crawler\n")

    def check_if_crawler_running(self, database):
        """ Checks if a Crawler is currently running """
        crawler = self.glue_client.get_crawler(Name=self.get_crawler_name(database))
        logger.info("Going to continuously check if crawler is still running every 30 seconds")
        while crawler['Crawler']['State'] != 'READY':
            time.sleep(30)
            logger.info("It's been 30 seconds but it's still not ready so let's check again")
            crawler = self.glue_client.get_crawler(Name=self.get_crawler_name(database))
            continue
        logger.info("Crawler has finally stopped\n")
        return True

    def stop_crawler(self, database):
        """ Stops a Crawler  """
        logger.info("About to stop a Glue crawler")
        self.glue_client.stop_crawler(Name=self.get_crawler_name(database))
        logger.info("Successfully stopped a Glue crawler\n")

    def get_table(self, database, table_name):
        """ Gets the table information from the Glue Catalog  """
        return self.glue_client.get_table(DatabaseName=database, Name=table_name)

    def get_schema(self, database, table_name):
        """ Reads the schema information from the specificied columns  """
        time.sleep(0.1)
        response = self.get_table(database, table_name)
        return response['Table']['StorageDescriptor']['Columns']






