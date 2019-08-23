# Data Lake Platform Adoption with POC
> The goal of this repo and the exercises in it is to walk through engineers ACP use cases for Analytical Services. While doing it, we aim to show rough edges of those services, perf consideration, immutability challenges and obviously some solutions to those problems

Each use case provides step by step instruction to simulate common scenarios in production like CRUD and Change Data Capture. Additionally, how services data can be introduced into AWS Analytical Services.
Each step shall wipe out the environment, resources it relies on and recreate all every time executed. Here is a link to the Confluence
page for more details: https://accoladeinc.atlassian.net/wiki/spaces/PD/pages/79364467/Dlp-Poc+Github+Repo+Documentation

The project requires three parameters; use-case, step-id, tag
* use-case: group of exercises(steps) for a particular topic 
* step-id: an exercise within a topic
* tag:an arbitrary value to tag each AWS resources and isolate engineers in AWS Sandbox.


The logs on the console shall provide enough information to showcase the use cases. Please refer
to AWS documentation for further explanation. 


## Use Case 1: Slice/ Dice CSV Data
We shall use Athena to access file(s) sitting in s3
1. create a bucket with folder /csv/
2. copy the csv file to this folder

#### Step 1: Access CSV file without metadata in Glue Catalog
Purpose: Show that Athena doesn't properly function without metadata in Glue Catalog

1. Read the data via Athena
2. Showcase the errors
#### Step 2: Access CSV file with metadata in Glue Catalog
Purpose: Show how Athena properly functions with metadata in Glue Catalog

1. Deploy Glue Crawler and Catalog
2. Read the metadata
3. Read the data via Athena


#### Step 3:  Generate metadata via Athena
Purpose: Show how to generate metadata via an Athena query

1. Manually generate metadata in the Catalog via Athena
2. Read the metadata
3. Read the updated data via Athena

#### Step 4:  Update CSV file 
Purpose: Show how Athena doesn't store underlying data and only properly functions with updated metadata

1. Run Step 3
2. Update the existing data in the backend
3. Read the updated data via Athena
4. Showcase the errors
5. Deploy Glue Crawler and Catalog on the updated data
6. Read the updated metadata
7. Read the updated data via Athena


~~#### Step3:Simulate Immutability of s3 data~~
~~1. Read the data~~  
~~2. Insert a row via Athena as duplicate to an existing record~~
~~3. Read the data and print to point out that we have duplicate in the data~~
We learned that that Athena does not support certain DML commands like update and insert commands, so we
can't insert a row via Athena

~~#### Step4: Dedup~~
~~1. Read the data~~  
~~2. Run dedup data via Athena~~
~~3. Read the data~~ 
Since Athena doesn't support numerous DML commands, we also can't showcase dedup. 

## Use Case 2: Slice/Dice Complex Data 
Files would be cvs, backspace delimited files, json
1. Create a bucket with the following folders for the file types
    1. /json/
    2. /json-omitted/
    3. /complex_json/
    4. /backspace/
2. Copy the files to their respective folders



 
#### Step 1: For a simple Json file
Purpose: Show how the built-in json classifier correctly crawls simple json

1. Deploy Glue Crawler and Catalog
2. Read the metadata
3. Read the data via Athena

#### Step 2: For a complex Json file(Explicit metadata)
Purpose: Show how the built-in json classifier crawls the entire json file, even if it
has a complex data structure. Furthermore, this step shows how to query complex data
using Athena if we have the metadata explicitly predefined using a Crawler. 


1. Deploy Glue Crawler and Catalog
2. Read the metadata
3. Read the data via Athena

#### Step 3: For a complex Json file(Generic metadata)
Purpose: This step shows how to query complex json data without having complex, explicit metadata 
predefined. Basically, we could use built-in json queries seen in Presto to simulate
the same output from step 2. The result is the same but the main difference is that
the data structure interpreation is happening during query creation time rather than
during table creation time. In this instance, the data structure interpretation is
is happening during query creation time, which permits for more dynamic querying but
requries users to have a understanding of Presto json functions.

1. Explain built-in Presto json fxns
2. Run ad-hoc json queries



#### Step 4: For a complex Json file(Custom Classifier)
Purpose: This step shows how to use a custom classifier for a complex json file to classify a specific
part of the complex json file instead of the entire complex json file. 

1. Create & Deploy Custom Glue Classifier
2. Read the metadata 


#### Step 5: For a Json file with Omitted Attribute
Purpose: Show how the the Crawler handles omitted attributes, like an optional
address attribute, for a complex json file.

1. Deploy Glue Crawler and Catalog
2. Read the metadata
3. Create & Deploy Custom Glue Classifier
4. Read the updated metadata 



#### Step 6: For a backspace delimited file
Purpose: Show how you need to build a custom classifier for Grok patterns

1. Deploy Glue Crawler and Catalog
2. Read the metadata
3. Read via Athena
4. Showcase the error 
5. Create & Deploy Custom Glue Classifier (Grok Pattern)
6. Read the updated metadata
7. Read the data via Athena



## Use Case 3: Firehose Data Transformation

Create the following resources:
1. S3 bucket
2. Firehose Stream

#### Step 1: Transform simple Json data without metadata
Purpose: Show how metadata must be in Glue Catalog for Firehose to properly transform function

1. Send a json file to Firehose through boto
2. Have Firehose ingest and transform the simple json file into Parquet
3. Showcase the errors 

#### Step 2: Transform simple Json data with metadata
Purpose: Show how Firehose properly functions for a simple file with metadata present in Catalog

1. Deploy the Crawler on the json file
2. Send a simple json file to Firehose through boto
3. Dump the Parquet file into S3

#### Step 3: Transform complex Json data with metadata 
Purpose: Show how Firehose properly functions even with complex, nested json files without any 
custom classifiers. This is especially important since a team only has to use a Glue Crawler
to generate the metadata for a complex, nested file for Firehose to be able to successfully transform it
to Parquet. 

1. Deploy the Crawler on the complex json file
2. Configure Firehose to transform the json into Parquet by pointing Firehose to the Glue Catalog
3. Send a complex json file to Firehose through Boto 
4. Dump the Parquet file into S3

#### Step 4: Transform complex Json data with with metadata & classifier
Purpose: Show how Firehose could also work with a custom Glue classifier if that is necessary. 

1. Deploy the Crawler with the custom Classifier on the complex json file
2. Configure Firehose to transform the json into Parquet by pointing Firehose to the Glue Catalog
3. Transform the json to only include the fields specified on the classifier.
4. Send the transformed json file to Firehose through Boto
5. Dump the Parquet file into S3

#### Step 5: Transform updated Json data with metadata
Purpose: Show how data transformation still works even when underlying data is updated and you want
to maintain previously created resources. The method below is the best option if you want to maintain 
"Version 1" resources and not delete them. If you never plan on using "Version 1" resources, then you 
only have to re-run the same Crawler on the updated data to update the schema that exists within 
the Glue Catalog. Everything else will works as is. 

1. Update json data and create a "Version 2" for every resource previously created
2. Deploy Crawler Version 2 on the updated json file
3. Send a complex json file to Firehose Version 2 through boto
4. Point Firehose Version 2 to the Glue Catalog Version 2
5. Have Firehose Version 2 ingest and transform the json file into Parquet
6. Dump the Parquet file into S3 bucket version 2

## Use Case 4: Glue Data Transformation

Important Note: Glue ETL runs Spark, which may be more expensive than using a Firehose stream 
to convert data to Parquet. Please refer to Use Case 3 to learn more about Firehose
data transformation.

Create the following resources:
1. S3 bucket
2. Upload a json file into this bucket

#### Step 1: Transform Json data without metadata
Purpose: Show how metadata must be in Glue Catalog for Glue ETL job to properly transform function

1. Run an ETL job without pointing to Catalog
2. Showcase the errors

#### Step 2: Transform simple Json data with metadata
Purpose: Show how a Glue ETL to properly transform data with metadata in Glue Catalog

1. Deploy Glue Catalog and Crawler
2. Run an ETL job pointing to Catalog that transforms the json file to parquet
3. Read the transformed file

#### Step 3: Transform complex Json data with metadata
Purpose: Show how a Glue ETL to properly transform data with metadata in Glue Catalog

1. Deploy Glue Catalog and Crawler with Custom Classifier
2. Run an ETL job pointing to Catalog that transforms the json file to parquet
3. Read the transformed file 











## Use Case X: Activity Service
Activity Service is expected to introduce many activity schemas and here we would like to provide analytical capabilities on those activity data.
Here is the proposal on [the confluence](https://accoladeinc.atlassian.net/wiki/spaces/PD/pages/3570246/Activity+Service+Data+Lake+Adoption)

The example sets up the following;
1. Wipes off resources with a given tag
2. Creates of dynamo table with Activity Schema
3. Creates Glue Classifier and Registers Json Schema for that Activity
4. Creates Glue Crawler and Glue Catalog
5. Creates of s3 bucket
6. Sets up Firehose
7.  Sets up Athena Table on Glue Catalog

Then
1. Initial data population
   * Populates batch of data into that Dynamo Table
   * Runs Athena Query and get Count

2. Updates on data
   * Inserts and updates data on the Dynamo Table
   * Runs Athena Query and checks updated record and shows two version
   * Runs Athena Query and checks the inserted record

3. Duplication by Athena(costly)
   * Runs Athena Query to show how to select the latest version of the same record

4. Deduplication of multiple versions
   * Runs Athena Update Query and purge two versions of the same data by picking the latest versions
   * Runs Athena Query and verifies that data got purged

