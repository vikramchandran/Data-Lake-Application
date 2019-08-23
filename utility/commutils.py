import csv
import json
import os
import sys
import logging
import time

logger = logging.getLogger(__name__)


def __create_json_generic(jsonfilename, data):
    with open(jsonfilename, 'w') as f:
        json.dump(data, f)


def create_test_json_simple(jsonfilename):
    data = {"ticker_symbol":"QXZ", "sector":"HEALTHCARE", "change": -0.05, "price":84.51}

    try:
        logger.info("About to create a simple json file!")
        __create_json_generic(jsonfilename, data)
        logger.info("Succesfully created a simple json file\n")
    except:
        logger.info("There was an error in creating the simple json file\n")


def get_json_unnested():
    stringified = '{"first_name": "Bob", "last_name": "Wilkerson", "year_born": "1943", "age": "24", "gender": "M"}\n' \
                  '{"first_name": "Lindsey", "last_name": "Collins", "year_born": "1930", "age": "22", "gender": "F"}\n' \
                  '{"first_name": "Ellison", "last_name": "Harvey", "year_born": "1944", "age": "43", "gender": "M"}\n' \
                  '{"first_name": "Chamberlin", "last_name": "Droves", "year_born": "1930", "age": "22", "gender": "M"}\n' \
                  '{"first_name": "Elizabeth", "last_name": "Weiber", "year_born": "1937", "age": "31", "gender": "F"}\n' \
                  '{"first_name": "Patrick", "last_name": "Steinin", "year_born": "1930", "age": "24", "gender": "M"}\n' \
                  '{"first_name": "Collin", "last_name": "Patterson", "year_born": "1943", "age": "30", "gender": "M"}\n' \
                  '{"first_name": "Beth", "last_name": "Poulson", "year_born": "1943", "age": "30", "gender": "F"}'
    return stringified


def create_test_json_complex(jsonfilename):

    data = {"people": [
                        {"first_name": "Bob", "last_name": "Wilkerson", "year_born": "1943", "age": "24", "gender": "M"},
                        {"first_name": "Lindsey", "last_name": "Collins", "year_born": "1930", "age": "22", "gender": "F"},
                        {"first_name": "Ellison", "last_name": "Harvey", "year_born": "1944", "age": "43", "gender": "M"},
                        {"first_name": "Chamberlin", "last_name": "Droves", "year_born": "1930", "age": "22", "gender": "M"},
                        {"first_name": "Elizabeth", "last_name": "Weiber", "year_born": "1937", "age": "31", "gender": "F"},
                        {"first_name": "Patrick", "last_name": "Steinin", "year_born": "1930", "age": "24", "gender": "M"},
                        {"first_name": "Collin", "last_name": "Patterson", "year_born": "1943", "age": "30", "gender": "M"},
                        {"first_name": "Beth", "last_name": "Poulson", "year_born": "1943", "age": "30", "gender": "F"}
                    ]
            }
    try:
        logger.info("About to create a complex json file!")
        __create_json_generic(jsonfilename, data)
        logger.info("Succesfully created a complex json file\n")
    except:
        logger.info("There was an error in creating a complex json file\n")


def create_nested_json(jsonfilename):

    data = {
            "id": "0001",
            "type": "donut",
            "name": "Cake",
            "ppu": 0.55,
            "batters":
                {
                    "batter":
                        [
                            { "id": "1001", "type": "Regular" },
                            { "id": "1002", "type": "Chocolate" },
                            { "id": "1003", "type": "Blueberry" },
                            { "id": "1004", "type": "Devil's Food" }
                        ]
                },
            "topping":
                [
                    { "id": "5001", "type": "None" },
                    { "id": "5002", "type": "Glazed" },
                    { "id": "5005", "type": "Sugar" },
                    { "id": "5007", "type": "Powdered Sugar" },
                    { "id": "5006", "type": "Chocolate with Sprinkles" },
                    { "id": "5003", "type": "Chocolate" },
                    { "id": "5004", "type": "Maple" }
                ]
            }
    try:
        logger.info("About to create a nested json file!")
        __create_json_generic(jsonfilename, data)
        logger.info("Succesfully created a nested json file\n")
    except:
        logger.info("There was an error in creating a nested json file\n")


def create_test_json_optional(optjsonfilename):
    data = {"people": [
                        {"first_name": "Bob", "last_name": "Wilkerson", "year_born": "1943", "age": "24"},
                        {"first_name": "Lindsey", "last_name": "Collins", "year_born": "1930", "age": "22", "gender": "F"},
                        {"first_name": "Ellison", "last_name": "Harvey", "year_born": "1944", "age": "43", "gender": "M"},
                        {"first_name": "Chamberlin", "last_name": "Droves", "year_born": "1930", "age": "22", "gender": "M"},
                        {"first_name": "Elizabeth", "year_born": "1937", "age": "31", "gender": "F"},
                        {"first_name": "Patrick", "last_name": "Steinin", "year_born": "1930", "age": "24", "gender": "M"},
                        {"first_name": "Collin", "last_name": "Patterson", "year_born": "1943", "age": "30", "gender": "M"},
                        {"first_name": "Beth", "last_name": "Poulson", "age": "30", "gender": "F"}
                    ]
    }
    try:
        logger.info("About to create a json file with optional attributes!")
        __create_json_generic(optjsonfilename, data)
        logger.info("Succesfully created a json file with optional attributes\n")
    except:
        logger.info("There was an error in creating a json file with optional attributes\n")


def create_test_backspace(backspacefilename):
    try:
        logger.info("About to create a backspace file!")
        data = ["Bob\b1943\b24\bM",
                "Lindsey\bCollins\b1930\b22\bF",
                "Ellison\bHarvey\b1944\b43\bM",
                "Chamberlin\bDroves\b1930\b22,\bM",
                "Elizabeth\bWeiber\b1937\b31\bF",
                "Patrick\bSteinin\b1930\b24\bM",
                "Collin\bPatterson\b1943\b30\bM",
                "Beth\bPoulson\b1943\b30\bF"]

        kwargs = {'newline': ''}
        mode = 'w'
        if sys.version_info < (3, 0):
            kwargs.pop('newline', None)
            mode = 'wb'

        with open(backspacefilename, mode, **kwargs) as fp:
            writer = csv.writer(fp, delimiter=',')
            writer.writerow(["first_name", "last_name", "year_born", "age", "gender"])
            writer.writerows(data)
        logger.info("Succesfully created backspace file\n")
    except:
        logger.info("There was an error in creating the backspace file\n")


def create_test_csv_generic(csvfilename, data):
    kwargs = {'newline': ''}
    mode = 'w'
    if sys.version_info < (3, 0):
        kwargs.pop('newline', None)
        mode = 'wb'

    with open(csvfilename, mode, **kwargs) as fp:
        writer = csv.writer(fp, delimiter=',')
        writer.writerow(["first_name", "last_name", "year_born", "age", "gender"])
        writer.writerows(data)


def create_test_csv(csvfilename):
    try:
        logger.info("About to create a csv file!")
        data = [("Bob", "Wilkerson", 1943, 24, "M"),
                ("Lindsey", "Collins", 1930, 22, "F"),
                ("Ellison", "Harvey", 1944, 43, "M"),
                ("Chamberlin", "Droves", 1930, 22, "M"),
                ("Elizabeth", "Weiber", 1937, 31, "F"),
                ("Patrick", "Steinin", 1930, 24, "M"),
                ("Collin", "Patterson", 1943, 30, "M"),
                ("Beth", "Poulson", 1943, 30, "F")]

        create_test_csv_generic(csvfilename, data)
        logger.info("Succesfully created csv file\n")
    except:
        logger.info("There was an error in creating the csv file\n")


def create_test_csv_altered(csvaltfilename):
    try:
        logger.info("About to create the altered csv file!")
        data = [("Greenberg", "Hoffington", 1943, 24, "G"),
                ("Bob", "Hurrid", 1930, 22, "M"),
                ("Patrick", "Steinin", 2002, 24, "F")]

        create_test_csv_generic(csvaltfilename, data)
        logger.info("Succesfully created the altered csv file\n")
    except:
        logger.info("There was an error in creating the csv file\n")


def get_json(jsonfilename):
    with open(jsonfilename) as f:
        data = json.load(f)
    return data


def get_json_str(jsonfilename):
    return json.dumps(get_json(jsonfilename))


def print_json_generic(jsonfilename):
    with open(jsonfilename, 'r') as handle:
        parsed = json.load(handle)
    logger.info(json.dumps(parsed, indent=4, sort_keys=True))


def print_json_simple(jsonfilename):
    logger.info("About to print out the simple json file that was uploaded to S3\n")
    print_json_generic(jsonfilename)


def print_json_complex(jsonfilename):
    logger.info("About to print out the complex json file that was uploaded to S3\n")
    print_json_generic(jsonfilename)


def print_json_optional(jsonoptfilename):
    logger.info("\nAbout to print out the json file with the optional attributes that was uploaded to S3\n")
    print_json_generic(jsonoptfilename)


def print_csv(csvfilename):
    logger.info("About to print out the csv that was uploaded to S3\n")
    with open(csvfilename) as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            logger.info(', '.join(row))


def print_dict(dict):
    logger.info(json.dumps(dict, indent=4, sort_keys=True))


def print_query_result(dictinput):
    logger.info("\nGoing to print results of the query in csv form since that is the output "
                 "created by AWS automatically\n")
    time.sleep(0.1)
    for i in range(len(dictinput)):
        data = dictinput[i]['Data']
        print('\n')
        for result in range(len(data)):
            try:
                if result != len(data) - 1:
                    print(data[result]['VarCharValue'] + ", ", end='')
                else:
                    print(data[result]['VarCharValue'], end='')
                    if i == len(dictinput) - 1:
                        print('\n')
            except:
                if result != len(data) - 1:
                    print(" " + ", ", end='')
                else:
                    print(" ", end='')
                    if i == len(dictinput) - 1:
                        print('\n')


def remove_with_os(filename):
    os.remove(filename)


def delete_testfile(testfilename, file_type):
    time.sleep(2)
    try:
        logger.info("About to delete the {} file that was used for testing purposes".format(file_type))
        remove_with_os(testfilename)
        logger.info("Successfully deleted the {} file that was used for testing purposes\n".format(file_type))
    except FileNotFoundError:
        logger.info("The {} file was already deleted!\n".format(file_type))


def delete_opttestfile_json(optjsonname):
    time.sleep(2)
    try:
        logger.info("About to delete the json file with optional attributes that was used for testing purposes")
        remove_with_os(optjsonname)
        logger.info("Successfully deleted the json file with optional attributes that was used for testing purposes\n")
    except FileNotFoundError:
        logger.info("The json file with optional attributes was already deleted!\n")


def delete_altered_csv(altcsvname):
    time.sleep(2)
    try:
        logger.info("About to delete the altered csv that was used for testing purposes")
        remove_with_os(altcsvname)
        logger.info("Successfully deleted the altered csv that was used for testing purposes\n")
    except FileNotFoundError:
        logger.info("The altered csv file was already deleted!\n")


