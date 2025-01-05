import json

from utils.dynamodb_utils import connect_to_dynamodb
from utils.local_mysql_utils import (
    connect_to_local_mysql,
    create_mysql_tables,
    create_dynamo_tables,
    insert_mysql_data,
    insert_dynamo_data,
)
from utils.mysql_utils import (
    connect_to_mysql,
    connect_to_ssh_tunnel,
    get_mysql_items,
    get_mysql_tables,
)

remote_mysql_tables = [
    "table_1",
    "table_2",
]

remote_dynamo_tables = [
    "table_3",
    "table_4",
]

local_mysql_tables = [
    # dynamodb tables
    "table_3",
    "table_4",
    # mysql tables
    "table_1",
    "table_2",
]

if __name__ == "__main__":
    with open("config/config.json") as config_file:
        config = json.load(config_file)

    print("INFO: Connecting to local MySQL...")
    local_mysql_connection = connect_to_local_mysql(config)

    if not local_mysql_connection:
        print("ERROR: Could not connect to local MySQL")
        exit()

    print("INFO: Connected to local MySQL")

    print("INFO: Creating SSH tunnel...")

    aws_tunnel = connect_to_ssh_tunnel(config)

    if not aws_tunnel:
        print("ERROR: Could not create SSH tunnel")
        exit()

    print("INFO: SSH tunnel created successfully")

    aws_tunnel.start()

    print("INFO: Connecting to DynamoDB...")

    remote_dynamodb_connection = connect_to_dynamodb(config)

    if not remote_dynamodb_connection:
        print("ERROR: Could not connect to DynamoDB")
        exit()

    print("INFO: Connected to DynamoDB")

    print("INFO: Connecting to Remote MySQL...")

    remote_mysql_connection = connect_to_mysql(config, aws_tunnel)

    if not remote_mysql_connection:
        print("INFO: Could not connect to Remote MySQL")
        exit()

    print("INFO: Connected to Remote MySQL")
    
    create_tables_input = input("Do you want to create the tables? (y/N): $")

    if create_tables_input.lower() == "y":
        print("INFO: Creating the tables from MySQL.")
        mysql_success = create_mysql_tables(local_mysql_connection, get_mysql_tables(remote_mysql_connection, remote_mysql_tables))
        
        print("INFO: Creating the tables from Dynamo.")
        dynamo_success = create_dynamo_tables(local_mysql_connection, remote_dynamodb_connection, remote_dynamo_tables)
        if not mysql_success and not dynamo_success:
            print("ERROR: Could not create MySQL and DynamoDB tables.")
            exit()
        elif not mysql_success:
            print("ERROR: Could not create MySQL tables.")
            exit()
        elif not dynamo_success:
            print("ERROR: Could not create DynamoDB tables.")
            exit()

    insert_data_into_mysql_input = input("Do you want to insert data from dynamo db into local mysql? (y/N): $")

    if insert_data_into_mysql_input.lower() == "y":
        print("INFO: Inserting remote data from Dynamo.")
        if not insert_dynamo_data(remote_dynamodb_connection, local_mysql_connection, remote_dynamo_tables):
            print("ERROR: Could not insert Remote Dynamo data")
        else:
            print("INFO: Data from Dynamo inserted into MySQL successfully.")
        
        print("INFO: Inserting the data from MySql.")
        if not insert_mysql_data(local_mysql_connection, get_mysql_items(remote_mysql_connection, remote_mysql_tables)):
            print("ERROR: Could not insert Remote Mysql data")
        else:
            print("INFO: Data from MySQL inserted into MySQL successfully.")

    print("INFO: Closing the connections...")

    local_mysql_connection.close()
    remote_mysql_connection.close()
    aws_tunnel.stop()

    print("INFO: Done.")
