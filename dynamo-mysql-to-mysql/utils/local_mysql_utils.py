from datetime import datetime
from utils.dynamodb_utils import get_dynamodb_items

import pymysql
import decimal
import json
from pymysql import Error
import mysql.connector

def parse_int(value):
    if value is None:
        return 0
    return int(value)

def parse_bool(value):
    if value is None:
        return None
    return bool(value)

def parse_decimal(value):
    if value is None:
        return 0
    return decimal.Decimal(value)

def connect_to_local_mysql(config):
    """Generates a connection to a local MySQL database."""

    try:
        connection = pymysql.connect(
            host=config["mysql_local"]["host"],
            user=config["mysql_local"]["user"],
            password=config["mysql_local"]["password"],
            database=config["mysql_local"]["database"],
        )

        return connection
    except Error as e:
        print(f"ERROR: {e}")
        return None
    
def create_mysql_tables(connection, mysql_tables):
    """Create local MySQL tables based on remote MySQL tables pased as params."""

    try:
        cursor = connection.cursor()

        for index in range(len(mysql_tables)):
            print(f"INFO: Creating local target table: '{mysql_tables[index][0]}'.")
            cursor.execute(mysql_tables[index][1])

        connection.commit()
        cursor.close()

        return True
    except Error as e:
        print(f"Error: {e}")

        return False

def insert_mysql_data(connection, mysql_items):
    """Insert Data from Remote MySQL into local MySQL database."""

    try:
        cursor = connection.cursor()

        for table in mysql_items:
            print(f"INFO: inserting data into '{table}' Table.")

            for item in mysql_items[table]:
                item = [
                    "1970-01-01 00:00:00" if value == "0000-00-00 00:00:00" else value
                    for value in item
                ]

                insert_query = (
                    f"INSERT INTO {table} VALUES ({', '.join(['%s'] * len(item))})"
                )

                cursor.execute(insert_query, item)

        connection.commit()
        cursor.close()

        return True
    except Error as e:
        print(f"Error: {e}")

        return False
    
def infer_schema(data):
    """Infers a schema from DynamoDB data."""
    schema = {}
    for item in data:
        for key, value in item.items():
            if key not in schema:
                schema[key] = type(value).__name__
            elif schema[key] != type(value).__name__:
                # Handling mixed types
                schema[key] = 'str'  # Default to string
    return schema

def generate_ddl(table_name, schema, primary_key=None):
    """Generates a MySQL DDL statement."""
    columns = []

    for column, data_type in schema.items():
        mysql_type = {
            'str': 'VARCHAR(255)',
            'int': 'INT',
            'float': 'FLOAT',
            'Decimal': 'DECIMAL(10,2)',
            'list': 'JSON',
            'dict': 'JSON', 
            'bool': 'BOOLEAN'
        }.get(data_type, 'VARCHAR(255)') 

        # Add exceptions for known mixed values, examples:
        # if "timestamp" in column.lower():
        #     mysql_type = "BIGINT"
        # elif mysql_type == "DECIMAL(10,2)" and "phone" in column.lower():
        #     mysql_type = "BIGINT"
            
        columns.append(f"`{column}` {mysql_type}")

    primary_key_clause = ""
    if primary_key:
        primary_key_clause = f", PRIMARY KEY ({', '.join([f'`{pk}`' for pk in primary_key])})"
    
    ddl = f"CREATE TABLE `{table_name}` ({', '.join(columns)}{primary_key_clause});"
    return ddl

def execute_ddl(ddl, mysql_conn):
    """Executes the DDL statement in MySQL."""
    mydb = None 
    try:
        cursor = mysql_conn.cursor()
        cursor.execute(ddl)
        mysql_conn.commit()
    except mysql.connector.Error as err:
        print(f"ERROR: executing DDL: {err}")
        if err.errno == 1061:
            print("ERROR: Index already exists")
        elif err.errno == 1050:
            print("ERROR: Table already exists")
        elif err.errno == 1062:
            print("ERROR: Duplicate entry for key")
    except Exception as err:
        print(f"ERROR: A general error occurred: {err}")
    finally:
        if mydb and mydb.is_connected():
            cursor.close()

def table_exists_in_mysql(mysql_conn, table_name):
    """Checks if a table exists in a MySQL database."""

    cursor = mysql_conn.cursor()
    cursor.execute("SHOW TABLES LIKE %s", (table_name,))
    result = cursor.fetchone()
    return result is not None

def create_dynamo_tables(connection, dynamodb, dynamo_tables):
    """Creates a table in a Mysql database by reading DynamoDB element."""

    try:
        for table_name in dynamo_tables:
            print(f"INFO: Creating local target table: '{table_name}'.")

            # Check if table already exists in MySQL
            if table_exists_in_mysql(connection, table_name):
                print(f"INFO: Table '{table_name}' already exists in Target MySQL. Skipping creation.")
                continue

            table = dynamodb.Table(table_name)

            # 1. Scan DynamoDB (retrieves only 1 item)
            # no multiple items evaluation for performance reasons
            response = table.scan(Limit=1)

            # 2. Infer Schema
            schema = infer_schema(response['Items'])

            # 3. Get Primary Key info from DynamoDB
            key_schema = table.key_schema
            primary_key_names = [key['AttributeName'] for key in key_schema if key['KeyType'] == 'HASH']
            if any(key['KeyType'] == 'RANGE' for key in key_schema):
                primary_key_names.extend([key['AttributeName'] for key in key_schema if key['KeyType'] == 'RANGE'])

            # 4. Generate DDL
            ddl = generate_ddl(table_name, schema, primary_key_names)

            # 5. Execute DDL (comment out if you just want to see the DDL)
            execute_ddl(ddl, connection)
        return True
    except Error as e:
        print(f"Error: {e}")

        return False
    
class DecimalEncoder(json.JSONEncoder):
  def default(self, obj):
    if isinstance(obj, decimal.Decimal):
      return str(obj)
    return super(DecimalEncoder, self).default(obj)

def insert_dynamo_data(dynamodb, mysql_conn, dynamodb_tables):
    """Insert Data from DynamoDB into local MySQL database."""

    cursor = None  # Initialize cursor outside the try block
    try:
        cursor = mysql_conn.cursor()

        for table_name in dynamodb_tables:
            print(f"INFO: inserting data into '{table_name}' Table.")
             # Fetch destination table column names
            cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
            destination_keys = [row[0] for row in cursor.fetchall()]

            dynamodb_items = get_dynamodb_items(dynamodb, table_name)

            for item in dynamodb_items:
                # Filter item keys based on destination table columns
                filtered_item = {k: v for k, v in item.items() if k in destination_keys} 
                keys = list(filtered_item.keys())
                placeholders = ','.join(['%s'] * len(keys))
                insert_sql = f"INSERT INTO `{table_name}` ({','.join(keys)}) VALUES ({placeholders})"
                
                values = []
                
                for key, value in filtered_item.items():
                    if isinstance(value, list):
                        if not value:
                            values.append("{}")
                        elif all(isinstance(item, decimal.Decimal) for item in value):
                            values.append(json.dumps([str(item) for item in value], separators=(',', ':')))
                        else:
                          values.append(json.dumps(value, cls=DecimalEncoder)) 
                    elif isinstance(value, dict):
                        values.append(json.dumps(value, cls=DecimalEncoder))
                    elif isinstance(value, decimal.Decimal):
                        values.append(str(value))
                    elif value is None: 
                        values.append(None)
                    else:
                        values.append(value)
                try:
                    cursor.execute(insert_sql, values)
                except mysql.connector.errors.IntegrityError as ie:
                    print(f"ERROR: Integrity Error inserting into {table_name}: {ie}, Values: {values}")
                    mysql_conn.rollback() # Important to rollback to avoid inconsistencies
                except mysql.connector.errors.ProgrammingError as pe:
                    print(f"ERROR: Programming Error (SQL Syntax) inserting into {table_name}: {pe}, Query: {insert_sql}")
                    return # Stop execution if there is a syntax error.
                except mysql.connector.Error as sqle:
                    print(f"ERROR: MySQL Error inserting into {table_name}: {sqle}, Query: {insert_sql}, Values: {values}")
                    print(f"SQLSTATE: {sqle.sqlstate}")
                    print(f"Error Code: {sqle.errno}")
                    mysql_conn.rollback()

        mysql_conn.commit()
        return True

    except mysql.connector.Error as mysql_conn_err:
        print(f"MySQL Connection or Cursor Error: {mysql_conn_err}")
    except Exception as e:
        print(f"General Error: {e} \nvalues: {values} \nquery: {insert_sql}")
    finally:
        if cursor: # Check if the cursor was created before closing
            cursor.close()