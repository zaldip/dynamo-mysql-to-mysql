import boto3
import decimal
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

def connect_to_dynamodb(config):
    """Generates a connection to a DynamoDB database."""

    try:
        session = boto3.Session(
            aws_access_key_id=config["dynamodb"]["accessKeyId"],
            aws_secret_access_key=config["dynamodb"]["secretAccessKey"],
            region_name=config["dynamodb"]["region"],
        )

        dynamodb = session.resource("dynamodb")

        # Monkey patch Decimal's default Context to allow 
        # inexact and rounded representation of floats
        from boto3.dynamodb.types import DYNAMODB_CONTEXT
        # Inhibit Inexact Exceptions
        DYNAMODB_CONTEXT.traps[decimal.Inexact] = 0
        # Inhibit Rounded Exceptions
        DYNAMODB_CONTEXT.traps[decimal.Rounded] = 0

        return dynamodb

    except NoCredentialsError:
        print("Error: AWS credentials not available.")
    except PartialCredentialsError:
        print("Error: AWS credentials are incomplete.")
    except Exception as e:
        print(f"Error at connecting to dynamodb: {e}")


def get_dynamodb_items(dynamodb, table_name):
    """Retrieves DynamoDB items for a specific table/object pased as param."""

    try:
        table = dynamodb.Table(table_name)
        response = table.scan()
        return response.get("Items", [])
    except Exception as e:
        print(f"Error at getting the items from the tables: {e}")
