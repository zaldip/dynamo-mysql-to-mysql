import pymysql
from pymysql import Error
from sshtunnel import SSHTunnelForwarder


def connect_to_ssh_tunnel(config):
    """Generates a connection to an SSH tunnel between host and AWS."""

    ssh_host = config["ssh"]["host"]
    ssh_user = config["ssh"]["user"]
    ssh_key_path = config["ssh"]["key_path"]
    db_host = config["mysql"]["host"]
    db_port = config["mysql"]["port"]

    try:
        tunnel = SSHTunnelForwarder(
            (ssh_host, 22),
            ssh_username=ssh_user,
            ssh_pkey=ssh_key_path,
            remote_bind_address=(db_host, db_port),
        )

        return tunnel
    except Exception as e:
        print(f"Error creating SSH tunnel: {e}")
        return None


def connect_to_mysql(config, tunnel):
    """Generates a connection to a Remote MySQL database."""

    try:
        connection = pymysql.connect(
            host="127.0.0.1",
            port=tunnel.local_bind_port,
            user=config["mysql"]["user"],
            password=config["mysql"]["password"],
            database=config["mysql"]["database"],
        )

        return connection

    except Error as db_error:
        print(f"Database connection error: {db_error}")
        return None


def get_mysql_tables(connection, remote_mysql_tables):
    """Retrieves MySQL tables CREATE DDL."""

    cursor = connection.cursor()
    tables = {}

    for index in range(len(remote_mysql_tables)):
        table = remote_mysql_tables[index]
        cursor.execute(f"SHOW CREATE TABLE {table}")
        tables[index] = cursor.fetchone()

    cursor.close()

    return tables


def get_mysql_items(connection, remote_mysql_tables):
    """Retrieves MuSQL items from multiple tables."""

    cursor = connection.cursor()
    items = {}

    for table in remote_mysql_tables:
        cursor.execute(f"SELECT * FROM {table}")
        items[table] = cursor.fetchall()

    cursor.close()

    return items
