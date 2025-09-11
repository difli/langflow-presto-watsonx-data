import pandas as pd
import json
import requests
from langflow.custom import Component
from langflow.io import Output, CodeInput, MultilineInput
from langflow.schema.data import Data
from prestodb import dbapi, auth

class PrestoWatsonxQuery(Component):
    display_name = "Presto Watsonx Query"
    description = (
        "Runs a SQL query on Presto. Requires a custom SSL certificate file. "
        "Ensure REQUESTS_CA_BUNDLE and NO_PROXY environment variables are set before launching Langflow."
    )
    icon = "prestodb"

    inputs = [
        CodeInput(
            name="connection_config",
            display_name="Connection Config (JSON)",
            info="JSON with host, port, user, password, catalog, schema, and ssl_verify (path to your .pem file).",
            value='{\n  "host": "eu-de.services.cloud.techzone.ibm.com",\n  "port": 27141,\n  "user": "ibmlhadmin",\n  "password": "password",\n  "catalog": "hive_data",\n  "schema": "gosalesdw",\n  "ssl_verify": "/tmp/presto-chain.pem"\n}'
        ),
        MultilineInput(
            name="sql_query",
            display_name="SQL Query",
            info="The SQL query to execute.",
            value="SHOW TABLES",
            tool_mode=True,
        )
    ]

    outputs = [
        Output(display_name="Result", name="result", method="run_query"),
    ]

    def run_query(self) -> Data:
        self.status = "Initializing..."
        
        connection_config = self.connection_config
        sql_query = self.sql_query

        if isinstance(connection_config, str):
            try:
                config = json.loads(connection_config)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in connection config: {e}")
        elif isinstance(connection_config, dict):
            config = connection_config
        else:
            raise TypeError(f"Unsupported type for connection_config: {type(connection_config)}")

        host = config.get("host")
        port = config.get("port")
        user = config.get("user")
        password = config.get("password")
        catalog = config.get("catalog")
        schema = config.get("schema")
        ssl_verify_path = config.get("ssl_verify")

        if not all([host, port, user, catalog, schema, ssl_verify_path]):
            raise ValueError("Connection config is missing required fields, including ssl_verify path.")

        session = requests.Session()
        session.verify = ssl_verify_path
        session.trust_env = False

        conn = None
        try:
            self.status = "Connecting with custom certificate..."
            conn = dbapi.connect(
                host=host,
                port=port,
                user=user,
                catalog=catalog,
                schema=schema,
                http_scheme='https',
                auth=auth.BasicAuthentication(user, password) if password else None,
                http_session=session
            )

            self.status = "Executing query..."
            cur = conn.cursor()
            try:
                cur.execute(sql_query)
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
            finally:
                cur.close()
            
            df = pd.DataFrame(rows, columns=columns)
            self.status = f"Query successful: {len(rows)} rows"
            return Data(value=df)

        except Exception as e:
            raise RuntimeError(f"Query failed: {e}") from e
        finally:
            if conn:
                conn.close()
