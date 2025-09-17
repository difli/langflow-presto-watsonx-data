import json
from typing import Type

import pandas as pd
import requests
from langchain_core.tools import StructuredTool, Tool
from pydantic import BaseModel, Field
from prestodb import dbapi, auth

from langflow.base.langchain_utilities.model import LCToolComponent
from langflow.io import CodeInput, StrInput, Output
from langflow.field_typing import Tool
from langflow.schema.data import Data


class IBMWatsonxDataPrestoTool(LCToolComponent):
    display_name = "IBM watsonx.data Presto Tool"
    description = "Creates a tool to run SQL queries on a Presto database."
    icon = "prestodb"

    _connection = None

    inputs = [
        CodeInput(
            name="connection_config",
            display_name="Connection Config (JSON)",
            info="JSON with host, port, user, password, catalog, schema, and ssl_verify path.",
            value='{\n  "host": "your-presto-host",\n  "port": 12345,\n  "user": "your-user",\n  "password": "your-password",\n  "catalog": "your-catalog",\n  "schema": "your-schema",\n  "ssl_verify": "/path/to/your/cert.pem"\n}'
        ),
        StrInput(
            name="tool_name",
            display_name="Tool Name",
            info="The name of the tool to be passed to the LLM.",
            value="presto_query_tool",
        ),
        StrInput(
            name="tool_description",
            display_name="Tool Description",
            info="Describe the tool to the LLM. This helps the agent decide when to use it.",
            value="Runs a SQL query on a Presto database and returns the result as a JSON string.",
        ),
    ]

    outputs = [
        Output(display_name="Tool", name="tool", method="build_tool"),
    ]

    def create_tool_schema(self) -> Type[BaseModel]:
        """Create a Pydantic model for the tool's arguments."""

        class PrestoQuerySchema(BaseModel):
            sql_query: str = Field(..., description="The SQL query to execute on the Presto database.")

        return PrestoQuerySchema

    def _get_connection(self):
        """Gets a cached connection or creates a new one."""
        if self._connection and self._connection.closed:
             self._connection = None

        if self._connection:
            return self._connection

        connection_config = self.connection_config
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
            raise ValueError("Connection config is missing required fields.")

        session = requests.Session()
        session.verify = ssl_verify_path
        session.trust_env = False

        try:
            conn = dbapi.connect(
                host=host,
                port=port,
                user=user,
                catalog=catalog,
                schema=schema,
                http_scheme='https',
                auth=auth.BasicAuthentication(user, password) if password else None,
                http_session=session,
            )
            self._connection = conn
            return self._connection
        except Exception as e:
            raise RuntimeError(f"Failed to connect to Presto: {e}") from e

    def _run_tool(self, sql_query: str) -> str:
        """Executes a SQL query on the Presto database and returns the result."""
        self.status = f"Executing query: {sql_query[:50]}..."
        conn = self._get_connection()

        try:
            cur = conn.cursor()
            try:
                cur.execute(sql_query)
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
            finally:
                cur.close()
            
            df = pd.DataFrame(rows, columns=columns)
            self.status = f"Query successful: {len(rows)} rows"
            return df.to_json(orient="records")
        except Exception as e:
            # Reset the connection on failure
            self._connection = None
            self.status = f"Query failed: {e}"
            return f"Error: Query failed with exception: {e}"

    def build_tool(self) -> Tool:
        """Builds the StructuredTool from the component's inputs."""
        tool_schema = self.create_tool_schema()
        
        tool = StructuredTool.from_function(
            name=self.tool_name,
            description=self.tool_description,
            func=self._run_tool,
            args_schema=tool_schema,
            return_direct=False,
        )
        
        self.status = "Presto DB Tool created"
        return tool
