import pyodbc

import pandas as pd
from pydantic import constr

from toucan_connectors.toucan_connector import ToucanDataSource, ToucanConnector


class ODBCDataSource(ToucanDataSource):
    query: constr(min_length=1)


class ODBCConnector(ToucanConnector):
    """
    Import data from Microsoft Azure SQL Server.
    """
    data_source_model: ODBCDataSource

    connection_string: str
    autocommit: bool = False
    ansi: bool = False
    connect_timeout: int = None

    def get_connection_params(self):
        con_params = {
            'autocommit': self.autocommit,
            'ansi': self.ansi,
            'timeout': self.connect_timeout
        }
        # remove None values
        return {k: v for k, v in con_params.items() if v is not None}

    def _retrieve_data(self, datasource: ODBCDataSource) -> pd.DataFrame:
        connection = pyodbc.connect(self.connection_string, **self.get_connection_params())

        df = pd.read_sql(datasource.query, con=connection)

        connection.close()
        return df
