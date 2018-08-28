import pandas as pd
import sqlite3
from cached_property import cached_property
from contextlib import AbstractContextManager


class SqlLiteClient(AbstractContextManager):
    def __init__(self, db_filepath):
        self.db_filepath = db_filepath

    @cached_property
    def db_conn(self):
        return sqlite3.connect(self.db_filepath)

    def __exit__(self, exc_type, exc_value, traceback):
        self.db_conn.close()

    def dataframe_from_csv(self, csv_file, load_in_chunks=False):
        """
        :param csv_file: CSV file which content will be loaded into DataFrame
        :type csv_file: str
        :rtype: pandas.DataFrame
        """
        if load_in_chunks:
            return pd.read_csv(csv_file, chunksize=1000000)
        return [pd.read_csv(csv_file),]

    def load_to_db(self, dataframe, table_name):
        """
        :param dataframe: pandas.DataFrame to load
        :type dataframe: pandas.DataFrame
        :param table_name: If table does not exist it will be created. If it exists data will be appended to it.
        :type table_name: str
        """
        dataframe.to_sql(table_name, con=self.db_conn, if_exists='append', index=False)
