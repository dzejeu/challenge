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
        :param load_in_chunks: If set to True pandas will read csv by smaller chunks
        :type load_in_chunks: bool
        :rtype: iterator / iterable of pandas.DataFrame
        """
        if load_in_chunks:
            return pd.read_csv(csv_file, chunksize=1000000)
        return [pd.read_csv(csv_file), ]

    def load_to_db(self, dataframe, table_name):
        """
        :param dataframe: pandas.DataFrame to load
        :type dataframe: pandas.DataFrame
        :param table_name: If table does not exist it will be created. If it exists data will be appended to it.
        :type table_name: str
        """
        dataframe.to_sql(table_name, con=self.db_conn, if_exists='append', index=False)

    def create_index(self, table, index_name, columns):
        if not columns:
            raise ValueError('Need at least one column to create index')

        query = 'CREATE INDEX {index} ON {table} (%s)' % ','.join('{}' for c in columns)
        cursor = self.db_conn.cursor()
        cursor.execute(query.format(index=index_name, table=table, *columns))
