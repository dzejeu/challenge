from challenge.db.db_client import SqlLiteClient
from unittest import mock
import pytest


def test_should_close_db_connection_when_exits_from_context_manager():
    sql_conn_mock = mock.MagicMock()
    with SqlLiteClient('some_db') as sql:
        sql.db_conn = sql_conn_mock
    sql_conn_mock.close.assert_called_once()


def test_should_raise_exception_when_no_columns_selected_for_index():
    with pytest.raises(ValueError):
        with SqlLiteClient('some_db') as sql:
            sql.create_index('tab', 'idx', [])
