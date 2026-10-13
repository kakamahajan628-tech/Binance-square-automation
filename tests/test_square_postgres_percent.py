"""Exercise the real psycopg parameter parser without a database/network."""
import ast
from pathlib import Path

import psycopg
import pytest
from psycopg._queries import PostgresQuery
from psycopg.adapt import Transformer

from test_square_postgres import connection


def cleanup_statement():
    root = Path(__file__).resolve().parents[1]
    source = root / 'REPLACEMENT_FILES/CHART_IMAGE_FIX/square_desk/store.py'
    if not source.exists():
        source = root / 'square_desk/store.py'
    tree = ast.parse(source.read_text(encoding='utf-8'))
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and "kind='media'" in node.value and 'LIKE' in node.value:
            return node.value
    raise AssertionError('Expected deployed media-retention query')


def test_real_psycopg_parser_reproduces_deployed_literal_percent_failure():
    query = cleanup_statement().replace('?', '%s')
    with pytest.raises(psycopg.ProgrammingError, match='placeholders'):
        PostgresQuery(Transformer()).convert(query, (123.0,))


def test_adapter_escapes_sql_percent_before_binding_and_preserves_server_wildcards(connection):
    adapter, raw = connection
    adapter.execute(cleanup_statement(), (123.0,))
    converted, args = raw.calls[-1]
    query = PostgresQuery(Transformer())
    query.convert(converted, args)
    assert b"LIKE '%' || entities.id || '%'" in query.query
    assert b'created<$1' in query.query
    assert len(query.params) == 1


def test_bound_percent_and_question_mark_values_remain_untouched(connection):
    adapter, raw = connection
    values = ('sample?', 'A 5% change; symbols $BTC and $SOL; token %s stays a value')
    adapter.execute('INSERT INTO state VALUES (?,?)', values)
    sql, bound = raw.calls[-1]
    assert bound == values
    query = PostgresQuery(Transformer())
    query.convert(sql, bound)
    assert query.query == b'INSERT INTO state VALUES ($1,$2)'
    assert b'5%' in bytes(query.params[1]) and b'%s' in bytes(query.params[1])


def test_sql_without_parameters_does_not_double_literal_percent(connection):
    adapter, raw = connection
    adapter.execute("SELECT '%' AS pattern")
    assert raw.calls[-1] == ("SELECT '%' AS pattern", None)
