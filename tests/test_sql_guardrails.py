import pytest

from lakesense.ai.sql_guardrails import InvalidSQLError, UnsafeSQLError, validate_sql

ALLOWED = ["orders", "daily_revenue", "customer_rfm"]


def test_simple_select_gets_limit():
    sql = validate_sql("SELECT * FROM orders", ALLOWED, max_rows=50)
    assert "LIMIT 50" in sql


def test_existing_small_limit_is_kept():
    assert "LIMIT 5" in validate_sql("SELECT * FROM orders LIMIT 5", ALLOWED, 50)


def test_large_limit_is_capped():
    assert "LIMIT 50" in validate_sql("SELECT * FROM orders LIMIT 100000", ALLOWED, 50)


def test_markdown_fences_and_semicolon_stripped():
    sql = validate_sql("```sql\nSELECT 1 AS x FROM orders;\n```", ALLOWED)
    assert sql.startswith("SELECT")


@pytest.mark.parametrize("sql", ["SELECT * FROM orders LIMIT 5+1000000", "SELECT * FROM orders LIMIT 100 PERCENT"])
def test_limit_tricks_are_overwritten(sql):
    out = validate_sql(sql, ALLOWED, 50)
    assert out.endswith("LIMIT 50") and "PERCENT" not in out


def test_main_schema_qualifier_is_allowed():
    assert "LIMIT" in validate_sql("SELECT * FROM main.orders", ALLOWED)


@pytest.mark.parametrize("bad", ["this is not sql at all", "SELECT 'unterminated FROM orders"])
def test_malformed_sql_is_retryable_not_unsafe(bad):
    with pytest.raises(InvalidSQLError):
        validate_sql(bad, ALLOWED)


def test_cte_names_are_allowed():
    sql = "WITH t AS (SELECT customer_id FROM orders) SELECT * FROM t"
    assert "LIMIT" in validate_sql(sql, ALLOWED)


@pytest.mark.parametrize("bad", [
    "DROP TABLE orders",
    "DELETE FROM orders",
    "UPDATE orders SET status = 'x'",
    "INSERT INTO orders VALUES (1)",
    "CREATE TABLE x AS SELECT 1",
    "SELECT 1; DROP TABLE orders",
    "SELECT * FROM secret_table",
    "SELECT * FROM read_csv('/etc/passwd')",
    "ATTACH 'other.db'",
    "COPY orders TO 'out.csv'",
    "PRAGMA database_list",
    "SELECT * FROM temp.main.orders",
    "SELECT * FROM other_db.main.orders",
])
def test_unsafe_sql_is_blocked(bad):
    with pytest.raises(UnsafeSQLError):
        validate_sql(bad, ALLOWED)


def test_readonly_connection_cannot_read_files_or_write(tmp_path):
    """Defence in depth: even SQL that bypassed the guardrails cannot touch the file system."""
    from dataclasses import replace

    import duckdb

    from lakesense.config import load_settings
    from lakesense.serving.warehouse import connect_readonly

    db = tmp_path / "wh.duckdb"
    with duckdb.connect(str(db)) as con:
        con.execute("CREATE TABLE orders AS SELECT 1 AS id")
    secret = tmp_path / "secret.csv"
    secret.write_text("a\n1\n")
    base = load_settings()
    settings = replace(base, paths=replace(base.paths, warehouse=db))

    with connect_readonly(settings) as ro:
        assert ro.execute("SELECT count(*) FROM orders").fetchone()[0] == 1
        with pytest.raises(duckdb.Error):
            ro.execute(f"SELECT * FROM read_csv('{secret}')").fetchall()
        with pytest.raises(duckdb.Error):
            ro.execute("DELETE FROM orders")
