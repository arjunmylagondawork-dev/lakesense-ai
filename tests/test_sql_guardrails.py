import pytest

from lakesense.ai.sql_guardrails import UnsafeSQLError, validate_sql

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
    "this is not sql at all",
])
def test_unsafe_sql_is_blocked(bad):
    with pytest.raises(UnsafeSQLError):
        validate_sql(bad, ALLOWED)
