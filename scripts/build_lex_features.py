"""Build SQL lexical features and update the DB table."""

import hashlib
import re
from datetime import datetime, timezone
from typing import List

import psycopg
from psycopg.rows import dict_row

try:
    from db_config import DB_CONFIG
except Exception:
    from scripts.db_config import DB_CONFIG

GET_CANDIDATES = """
SELECT DISTINCT ON (s.dbid, s.userid, s.queryid)
    s.dbid,
    s.userid,
    s.queryid,
    s.query_text,
    s.snapshot_ts
FROM monitoring.pgss_snapshots_raw s
WHERE s.query_text IS NOT NULL
ORDER BY s.dbid, s.userid, s.queryid, s.snapshot_ts DESC;
"""


GET_EXISTING_MD5 = """
SELECT
    dbid, userid, queryid, query_md5
FROM monitoring.query_lex_features;
"""

UPSERT_LEX = """
INSERT INTO monitoring.query_lex_features (
    dbid, userid, queryid,
    query_text, query_md5,
    query_len_chars, query_len_norm_chars, num_tokens,
    num_joins, num_where, num_group_by, num_order_by, num_having,
    num_union, num_subqueries, num_cte,
    has_write, has_ddl, has_tx,
    num_case, num_functions,
    last_seen_ts
)
VALUES (
    %(dbid)s, %(userid)s, %(queryid)s,
    %(query_text)s, %(query_md5)s,
    %(query_len_chars)s, %(query_len_norm_chars)s, %(num_tokens)s,
    %(num_joins)s, %(num_where)s, %(num_group_by)s, %(num_order_by)s, %(num_having)s,
    %(num_union)s, %(num_subqueries)s, %(num_cte)s,
    %(has_write)s, %(has_ddl)s, %(has_tx)s,
    %(num_case)s, %(num_functions)s,
    %(last_seen_ts)s
)
ON CONFLICT (dbid, userid, queryid)
DO UPDATE SET
    query_text = EXCLUDED.query_text,
    query_md5 = EXCLUDED.query_md5,

    query_len_chars = EXCLUDED.query_len_chars,
    query_len_norm_chars = EXCLUDED.query_len_norm_chars,
    num_tokens = EXCLUDED.num_tokens,

    num_joins = EXCLUDED.num_joins,
    num_where = EXCLUDED.num_where,
    num_group_by = EXCLUDED.num_group_by,
    num_order_by = EXCLUDED.num_order_by,
    num_having = EXCLUDED.num_having,

    num_union = EXCLUDED.num_union,
    num_subqueries = EXCLUDED.num_subqueries,
    num_cte = EXCLUDED.num_cte,

    has_write = EXCLUDED.has_write,
    has_ddl = EXCLUDED.has_ddl,
    has_tx = EXCLUDED.has_tx,

    num_case = EXCLUDED.num_case,
    num_functions = EXCLUDED.num_functions,

    last_seen_ts = EXCLUDED.last_seen_ts;
"""


_re_block_comment = re.compile(r"/\*.*?\*/", re.DOTALL)
_re_line_comment = re.compile(r"--[^\n]*")
_re_dollar_quoted = re.compile(r"\$\$.*?\$\$", re.DOTALL)
_re_single_quoted = re.compile(r"'([^']|'')*'")
_re_number = re.compile(r"\b\d+(\.\d+)?\b")
_re_ws = re.compile(r"\s+")
_re_token = re.compile(r"[a-z_]+|\d+|<=|>=|<>|!=|[()*,;=]")


def normalize_sql(sql):
    """Normalize SQL for stable comparison and tokenization.

    Removes comments, replaces literals and numbers, lowercases, and
    collapses whitespace.
    """
    s = sql
    s = _re_block_comment.sub(" ", s)
    s = _re_line_comment.sub(" ", s)
    s = _re_dollar_quoted.sub(" ?", s)
    s = _re_single_quoted.sub(" ?", s)
    s = _re_number.sub(" 0 ", s)
    s = s.lower()
    s = _re_ws.sub(" ", s).strip()
    return s


def md5_text(s):
    """Return MD5 hash of text as a hex string."""
    return hashlib.md5(s.encode("utf-8", errors="ignore")).hexdigest()


def count_kw(norm, kw):
    """Count keyword occurrences as standalone tokens."""
    return len(re.findall(rf"\b{re.escape(kw)}\b", norm))


def has_any(norm: str, kws: List[str]) -> bool:
    """Return True if any keyword is present."""
    return any(re.search(rf"\b{re.escape(k)}\b", norm) for k in kws)


def count_subqueries(norm: str) -> int:
    """Count subqueries matching the '( select' pattern."""
    return len(re.findall(r"\(\s*select\b", norm))


def count_functions(norm):
    """Count function calls using a simple pattern.

    Excludes SQL keywords to avoid inflating the count.
    """
    blacklist = {
        "select",
        "from",
        "where",
        "join",
        "inner",
        "left",
        "right",
        "full",
        "cross",
        "group",
        "order",
        "having",
        "limit",
        "offset",
        "values",
        "into",
        "update",
        "insert",
        "delete",
        "create",
        "alter",
        "drop",
        "truncate",
        "on",
        "and",
        "or",
        "case",
        "when",
        "then",
        "else",
        "end",
    }
    candidates = re.findall(r"\b([a-z_][a-z0-9_]*)\s*\(", norm)
    return sum(1 for w in candidates if w not in blacklist)


def compute_lex_features(query_text):
    """Compute lexical features for a query text."""
    norm = normalize_sql(query_text)

    tokens = _re_token.findall(norm)

    num_joins = count_kw(norm, "join")
    num_where = count_kw(norm, "where")
    num_group_by = len(re.findall(r"\bgroup\s+by\b", norm))
    num_order_by = len(re.findall(r"\border\s+by\b", norm))
    num_having = count_kw(norm, "having")
    num_union = count_kw(norm, "union")
    num_cte = 1 if norm.startswith("with ") else 0
    num_subq = count_subqueries(norm)

    has_write = has_any(norm, ["insert", "update", "delete", "merge"])
    has_ddl = has_any(norm, ["create", "alter", "drop", "truncate"])
    has_tx = has_any(norm, ["begin", "commit", "rollback"])

    num_case = count_kw(norm, "case")
    num_functions = count_functions(norm)

    return {
        "query_len_chars": len(query_text),
        "query_len_norm_chars": len(norm),
        "num_tokens": len(tokens),
        "num_joins": num_joins,
        "num_where": num_where,
        "num_group_by": num_group_by,
        "num_order_by": num_order_by,
        "num_having": num_having,
        "num_union": num_union,
        "num_subqueries": num_subq,
        "num_cte": num_cte,
        "has_write": has_write,
        "has_ddl": has_ddl,
        "has_tx": has_tx,
        "num_case": num_case,
        "num_functions": num_functions,
        "query_md5": md5_text(norm),
        "query_text": query_text,
    }


def load_existing_md5(cur):
    """Load stored md5 values keyed by (dbid, userid, queryid)."""
    cur.execute(GET_EXISTING_MD5)
    rows = cur.fetchall()
    return {(r["dbid"], r["userid"], r["queryid"]): r["query_md5"] for r in rows}


def load_candidates(cur):
    """Load candidate queries from recent pgss snapshots."""
    cur.execute(GET_CANDIDATES)
    return cur.fetchall()


def build_lex_features():
    """Compute features and upsert monitoring.query_lex_features."""
    with psycopg.connect(**DB_CONFIG, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            existing = load_existing_md5(cur)
            candidates = load_candidates(cur)

            if not candidates:
                print("No candidates with query_text found.")
                return

            now_ts = datetime.now(timezone.utc)
            to_upsert = []

            for row in candidates:
                key = (row["dbid"], row["userid"], row["queryid"])
                query_text = row["query_text"]

                feats = compute_lex_features(query_text)

                old_md5 = existing.get(key)
                if old_md5 is not None and old_md5 == feats["query_md5"]:
                    continue

                to_upsert.append(
                    {
                        "dbid": row["dbid"],
                        "userid": row["userid"],
                        "queryid": row["queryid"],
                        "last_seen_ts": now_ts,
                        **feats,
                    }
                )

            if not to_upsert:
                print("Lex features are up-to-date (nothing to insert/update).")
                return

            cur.executemany(UPSERT_LEX, to_upsert)
            conn.commit()

            print(
                f"{datetime.now()}: upserted {len(to_upsert)} rows into monitoring.query_lex_features"
            )


if __name__ == "__main__":
    build_lex_features()
