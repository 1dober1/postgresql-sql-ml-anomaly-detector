import os
import requests


SYSTEM_KEYWORDS = [
    "pg_catalog",
    "information_schema",
    "pg_toast",
    "pg_stat_statements",
    "monitoring.",
    "set application_name",
    "show transaction isolation level",
    "begin",
    "commit",
    "rollback",
    "from pg_type",
    "from pg_attribute",
    "from pg_class",
    "from pg_namespace",
    "to_regtype(",
    "::regtype",
]


def is_system_query(text: str) -> bool:
    if not isinstance(text, str):
        return False
    t = text.lower().strip()
    if len(t) < 10 and ("set" in t or "show" in t):
        return True
    return any(kw in t for kw in SYSTEM_KEYWORDS)


def send_telegram(text: str):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    if len(text) > 4000:
        text = text[:4000] + "..."
    try:
        requests.post(
            url,
            data={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception:
        pass


def fetch_usernames_batch(conn, userids):
    if not userids:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT oid, rolname FROM pg_roles WHERE oid = ANY(%s);",
            (list(userids),),
        )
        rows = cur.fetchall()
    return {r["oid"]: r["rolname"] for r in rows}


def build_alert_message(
    username: str, score: float, query_text: str, metrics: dict
) -> str:
    sql_safe = (str(query_text)[:200]).replace("<", "&lt;")
    parts = []
    if metrics.get("exec_time_per_call_ms", 0) > 0:
        parts.append(f"⏱ {metrics['exec_time_per_call_ms']:.2f} ms")
    if metrics.get("rows_per_call", 0) > 0:
        parts.append(f"📄 {int(metrics['rows_per_call'])} rows")
    if metrics.get("shared_read_per_call", 0) > 0:
        parts.append(f"💾 {int(metrics['shared_read_per_call'])} blks")
    if metrics.get("wal_bytes_per_call", 0) > 0:
        parts.append(f"🧾 {int(metrics['wal_bytes_per_call'])} wal")

    metrics_str = " | ".join(parts)
    return (
        f"🚨 <b>ANOMALY DETECTED</b> 🚨\n"
        f"👤 <b>User:</b> {username}\n"
        f"<b>Anomaly score:</b> {score:.3f}\n"
        f"{metrics_str}\n"
        f"--------------------------\n"
        f"SQL: <code>{sql_safe}</code>"
    )
