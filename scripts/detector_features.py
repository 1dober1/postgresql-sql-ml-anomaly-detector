import json
import math

LOG_FEATURES = ["shared_read_per_call", "temp_read_per_call", "ms_per_row"]
OTHER_NUM_FEATURES = [
    "calls_per_sec",
    "cache_miss_ratio",
    "temp_share",
    "read_blks_per_row",
    "exec_time_per_call_ms",
    "rows_per_call",
    "wal_bytes_per_call",
]
LEX_FEATURES = [
    "query_len_chars",
    "query_len_norm_chars",
    "num_tokens",
    "num_joins",
    "num_where",
    "num_group_by",
    "num_order_by",
    "num_having",
    "num_union",
    "num_subqueries",
    "num_cte",
    "has_write",
    "has_ddl",
    "has_tx",
    "num_case",
    "num_functions",
]

ALL_FEATURES = LOG_FEATURES + OTHER_NUM_FEATURES + LEX_FEATURES
META_COLS = ["window_start", "window_end", "dbid", "userid", "queryid"]

MODEL_LOG1P_FEATURES = [
    "exec_time_per_call_ms",
    "rows_per_call",
    "wal_bytes_per_call",
    "shared_read_per_call",
    "temp_read_per_call",
    "ms_per_row",
    "read_blks_per_row",
    "calls_per_sec",
]


def _to_number(v):
    if v is None:
        return 0.0
    if isinstance(v, bool):
        return int(v)
    try:
        fv = float(v)
        if math.isnan(fv) or math.isinf(fv):
            return 0.0
        return fv
    except Exception:
        return 0.0


def coerce_features_df(df):
    """Приводим типы и NaN к нормальному виду (для модели и json-вектора)."""
    for c in ALL_FEATURES:
        if c not in df.columns:
            df[c] = 0
        df[c] = df[c].map(_to_number)
    return df


def prepare_model_features_df(df):
    X = df[ALL_FEATURES].copy()
    for c in MODEL_LOG1P_FEATURES:
        if c not in X.columns:
            continue
        X[c] = X[c].map(lambda v: math.log1p(v) if v > 0 else 0.0)
    return X


def build_features_json(row) -> dict:
    """Приводим типы и NaN к нормальному виду (для модели и json-вектора)."""
    return {c: _to_number(row.get(c)) for c in ALL_FEATURES}


def dumps_json(obj) -> str:
    return json.dumps(obj, ensure_ascii=False)
