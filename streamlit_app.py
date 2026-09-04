from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from html import escape
import os
import time
from typing import Any, Iterable
from urllib.parse import urlparse, urlunparse

import pandas as pd
import plotly.express as px
import requests
import streamlit as st

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

if load_dotenv is not None:
    load_dotenv()


ASSET_TYPE_ALL = "全部"
ASSET_TYPE_COMMODITY = "商品"
ASSET_TYPE_STOCK = "股票"
ASSET_TYPE_ETF = "ETF"
ASSET_TYPE_OPTIONS = [ASSET_TYPE_COMMODITY, ASSET_TYPE_STOCK, ASSET_TYPE_ETF]
APR_COMPARISON_WINDOWS = (
    ("24h APR", "apr_24h_percent"),
    ("7d APR", "apr_7d_percent"),
    ("15d APR", "apr_15d_percent"),
    ("30d APR", "apr_30d_percent"),
)
APR_COMPARISON_FIELDS = {field for _, field in APR_COMPARISON_WINDOWS}
CHART_COLORS = ["#8be9fd", "#50fa7b", "#ffb86c", "#ff79c6", "#bd93f9", "#f1fa8c"]
EXCHANGE_OPTIONS = ["binance", "aster", "bitget", "coinbase", "okx", "bybit", "hyperliquid", "lighter", "extended", "variational"]
MILLION_USD = 1_000_000
PAGE_SIZE = 1000
DEFAULT_REFRESH_SECONDS = 300
ZERO_DECIMAL = Decimal("0")
DEFAULT_EXCHANGES = ["okx", "binance"]
DEFAULT_ASSET_TYPE_FILTERS = [ASSET_TYPE_STOCK]
HOME_EXCHANGES_KEY = "rwa_home_exchanges"
HOME_ASSET_TYPES_KEY = "rwa_home_asset_types"
HOME_SYMBOLS_KEY = "rwa_symbol_filter"
HOME_HAS_STOCK_SPOT_KEY = "rwa_home_has_stock_spot"
COMPARE_EXCHANGES_KEY = "rwa_compare_exchanges_picker"
COMPARE_ASSET_TYPES_KEY = "rwa_compare_asset_types_picker"
COMPARE_SYMBOLS_KEY = "rwa_compare_symbol_filter"
EMPTY_QUERY_SELECTION = "__empty__"
TOP_TAB_STATE_KEY = "rwa_top_tab"
TOP_TAB_OPTIONS = {
    "home": ":material/dashboard: 首页",
    "compare": ":material/compare_arrows: APR比较",
    "rh_pools": ":material/water_drop: RH Pools",
}
RH_TABLE_NAMES = (
    "rh_pool_hourly_metrics",
    "rh_pool_window_rankings",
    "rh_rwa_assets",
    "rh_sync_checkpoints",
)
RH_DASHBOARD_VIEW = "rh_pool_dashboard"
RH_RANKINGS_TABLE = "rh_pool_window_rankings"
RH_PUBLIC_TABLE_NAMES = (RH_DASHBOARD_VIEW, RH_RANKINGS_TABLE)
RH_TOKEN_UNIVERSE_KEY = "__rh_token_universe__"
RH_WINDOW_OPTIONS = ("2h", "4h", "24h")
RH_WINDOW_LABELS = {"2h": "最近 2 小时", "4h": "最近 4 小时", "24h": "最近 24 小时"}
RH_CHAIN_ID = "4663"
RH_WINDOW_KEY = "rh_pool_window"
RH_WINDOW_QUERY_KEY = "rh_window"
RH_TOKEN_KEY = "rh_pool_tokens"
RH_ADDRESS_KEY = "rh_pool_addresses"
RH_NEW_ISSUE_KEY = "rh_pool_new_issue"
RH_TABLE_ERRORS_KEY = "__errors__"


@dataclass(frozen=True, slots=True)
class DashboardFundingRow:
    exchange: str
    instrument: str
    canonical_symbol: str
    asset_type: str
    has_stock_spot: bool
    latest_apr_percent: Decimal
    next_funding_rate: Decimal | None
    next_funding_apr_percent: Decimal | None
    next_funding_time_iso: str
    apr_24h_percent: Decimal
    apr_7d_percent: Decimal
    apr_15d_percent: Decimal
    apr_30d_percent: Decimal
    open_interest_usd: Decimal
    volume_24h_usd: Decimal
    funding_points: int
    last_time_iso: str


@dataclass(frozen=True, slots=True)
class AprComparisonRow:
    canonical_symbol: str
    exchange_count: int
    max_exchange: str
    max_instrument: str
    max_apr_percent: Decimal
    min_exchange: str
    min_instrument: str
    min_apr_percent: Decimal
    apr_diff_percent: Decimal
    exchange_aprs: str


@dataclass(frozen=True, slots=True)
class RhPoolRow:
    pool_address: str
    token: str
    token_name: str
    pool_name: str
    tvl_usd: Decimal | None
    volume_24h_usd: Decimal | None
    is_new_issue: bool
    fee_income_2h_usd: Decimal | None
    fee_income_4h_usd: Decimal | None
    fee_income_24h_usd: Decimal | None
    fee_apr_percent: Decimal | None
    window_2h_percent: Decimal | None
    window_4h_percent: Decimal | None
    window_24h_percent: Decimal | None
    rank_2h: int | None
    rank_4h: int | None
    rank_24h: int | None
    last_metric_time_iso: str
    synced_at_iso: str


@dataclass(frozen=True, slots=True)
class DataConfig:
    url: str
    api_key: str
    dashboard_table: str = "rwa_dashboard_rows"
    timeout_seconds: int = 20

    @property
    def rest_url(self) -> str:
        return f"{self.url.rstrip('/')}/rest/v1"


class DataApiError(RuntimeError):
    pass


def secret_value(key: str, default: str = "") -> str:
    env_value = os.getenv(key, "").strip()
    if env_value:
        return env_value
    try:
        value = st.secrets.get(key, default)
    except (FileNotFoundError, KeyError, AttributeError):
        return default
    return str(value).strip()


def normalize_data_url(url: str) -> str:
    parsed = urlparse(url.strip())
    normalized_path = parsed.path.rstrip("/")
    if normalized_path.endswith("/rest/v1"):
        normalized_path = normalized_path.removesuffix("/rest/v1")
    return urlunparse(parsed._replace(path=normalized_path, params="", query="", fragment="")).rstrip("/")


def load_config() -> DataConfig | None:
    url = secret_value("SUPABASE_URL")
    api_key = secret_value("SUPABASE_PUBLISHABLE_KEY") or secret_value("SUPABASE_ANON_KEY")
    if not url or not api_key:
        return None
    timeout_raw = secret_value("SUPABASE_TIMEOUT_SECONDS", "20")
    try:
        timeout_seconds = int(timeout_raw)
    except ValueError:
        timeout_seconds = 20
    return DataConfig(
        url=normalize_data_url(url),
        api_key=api_key,
        dashboard_table=secret_value("SUPABASE_RWA_DASHBOARD_TABLE", "rwa_dashboard_rows"),
        timeout_seconds=timeout_seconds,
    )


def to_decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return ZERO_DECIMAL


def to_optional_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    return to_decimal(value)


def to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "y", "t"}
    return bool(value)


def first_value(row: dict[str, Any], *keys: str) -> Any:
    lowered = {str(key).lower(): value for key, value in row.items()}
    for key in keys:
        value = row.get(key, lowered.get(key.lower()))
        if value not in (None, ""):
            return value
    return None


def text_value(row: dict[str, Any], *keys: str, default: str = "") -> str:
    value = first_value(row, *keys)
    if value in (None, ""):
        return default
    if isinstance(value, (list, tuple, set)):
        return ", ".join(str(item).strip() for item in value if str(item).strip())
    return str(value).strip()


def normalize_window(value: Any) -> str | None:
    if value in (None, ""):
        return None
    text = str(value).strip().lower().replace("hours", "h").replace("hour", "h")
    digits = "".join(character for character in text if character.isdigit())
    return f"{digits}h" if digits in {"2", "4", "24"} else None


def parse_timestamp(value: Any) -> pd.Timestamp | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        unit = "ms" if numeric > 10_000_000_000 else "s"
        parsed = pd.to_datetime(numeric, unit=unit, utc=True, errors="coerce")
    else:
        parsed = pd.to_datetime(value, utc=True, errors="coerce")
    return None if pd.isna(parsed) else parsed


def row_from_payload(row: dict[str, Any]) -> DashboardFundingRow:
    return DashboardFundingRow(
        exchange=str(row.get("exchange", "")),
        instrument=str(row.get("instrument", "")),
        canonical_symbol=str(row.get("canonical_symbol", "")),
        asset_type=str(row.get("asset_type", "")),
        has_stock_spot=to_bool(row.get("has_stock_spot", False)),
        latest_apr_percent=to_decimal(row.get("latest_apr_percent")),
        next_funding_rate=to_optional_decimal(row.get("next_funding_rate")),
        next_funding_apr_percent=to_optional_decimal(row.get("next_funding_apr_percent")),
        next_funding_time_iso=str(row.get("next_funding_time_utc") or ""),
        apr_24h_percent=to_decimal(row.get("apr_24h_percent")),
        apr_7d_percent=to_decimal(row.get("apr_7d_percent")),
        apr_15d_percent=to_decimal(row.get("apr_15d_percent")),
        apr_30d_percent=to_decimal(row.get("apr_30d_percent")),
        open_interest_usd=to_decimal(row.get("open_interest_usd")),
        volume_24h_usd=to_decimal(row.get("volume_24h_usd")),
        funding_points=int(row.get("funding_points") or 0),
        last_time_iso=str(row.get("last_time_utc") or ""),
    )


def api_headers(config: DataConfig) -> dict[str, str]:
    return {
        "apikey": config.api_key,
        "Authorization": f"Bearer {config.api_key}",
        "Content-Type": "application/json",
    }


def raise_for_response(response: requests.Response) -> None:
    if response.status_code < 400:
        return
    raise DataApiError(f"status={response.status_code}")


def fetch_dashboard_rows(
    config: DataConfig,
    exchanges: Iterable[str],
    asset_types: Iterable[str],
) -> list[DashboardFundingRow]:
    selected_exchanges = [item for item in exchanges if item]
    selected_types = [item for item in asset_types if item and item != ASSET_TYPE_ALL]
    params: dict[str, str] = {
        "select": "*",
        "order": "sort_apr.desc,exchange.asc,instrument.asc",
    }
    if selected_exchanges:
        params["exchange"] = f"in.({','.join(selected_exchanges)})"
    if selected_types:
        params["asset_type"] = f"in.({','.join(selected_types)})"

    rows: list[DashboardFundingRow] = []
    offset = 0
    while True:
        page_params = {**params, "limit": str(PAGE_SIZE), "offset": str(offset)}
        response = requests.get(
            f"{config.rest_url}/{config.dashboard_table}",
            headers=api_headers(config),
            params=page_params,
            timeout=config.timeout_seconds,
        )
        raise_for_response(response)
        payload = response.json()
        if not isinstance(payload, list):
            return rows
        rows.extend(row_from_payload(item) for item in payload if isinstance(item, dict))
        if len(payload) < PAGE_SIZE:
            return rows
        offset += PAGE_SIZE


def get_cached_rows(
    exchanges: list[str],
    asset_types: list[str],
    refresh_seconds: int,
) -> tuple[list[DashboardFundingRow], float]:
    config = load_config()
    if config is None:
        raise DataApiError("missing_config")
    cache_key = (tuple(exchanges), tuple(asset_types), config.url, config.dashboard_table)
    cache = st.session_state.get("rwa_rows_cache")
    now_ts = time.time()
    if (
        isinstance(cache, dict)
        and cache.get("key") == cache_key
        and refresh_seconds > 0
        and now_ts - float(cache.get("loaded_at", 0.0)) < refresh_seconds
    ):
        return cache["rows"], float(cache["loaded_at"])
    rows = fetch_dashboard_rows(config, exchanges, asset_types)
    loaded_at = time.time()
    st.session_state["rwa_rows_cache"] = {
        "key": cache_key,
        "rows": rows,
        "loaded_at": loaded_at,
    }
    return rows, loaded_at


def fetch_table_rows(
    config: DataConfig,
    table_name: str,
    filters: dict[str, str] | None = None,
    select: str = "*",
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0
    while True:
        params = {"select": select, "limit": str(PAGE_SIZE), "offset": str(offset)}
        params.update(filters or {})
        response = requests.get(
            f"{config.rest_url}/{table_name}",
            headers=api_headers(config),
            params=params,
            timeout=config.timeout_seconds,
        )
        raise_for_response(response)
        payload = response.json()
        if not isinstance(payload, list):
            return rows
        rows.extend(item for item in payload if isinstance(item, dict))
        if len(payload) < PAGE_SIZE:
            return rows
        offset += PAGE_SIZE


def fetch_rh_tables(config: DataConfig, new_issue_only: bool = False) -> dict[str, Any]:
    tables: dict[str, Any] = {}
    errors: dict[str, str] = {}
    asset_scope = "all_active" if new_issue_only else "latest20"
    dashboard_select = "token,pool_address,pool,tvl_usd,volume_24h_usd,fee_apr,current_apr,apr_2h,rank_2h,rank_24h,is_new_issue,new_issue_discovered_at,metric_time,sync_time,swap_count_2h,swap_count_24h,fee_income_2h_usd,fee_income_24h_usd,yield_2h_percent,yield_24h_percent,data_quality_2h,data_quality_24h,chain_id,asset_scope"
    rankings_select = "pool_id,pool_address,window_hours,pool_pair,token0_symbol,token1_symbol,rwa_symbols,fee_income_usd,pool_size_usd_proxy,annualized_yield_percent,window_yield_percent,window_start,window_end,data_quality,computed_at,chain_id,asset_scope,is_public"
    dashboard_filters = {"chain_id": f"eq.{RH_CHAIN_ID}", "asset_scope": f"eq.{asset_scope}", "order": "rank_24h.asc.nullslast"}
    if new_issue_only:
        dashboard_filters["is_new_issue"] = "eq.true"
    table_specs = (
        (RH_DASHBOARD_VIEW, RH_DASHBOARD_VIEW, dashboard_select, dashboard_filters),
        (RH_RANKINGS_TABLE, RH_RANKINGS_TABLE, rankings_select, {"chain_id": f"eq.{RH_CHAIN_ID}", "asset_scope": f"eq.{asset_scope}", "is_public": "eq.true", "window_hours": "eq.4", "order": "annualized_yield_percent.desc.nullslast"}),
        (RH_TOKEN_UNIVERSE_KEY, RH_DASHBOARD_VIEW, "token", {"chain_id": f"eq.{RH_CHAIN_ID}", "asset_scope": "eq.all_active"}),
    )
    for table_key, table_name, select, filters in table_specs:
        try:
            tables[table_key] = fetch_table_rows(config, table_name, filters, select)
        except (requests.RequestException, DataApiError, ValueError) as exc:
            tables[table_key] = []
            errors[table_key] = type(exc).__name__
    tables[RH_TABLE_ERRORS_KEY] = errors
    return tables


def get_cached_rh_tables(config: DataConfig, refresh_seconds: int, new_issue_only: bool = False) -> dict[str, Any]:
    cache_key = (config.url, config.api_key, RH_PUBLIC_TABLE_NAMES, new_issue_only)
    cache = st.session_state.get("rh_tables_cache")
    now_ts = time.time()
    if (
        isinstance(cache, dict)
        and cache.get("key") == cache_key
        and refresh_seconds > 0
        and now_ts - float(cache.get("loaded_at", 0.0)) < refresh_seconds
    ):
        return cache["tables"]
    tables = fetch_rh_tables(config, new_issue_only)
    st.session_state["rh_tables_cache"] = {"key": cache_key, "tables": tables, "loaded_at": time.time()}
    return tables


def pool_address_from(row: dict[str, Any]) -> str:
    return text_value(
        row,
        "pool_address",
        "pool_addr",
        "pool",
        "address",
        "amm_pool_address",
        "pool_id",
    )


def pool_name_from(row: dict[str, Any], address: str) -> str:
    pair = text_value(row, "pool", "pool_pair", "pool_name", "pair", "token_pair")
    if pair:
        return pair
    token0 = text_value(row, "token0_symbol", "token0")
    token1 = text_value(row, "token1_symbol", "token1")
    if token0 and token1:
        return f"{token0}/{token1}"
    return address[:10] + "…"


def is_rh_chain_row(row: dict[str, Any]) -> bool:
    chain_value = first_value(row, "chain_id", "chain", "network", "chain_name")
    if chain_value in (None, ""):
        return True
    return str(chain_value).strip().lower() in {RH_CHAIN_ID, "rh", "rhodium"}


def asset_lookup_from(rows: list[dict[str, Any]]) -> dict[str, tuple[str, str]]:
    lookup: dict[str, tuple[str, str]] = {}
    for row in rows:
        token = text_value(row, "token_symbol", "symbol", "asset_symbol", "ticker", "token")
        token_name = text_value(row, "token_name", "asset_name", "name", default=token)
        identifiers = (
            text_value(row, "id", "asset_id", "rwa_asset_id", "token_id"),
            text_value(row, "address", "token_address", "contract_address"),
            token,
        )
        if not token:
            continue
        for identifier in identifiers:
            if identifier:
                lookup[identifier.lower()] = (token, token_name)
    return lookup


def token_from(row: dict[str, Any], assets: dict[str, tuple[str, str]]) -> tuple[str, str]:
    direct_token = text_value(
        row,
        "rwa_symbols",
        "rwa_symbol",
        "token_symbol",
        "token0_symbol",
        "symbol",
        "asset_symbol",
        "ticker",
        "token",
        "canonical_symbol",
    )
    direct_name = text_value(row, "token_name", "asset_name", "name", default=direct_token)
    if direct_token:
        return direct_token, direct_name
    asset_ref = text_value(row, "asset_id", "rwa_asset_id", "token_id", "asset_address", "token_address")
    return assets.get(asset_ref.lower(), ("未标注", "未标注"))


def metric_value(row: dict[str, Any], metric: str, window: str | None = None) -> Decimal | None:
    keys: list[str] = []
    if window:
        hours = window.removesuffix("h")
        keys.extend(
            [
                f"{metric}_{window}",
                f"{metric}_{hours}h",
                f"{window}_{metric}",
                f"{hours}h_{metric}",
                f"window_{window}_{metric}",
                f"window_{hours}h_{metric}",
            ]
        )
    keys.extend(
        {
            "tvl": ("tvl_usd", "tvl", "liquidity_usd", "pool_tvl_usd", "pool_size_usd_proxy", "total_value_locked"),
            "fee_income": ("fee_income_usd",),
            "apr": ("annualized_yield_percent", "window_yield_percent", "fee_apr_percent", "fee_apr", "apr_percent", "apr", "apy_percent", "apy", "yield_percent"),
        }.get(metric, (metric,))
    )
    value = first_value(row, *keys)
    return None if value in (None, "") else to_decimal(value)


def rank_value(row: dict[str, Any], window: str) -> int | None:
    value = first_value(row, "rank", "pool_rank", "ranking", f"rank_{window}", f"rank_{window.removesuffix('h')}h")
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def build_rh_pool_rows(tables: dict[str, Any]) -> list[RhPoolRow]:
    assets = asset_lookup_from(tables.get("rh_rwa_assets", []))
    records: dict[str, dict[str, Any]] = {}

    def record_for(row: dict[str, Any]) -> dict[str, Any] | None:
        address = pool_address_from(row)
        if not address:
            return None
        key = address.lower()
        token, token_name = token_from(row, assets)
        record = records.setdefault(
            key,
            {
                "pool_address": address,
                "token": token,
                "token_name": token_name,
                "pool_name": pool_name_from(row, address),
                "tvl_usd": None,
                "volume_24h_usd": None,
                "is_new_issue": False,
                "window_fee_income": {window: None for window in RH_WINDOW_OPTIONS},
                "fee_apr_percent": None,
                "windows": {window: None for window in RH_WINDOW_OPTIONS},
                "ranks": {window: None for window in RH_WINDOW_OPTIONS},
                "last_metric_time": None,
                "synced_at_iso": "",
            },
        )
        if token != "未标注":
            record["token"], record["token_name"] = token, token_name
        if record["pool_name"] == address[:10] + "…":
            record["pool_name"] = pool_name_from(row, address)
        return record

    for row in tables.get(RH_DASHBOARD_VIEW, []):
        if not is_rh_chain_row(row):
            continue
        record = record_for(row)
        if record is None:
            continue
        tvl = metric_value(row, "tvl")
        if tvl is not None and record["tvl_usd"] is None:
            record["tvl_usd"] = tvl
        volume = to_optional_decimal(first_value(row, "volume_24h_usd"))
        if volume is not None:
            record["volume_24h_usd"] = volume
        current_apr = to_optional_decimal(first_value(row, "current_apr", "fee_apr"))
        fee_apr = to_optional_decimal(first_value(row, "fee_apr"))
        if current_apr is not None:
            record["fee_apr_percent"] = current_apr
        elif fee_apr is not None:
            record["fee_apr_percent"] = fee_apr
        record["windows"]["2h"] = to_optional_decimal(first_value(row, "apr_2h", "current_apr"))
        record["windows"]["24h"] = fee_apr
        record["window_fee_income"]["2h"] = to_optional_decimal(first_value(row, "fee_income_2h_usd"))
        record["window_fee_income"]["24h"] = to_optional_decimal(first_value(row, "fee_income_24h_usd"))
        record["ranks"]["2h"] = rank_value(row, "2h")
        record["ranks"]["24h"] = rank_value(row, "24h")
        record["is_new_issue"] = to_bool(first_value(row, "is_new_issue"))
        metric_time = parse_timestamp(first_value(row, "metric_time", "computed_at"))
        if metric_time is not None:
            record["last_metric_time"] = metric_time
        sync_time = parse_timestamp(first_value(row, "sync_time"))
        if sync_time is not None:
            record["synced_at_iso"] = sync_time.isoformat()

    for row in tables.get("rh_pool_hourly_metrics", []):
        if not is_rh_chain_row(row):
            continue
        record = record_for(row)
        if record is None:
            continue
        tvl = metric_value(row, "tvl")
        fee_income = metric_value(row, "fee_income")
        apr = metric_value(row, "apr")
        if tvl is not None and record["tvl_usd"] is None:
            record["tvl_usd"] = tvl
        if fee_income is not None:
            record["window_fee_income"]["24h"] = fee_income
        if apr is not None:
            record["fee_apr_percent"] = apr
        metric_time = parse_timestamp(first_value(row, "metric_time", "bucket_start", "timestamp", "hour", "hour_start", "recorded_at", "created_at"))
        if metric_time is not None and (record["last_metric_time"] is None or metric_time > record["last_metric_time"]):
            record["last_metric_time"] = metric_time
        for window in RH_WINDOW_OPTIONS:
            window_metric = metric_value(row, "apr", window)
            if window_metric is not None:
                record["windows"][window] = window_metric

    dashboard_view_present = RH_DASHBOARD_VIEW in tables
    dashboard_pool_addresses = {
        pool_address_from(row).lower()
        for row in tables.get(RH_DASHBOARD_VIEW, [])
        if pool_address_from(row)
    }
    for row in tables.get("rh_pool_window_rankings", []):
        if not is_rh_chain_row(row):
            continue
        if dashboard_view_present and pool_address_from(row).lower() not in dashboard_pool_addresses:
            continue
        record = record_for(row)
        if record is None:
            continue
        window = normalize_window(first_value(row, "window_hours", "window", "hours", "period", "time_window"))
        if window is None:
            for candidate in RH_WINDOW_OPTIONS:
                candidate_metric = metric_value(row, "apr", candidate)
                if candidate_metric is not None:
                    record["windows"][candidate] = candidate_metric
                candidate_rank = rank_value(row, candidate)
                if candidate_rank is not None:
                    record["ranks"][candidate] = candidate_rank
        else:
            window_metric = metric_value(row, "apr") or metric_value(row, "apr", window)
            if window_metric is not None:
                record["windows"][window] = window_metric
            window_fee_income = metric_value(row, "fee_income")
            if window_fee_income is not None:
                record["window_fee_income"][window] = window_fee_income
            record["ranks"][window] = rank_value(row, window) or rank_value(row, "24h")
            ranking_time = parse_timestamp(first_value(row, "computed_at"))
            if ranking_time is not None and record["last_metric_time"] is None:
                record["last_metric_time"] = ranking_time

    checkpoint_rows = tables.get("rh_sync_checkpoints", [])
    checkpoint_times = [
        parse_timestamp(first_value(row, "synced_at", "last_synced_at", "checkpoint_time", "updated_at", "created_at"))
        for row in checkpoint_rows
    ]
    checkpoint_times = [value for value in checkpoint_times if value is not None]
    synced_at_iso = max(checkpoint_times).isoformat() if checkpoint_times else ""
    if synced_at_iso:
        for record in records.values():
            record["synced_at_iso"] = synced_at_iso

    result = [
        RhPoolRow(
            pool_address=record["pool_address"],
            token=record["token"],
            token_name=record["token_name"],
            pool_name=record["pool_name"],
            tvl_usd=record["tvl_usd"],
            volume_24h_usd=record["volume_24h_usd"],
            is_new_issue=record["is_new_issue"],
            fee_income_2h_usd=record["window_fee_income"]["2h"],
            fee_income_4h_usd=record["window_fee_income"]["4h"],
            fee_income_24h_usd=record["window_fee_income"]["24h"],
            fee_apr_percent=record["fee_apr_percent"],
            window_2h_percent=record["windows"]["2h"],
            window_4h_percent=record["windows"]["4h"],
            window_24h_percent=record["windows"]["24h"],
            rank_2h=record["ranks"]["2h"],
            rank_4h=record["ranks"]["4h"],
            rank_24h=record["ranks"]["24h"],
            last_metric_time_iso=record["last_metric_time"].isoformat() if record["last_metric_time"] is not None else "",
            synced_at_iso=record["synced_at_iso"],
        )
        for record in records.values()
    ]
    for window in RH_WINDOW_OPTIONS:
        ranked = sorted(
            (row for row in result if getattr(row, f"window_{window}_percent") is not None),
            key=lambda row: getattr(row, f"window_{window}_percent"),
            reverse=True,
        )
        computed_ranks = {row.pool_address.lower(): index for index, row in enumerate(ranked, 1)}
        result = [
            row if getattr(row, f"rank_{window}") is not None else RhPoolRow(
                pool_address=row.pool_address,
                token=row.token,
                token_name=row.token_name,
                pool_name=row.pool_name,
                tvl_usd=row.tvl_usd,
                volume_24h_usd=row.volume_24h_usd,
                is_new_issue=row.is_new_issue,
                fee_income_2h_usd=row.fee_income_2h_usd,
                fee_income_4h_usd=row.fee_income_4h_usd,
                fee_income_24h_usd=row.fee_income_24h_usd,
                fee_apr_percent=row.fee_apr_percent,
                window_2h_percent=row.window_2h_percent,
                window_4h_percent=row.window_4h_percent,
                window_24h_percent=row.window_24h_percent,
                rank_2h=computed_ranks.get(row.pool_address.lower()) if window == "2h" else row.rank_2h,
                rank_4h=computed_ranks.get(row.pool_address.lower()) if window == "4h" else row.rank_4h,
                rank_24h=computed_ranks.get(row.pool_address.lower()) if window == "24h" else row.rank_24h,
                last_metric_time_iso=row.last_metric_time_iso,
                synced_at_iso=row.synced_at_iso,
            )
            for row in result
        ]
    return sorted(result, key=lambda item: (item.token, item.pool_address))


def demo_rh_pool_rows() -> list[RhPoolRow]:
    now_iso = datetime.now(timezone.utc).isoformat()
    samples = [
        ("USYC", "USYC / USDC", "0x7a2f…91c4", 18_420_000, 3_680_000, 8.42, 5.18, 7.36, 8.42),
        ("OUSG", "OUSG / USDC", "0x2d8b…f0a1", 11_760_000, 2_140_000, 6.85, 4.12, 5.64, 6.85),
        ("USDY", "USDY / USDC", "0xb910…2e77", 8_960_000, 1_760_000, 7.63, 5.74, 6.92, 7.63),
        ("TBILL", "TBILL / USDC", "0x4c61…d8aa", 6_310_000, 920_000, 5.92, 3.88, 5.21, 5.92),
        ("mTBILL", "mTBILL / USDC", "0xa83e…c512", 4_280_000, 610_000, 4.76, 3.42, 4.18, 4.76),
        ("BUIDL", "BUIDL / USDC", "0xf12a…74be", 3_740_000, 480_000, 4.31, 2.97, 3.82, 4.31),
    ]
    return [
        RhPoolRow(
            pool_address=address,
            token=token,
            token_name=token,
            pool_name=pool_name,
            tvl_usd=Decimal(str(tvl)),
            volume_24h_usd=Decimal(str(volume)),
            is_new_issue=index in {1, 3},
            fee_income_2h_usd=Decimal(str(volume)),
            fee_income_4h_usd=Decimal(str(volume)),
            fee_income_24h_usd=Decimal(str(volume)),
            fee_apr_percent=Decimal(str(fee_apr)),
            window_2h_percent=Decimal(str(apr_2h)),
            window_4h_percent=Decimal(str(apr_4h)),
            window_24h_percent=Decimal(str(apr_24h)),
            rank_2h=index,
            rank_4h=index,
            rank_24h=index,
            last_metric_time_iso=now_iso,
            synced_at_iso=now_iso,
        )
        for index, (token, pool_name, address, tvl, volume, fee_apr, apr_2h, apr_4h, apr_24h) in enumerate(samples, 1)
    ]


def get_apr_value(row: DashboardFundingRow, apr_field: str) -> Decimal:
    if apr_field not in APR_COMPARISON_FIELDS:
        raise ValueError(f"unsupported APR comparison field: {apr_field}")
    value = getattr(row, apr_field)
    return value if isinstance(value, Decimal) else to_decimal(value)


def prefer_comparison_row(current: DashboardFundingRow, candidate: DashboardFundingRow) -> DashboardFundingRow:
    if candidate.funding_points != current.funding_points:
        return candidate if candidate.funding_points > current.funding_points else current
    if candidate.last_time_iso != current.last_time_iso:
        return candidate if candidate.last_time_iso > current.last_time_iso else current
    return candidate if candidate.instrument < current.instrument else current


def build_apr_comparison_rows(rows: list[DashboardFundingRow], apr_field: str) -> list[AprComparisonRow]:
    rows_by_symbol: dict[str, dict[str, DashboardFundingRow]] = {}
    for row in rows:
        if not row.canonical_symbol:
            continue
        exchange_rows = rows_by_symbol.setdefault(row.canonical_symbol, {})
        current = exchange_rows.get(row.exchange)
        exchange_rows[row.exchange] = row if current is None else prefer_comparison_row(current, row)

    comparison_rows: list[AprComparisonRow] = []
    for canonical_symbol, exchange_rows in rows_by_symbol.items():
        if len(exchange_rows) < 2:
            continue
        ordered_rows = sorted(
            exchange_rows.values(),
            key=lambda item: (get_apr_value(item, apr_field), item.exchange, item.instrument),
            reverse=True,
        )
        max_row = ordered_rows[0]
        min_row = ordered_rows[-1]
        max_apr = get_apr_value(max_row, apr_field)
        min_apr = get_apr_value(min_row, apr_field)
        exchange_aprs = " | ".join(
            f"{row.exchange}:{row.instrument}={get_apr_value(row, apr_field):.2f}%"
            for row in ordered_rows
        )
        comparison_rows.append(
            AprComparisonRow(
                canonical_symbol=canonical_symbol,
                exchange_count=len(exchange_rows),
                max_exchange=max_row.exchange,
                max_instrument=max_row.instrument,
                max_apr_percent=max_apr,
                min_exchange=min_row.exchange,
                min_instrument=min_row.instrument,
                min_apr_percent=min_apr,
                apr_diff_percent=max_apr - min_apr,
                exchange_aprs=exchange_aprs,
            )
        )
    comparison_rows.sort(key=lambda item: (-item.apr_diff_percent, item.canonical_symbol))
    return comparison_rows


def format_apr(value: float) -> str:
    return f"{value:.2f}%"


def format_musd(value: float) -> str:
    return f"{value:,.2f}M"


def inject_style() -> None:
    st.markdown(
        """
        <style>
        :root {
            --rwa-bg-0: #0f111a;
            --rwa-bg-1: #171923;
            --rwa-bg-2: #222533;
            --rwa-border: rgba(139, 233, 253, 0.18);
            --rwa-cyan: #8be9fd;
            --rwa-green: #50fa7b;
            --rwa-pink: #ff79c6;
            --rwa-orange: #ffb86c;
            --rwa-purple: #bd93f9;
            --rwa-text-soft: #aeb7d6;
            --rwa-font-cjk: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC",
                "Microsoft YaHei", "Noto Sans CJK SC", "Hiragino Sans GB", Arial, sans-serif;
        }
        .stApp {
            font-family: var(--rwa-font-cjk);
            background:
                radial-gradient(circle at 12% 0%, rgba(139, 233, 253, 0.14), transparent 32rem),
                radial-gradient(circle at 86% 4%, rgba(189, 147, 249, 0.16), transparent 30rem),
                linear-gradient(135deg, #0f111a 0%, #171923 48%, #10121a 100%);
        }
        .stApp :where(
            [data-testid="stMarkdownContainer"],
            [data-testid="stWidgetLabel"],
            [data-testid="stMetric"],
            [data-testid="stDataFrame"],
            [data-baseweb="select"],
            label,
            button,
            input
        ) {
            font-family: var(--rwa-font-cjk) !important;
        }
        .block-container { padding-top: 1.4rem; padding-bottom: 3rem; }
        header[data-testid="stHeader"], div[data-testid="stToolbar"], div[data-testid="stDecoration"],
        div[data-testid="stStatusWidget"], .stDeployButton, #MainMenu, footer {
            display: none !important; visibility: hidden !important;
        }
        .rwa-hero {
            position: relative; overflow: hidden; padding: 1.35rem 1.5rem;
            border: 1px solid var(--rwa-border); border-radius: 24px;
            background: linear-gradient(135deg, rgba(34, 37, 51, 0.92), rgba(15, 17, 26, 0.78)),
                        linear-gradient(90deg, rgba(139, 233, 253, 0.08), rgba(255, 121, 198, 0.08));
            box-shadow: 0 24px 70px rgba(0, 0, 0, 0.34); margin-bottom: 1rem;
        }
        .rwa-hero:after {
            content: ""; position: absolute; inset: -45%;
            background: radial-gradient(circle, rgba(139, 233, 253, 0.18), transparent 34%);
            animation: rwa-breathe 6s ease-in-out infinite; pointer-events: none;
        }
        @keyframes rwa-breathe {
            0%, 100% { transform: translate3d(-8%, -4%, 0) scale(0.92); opacity: 0.28; }
            50% { transform: translate3d(7%, 6%, 0) scale(1.08); opacity: 0.56; }
        }
        .rwa-hero-content { position: relative; z-index: 1; }
        .rwa-eyebrow { color: var(--rwa-cyan); font-size: 0.78rem; letter-spacing: 0.16em; text-transform: uppercase; font-weight: 700; margin-bottom: 0.25rem; }
        .rwa-title { color: #f8f8f2; font-size: clamp(2rem, 4vw, 3.5rem); line-height: 0.95; font-weight: 800; margin: 0; }
        .rwa-subtitle { color: var(--rwa-text-soft); max-width: 920px; font-size: 1rem; margin-top: 0.85rem; }
        .rwa-status { border: 1px solid rgba(80, 250, 123, 0.18); border-radius: 18px; padding: 0.65rem 0.9rem; background: rgba(22, 25, 36, 0.78); color: var(--rwa-text-soft); font-size: 0.86rem; margin: 0.6rem 0 1rem; }
        .rwa-status strong { color: #f8f8f2; }
        div[data-testid="stMetric"] { background: linear-gradient(180deg, rgba(34, 37, 51, 0.92), rgba(22, 25, 36, 0.92)); border-color: rgba(139, 233, 253, 0.18) !important; border-radius: 18px !important; box-shadow: 0 14px 40px rgba(0,0,0,0.22); transition: transform 180ms ease, border-color 180ms ease, box-shadow 180ms ease; }
        div[data-testid="stMetric"]:hover { transform: translateY(-3px); border-color: rgba(139, 233, 253, 0.42) !important; box-shadow: 0 18px 54px rgba(139, 233, 253, 0.12); }
        div[data-testid="stVerticalBlockBorderWrapper"] { border-color: rgba(139, 233, 253, 0.16) !important; border-radius: 20px !important; background: rgba(22, 25, 36, 0.58); box-shadow: 0 14px 44px rgba(0,0,0,0.18); }
        div[data-testid="stDataFrame"] { border-radius: 16px; overflow: hidden; }
        div[data-testid="stTabs"] button { border-radius: 999px; }
        div[data-baseweb="select"] > div { background: rgba(34, 37, 51, 0.86); border-color: rgba(139, 233, 253, 0.18); }
        .rwa-chip-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 0.75rem; margin: 0.35rem 0 1rem; }
        .rwa-chip { border: 1px solid rgba(139, 233, 253, 0.16); border-radius: 16px; padding: 0.85rem 0.95rem; background: linear-gradient(180deg, rgba(34, 37, 51, 0.78), rgba(15, 17, 26, 0.62)); box-shadow: inset 0 1px 0 rgba(248, 248, 242, 0.04); transition: transform 180ms ease, border-color 180ms ease, background 180ms ease; }
        .rwa-chip:hover { transform: translateY(-2px); border-color: rgba(255, 184, 108, 0.36); background: linear-gradient(180deg, rgba(43, 46, 63, 0.92), rgba(18, 20, 30, 0.76)); }
        .rwa-chip-label { color: var(--rwa-text-soft); font-size: 0.74rem; letter-spacing: 0.08em; text-transform: uppercase; margin-bottom: 0.2rem; }
        .rwa-chip-value { color: #f8f8f2; font-size: 1.18rem; font-weight: 800; }
        @media (max-width: 900px) { .rwa-chip-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_hero(title: str, subtitle: str) -> None:
    st.markdown(
        f"""
        <div class="rwa-hero">
            <div class="rwa-hero-content">
                <div class="rwa-eyebrow">US equity funding terminal</div>
                <h1 class="rwa-title">{escape(title)}</h1>
                <div class="rwa-subtitle">{escape(subtitle)}</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def as_table_rows(rows: list[DashboardFundingRow]) -> list[dict[str, object]]:
    return [
        {
            "exchange": row.exchange,
            "symbol": row.instrument,
            "canonical_symbol": row.canonical_symbol,
            "asset_type": row.asset_type,
            "has_stock_spot": row.has_stock_spot,
            "latest_apr": float(row.latest_apr_percent),
            "next_apr": float(row.next_funding_apr_percent) if row.next_funding_apr_percent is not None else None,
            "open_interest_musd": float(row.open_interest_usd) / MILLION_USD,
            "volume_24h_musd": float(row.volume_24h_usd) / MILLION_USD,
            "sort_apr": float(row.latest_apr_percent) if row.funding_points > 0 or row.next_funding_apr_percent is None else float(row.next_funding_apr_percent),
            "apr_24h": float(row.apr_24h_percent),
            "apr_7d": float(row.apr_7d_percent),
            "apr_15d": float(row.apr_15d_percent),
            "apr_30d": float(row.apr_30d_percent),
            "funding_points": row.funding_points,
            "next_funding_time_utc": row.next_funding_time_iso,
            "last_time_utc": row.last_time_iso,
        }
        for row in rows
    ]


def as_comparison_table_rows(rows: list[AprComparisonRow]) -> list[dict[str, object]]:
    return [
        {
            "canonical_symbol": row.canonical_symbol,
            "exchange_count": row.exchange_count,
            "apr_diff": float(row.apr_diff_percent),
            "max_exchange": row.max_exchange,
            "max_symbol": row.max_instrument,
            "max_apr": float(row.max_apr_percent),
            "min_exchange": row.min_exchange,
            "min_symbol": row.min_instrument,
            "min_apr": float(row.min_apr_percent),
            "exchange_aprs": row.exchange_aprs,
        }
        for row in rows
    ]


def build_apr_bar_chart(frame: pd.DataFrame):
    chart_frame = frame.nlargest(min(14, len(frame)), "sort_apr")[["exchange", "symbol", "canonical_symbol", "latest_apr", "next_apr", "apr_24h"]].copy()
    chart_frame["market"] = chart_frame["exchange"] + ":" + chart_frame["canonical_symbol"]
    melted = chart_frame.melt(
        id_vars=["market", "symbol"],
        value_vars=["latest_apr", "next_apr", "apr_24h"],
        var_name="APR type",
        value_name="APR %",
    ).dropna()
    fig = px.bar(
        melted,
        x="APR %",
        y="market",
        color="APR type",
        orientation="h",
        barmode="group",
        hover_data={"symbol": True, "APR %": ":.2f"},
        color_discrete_sequence=CHART_COLORS,
        title="Top APR surface",
    )
    fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", title_font_color="#f8f8f2", font_color="#d7defa", legend_title_text="", margin=dict(l=8, r=8, t=48, b=8), height=390, yaxis=dict(autorange="reversed"))
    fig.update_xaxes(tickformat=".2f")
    return fig


def build_liquidity_scatter(frame: pd.DataFrame):
    scatter_frame = frame.copy()
    scatter_frame["bubble_size"] = scatter_frame["open_interest_musd"].clip(lower=0.1)
    fig = px.scatter(
        scatter_frame,
        x="volume_24h_musd",
        y="latest_apr",
        size="bubble_size",
        color="exchange",
        hover_name="symbol",
        hover_data={"canonical_symbol": True, "asset_type": True, "open_interest_musd": ":.2f", "volume_24h_musd": ":.2f", "latest_apr": ":.2f", "bubble_size": False},
        color_discrete_sequence=CHART_COLORS,
        title="Liquidity vs latest APR",
    )
    fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", title_font_color="#f8f8f2", font_color="#d7defa", legend_title_text="", margin=dict(l=8, r=8, t=48, b=8), height=390, xaxis_title="24h volume (M USD)", yaxis_title="latest APR (%)")
    fig.update_yaxes(tickformat=".2f")
    fig.update_traces(marker=dict(line=dict(width=0.8, color="rgba(248,248,242,0.55)")))
    return fig


def build_exchange_oi_share_chart(frame: pd.DataFrame):
    grouped = frame.groupby("exchange", as_index=False)["open_interest_musd"].sum().sort_values("open_interest_musd", ascending=False)
    if grouped["open_interest_musd"].sum() <= 0:
        grouped["open_interest_musd"] = 1
    fig = px.pie(grouped, names="exchange", values="open_interest_musd", hole=0.58, color_discrete_sequence=CHART_COLORS, title="OI share by exchange")
    fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", title_font_color="#f8f8f2", font_color="#d7defa", legend_title_text="", margin=dict(l=8, r=8, t=48, b=8), height=320)
    fig.update_traces(textinfo="percent+label", hovertemplate="%{label}<br>OI=%{value:.2f}M USD<br>%{percent}<extra></extra>")
    return fig


def build_symbol_volume_chart(frame: pd.DataFrame):
    grouped = frame.groupby("canonical_symbol", as_index=False).agg(volume_24h_musd=("volume_24h_musd", "sum"), exchange_count=("exchange", "nunique"), asset_type=("asset_type", "first")).nlargest(16, "volume_24h_musd").sort_values("volume_24h_musd", ascending=True)
    fig = px.bar(grouped, x="volume_24h_musd", y="canonical_symbol", orientation="h", color="volume_24h_musd", hover_data={"asset_type": True, "exchange_count": True, "volume_24h_musd": ":.2f"}, color_continuous_scale=["#50fa7b", "#8be9fd", "#bd93f9", "#ff79c6"], title="24h volume by symbol")
    symbols = grouped["canonical_symbol"].tolist()
    fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", title_font_color="#f8f8f2", font_color="#d7defa", showlegend=False, coloraxis_showscale=False, margin=dict(l=8, r=8, t=48, b=8), height=max(320, 42 * len(symbols)), xaxis_title="24h volume (M USD)", yaxis_title="")
    fig.update_yaxes(tickmode="array", tickvals=symbols, ticktext=symbols)
    return fig


def build_comparison_chart(rows: list[AprComparisonRow], label: str):
    frame = pd.DataFrame(as_comparison_table_rows(rows)).head(16)
    fig = px.bar(frame, x="apr_diff", y="canonical_symbol", orientation="h", color="apr_diff", color_continuous_scale=["#50fa7b", "#8be9fd", "#bd93f9", "#ff79c6"], hover_data={"max_exchange": True, "min_exchange": True, "max_apr": ":.2f", "min_apr": ":.2f", "apr_diff": ":.2f"}, title=f"{label} spread leaders")
    symbols = frame["canonical_symbol"].tolist()
    fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", title_font_color="#f8f8f2", font_color="#d7defa", coloraxis_showscale=False, margin=dict(l=8, r=8, t=48, b=8), height=max(360, 42 * len(symbols)), xaxis_title="APR spread (%)", yaxis_title="", yaxis=dict(autorange="reversed"))
    fig.update_xaxes(tickformat=".2f")
    fig.update_yaxes(tickmode="array", tickvals=symbols, ticktext=symbols)
    return fig


def render_kpis(frame: pd.DataFrame) -> None:
    total_oi = float(frame["open_interest_musd"].sum())
    total_volume = float(frame["volume_24h_musd"].sum())
    top_latest = frame.sort_values("latest_apr", ascending=False).iloc[0]
    next_frame = frame.dropna(subset=["next_apr"])
    top_next = next_frame.sort_values("next_apr", ascending=False).iloc[0] if not next_frame.empty else top_latest
    spark = frame.sort_values("latest_apr", ascending=False)["latest_apr"].head(12).tolist()
    cols = st.columns(4, gap="medium")
    with cols[0]:
        st.metric("Max latest APR", format_apr(float(top_latest["latest_apr"])), f"{top_latest['exchange']}:{top_latest['canonical_symbol']}", border=True, chart_data=spark, chart_type="bar")
    with cols[1]:
        next_value = top_next["next_apr"]
        st.metric("Max next APR", format_apr(float(next_value)) if pd.notna(next_value) else "-", f"{top_next['exchange']}:{top_next['canonical_symbol']}", border=True)
    with cols[2]:
        st.metric("Total OI", format_musd(total_oi), "million USD", border=True)
    with cols[3]:
        st.metric("24h volume", format_musd(total_volume), "million USD", border=True)


def render_breadth_chips(frame: pd.DataFrame) -> None:
    avg_apr_24h = float(frame["apr_24h"].mean()) if not frame.empty else 0.0
    payload = [("Markets", f"{len(frame):,}"), ("Unified symbols", f"{frame['canonical_symbol'].nunique():,}"), ("Exchanges", f"{frame['exchange'].nunique():,}"), ("Avg 24h APR", format_apr(avg_apr_24h))]
    chips = "".join(f'<div class="rwa-chip"><div class="rwa-chip-label">{escape(label)}</div><div class="rwa-chip-value">{escape(value)}</div></div>' for label, value in payload)
    st.markdown(f'<div class="rwa-chip-grid">{chips}</div>', unsafe_allow_html=True)


def render_funding_table(frame: pd.DataFrame) -> None:
    with st.container(border=True):
        st.markdown("#### :material/table_chart: Funding surface")
        oi_min_col, oi_max_col, symbol_col, stock_spot_col, _ = st.columns([0.75, 0.75, 1.2, 0.65, 2.65])
        with oi_min_col:
            min_oi = st.number_input("最小 OI (M USD)", min_value=0.0, value=None, placeholder="不限制", key="rwa_table_min_oi")
        with oi_max_col:
            max_oi = st.number_input("最大 OI (M USD)", min_value=0.0, value=None, placeholder="不限制", key="rwa_table_max_oi")
        with symbol_col:
            symbol_options = sorted(frame["canonical_symbol"].dropna().unique().tolist())
            prepare_multiselect_state(HOME_SYMBOLS_KEY, "symbols", symbol_options)
            selected_symbols = st.multiselect("Symbol（多选）", options=symbol_options, placeholder="全部", key=HOME_SYMBOLS_KEY)
        with stock_spot_col:
            has_stock_spot = st.checkbox("币股", value=False, key=HOME_HAS_STOCK_SPOT_KEY, help="仅显示 Supabase 中 has_stock_spot=true 的数据")
        set_query_param_selection("symbols", selected_symbols)
        if min_oi is not None and max_oi is not None and min_oi > max_oi:
            st.warning("最小 OI 不能大于最大 OI。")
            return
        table_frame = frame
        if selected_symbols:
            table_frame = table_frame[table_frame["canonical_symbol"].isin(set(selected_symbols))]
        if has_stock_spot:
            table_frame = table_frame[table_frame["has_stock_spot"]]
        if min_oi is not None:
            table_frame = table_frame[table_frame["open_interest_musd"] >= min_oi]
        if max_oi is not None:
            table_frame = table_frame[table_frame["open_interest_musd"] <= max_oi]
        st.dataframe(
            table_frame,
            width="stretch",
            hide_index=True,
            column_order=["exchange", "symbol", "canonical_symbol", "asset_type", "open_interest_musd", "volume_24h_musd", "latest_apr", "next_apr", "apr_24h", "apr_7d", "apr_15d", "apr_30d", "funding_points", "next_funding_time_utc", "last_time_utc"],
            column_config={
                "exchange": st.column_config.TextColumn("交易所", pinned=True),
                "symbol": st.column_config.TextColumn("Symbol", pinned=True),
                "canonical_symbol": st.column_config.TextColumn("统一 Symbol"),
                "latest_apr": st.column_config.NumberColumn("latest APR", format="%.2f%%"),
                "next_apr": st.column_config.NumberColumn("next APR", format="%.2f%%"),
                "asset_type": st.column_config.TextColumn("类型"),
                "open_interest_musd": st.column_config.NumberColumn("OI (M USD)", format="%.2f"),
                "volume_24h_musd": st.column_config.NumberColumn("24h volume (M USD)", format="%.2f"),
                "apr_24h": st.column_config.NumberColumn("24h APR", format="%.2f%%"),
                "apr_7d": st.column_config.NumberColumn("7d APR", format="%.2f%%"),
                "apr_15d": st.column_config.NumberColumn("15d APR", format="%.2f%%"),
                "apr_30d": st.column_config.NumberColumn("30d APR", format="%.2f%%"),
                "funding_points": st.column_config.NumberColumn("funding points", format="%d"),
                "next_funding_time_utc": st.column_config.TextColumn("next funding time (UTC)"),
                "last_time_utc": st.column_config.TextColumn("latest funding time (UTC)"),
            },
        )


def render_dashboard_rows(
    rows: list[DashboardFundingRow],
    exchanges: list[str],
    loaded_at: float,
) -> None:
    loaded_at_iso = datetime.fromtimestamp(loaded_at, tz=timezone.utc).isoformat()
    st.markdown(f'<div class="rwa-status"><strong>数据状态</strong> 后台数据({len(rows):,}) &nbsp; | &nbsp; <strong>交易所</strong> {escape(",".join(exchanges))} &nbsp; | &nbsp; <strong>加载时间</strong> {escape(loaded_at_iso)} &nbsp; | &nbsp; <strong>Now UTC</strong> {datetime.now(timezone.utc).isoformat()}</div>', unsafe_allow_html=True)
    if not rows:
        st.warning("暂无可展示的数据。请确认后台任务已经写入数据。")
        return

    frame = pd.DataFrame(as_table_rows(rows))
    frame = frame.sort_values(by=["sort_apr", "exchange", "symbol"], ascending=[False, True, True]).reset_index(drop=True)
    render_kpis(frame)
    render_breadth_chips(frame)

    chart_left, chart_right = st.columns([1.15, 1], gap="medium")
    with chart_left:
        with st.container(border=True):
            st.plotly_chart(build_apr_bar_chart(frame), width="stretch", config={"displayModeBar": False})
    with chart_right:
        with st.container(border=True):
            st.plotly_chart(build_liquidity_scatter(frame), width="stretch", config={"displayModeBar": False})

    left, right = st.columns(2, gap="medium")
    with left:
        with st.container(border=True):
            st.plotly_chart(build_exchange_oi_share_chart(frame), width="stretch", config={"displayModeBar": False})
    with right:
        with st.container(border=True):
            st.plotly_chart(build_symbol_volume_chart(frame), width="stretch", config={"displayModeBar": False})
    render_funding_table(frame)


def render_comparison_table(rows: list[AprComparisonRow], label: str) -> None:
    if not rows:
        st.info(f"{label} 下暂无两个及以上交易所共有的 symbol。")
        return
    frame = pd.DataFrame(as_comparison_table_rows(rows)).sort_values(by=["apr_diff", "canonical_symbol"], ascending=[False, True]).reset_index(drop=True)
    with st.container(border=True):
        st.plotly_chart(build_comparison_chart(rows, label), width="stretch", config={"displayModeBar": False})
    with st.container(border=True):
        st.dataframe(
            frame,
            width="stretch",
            hide_index=True,
            column_order=["canonical_symbol", "exchange_count", "apr_diff", "max_exchange", "max_symbol", "max_apr", "min_exchange", "min_symbol", "min_apr", "exchange_aprs"],
            column_config={
                "canonical_symbol": st.column_config.TextColumn("统一 Symbol", pinned=True),
                "exchange_count": st.column_config.NumberColumn("交易所数", format="%d"),
                "apr_diff": st.column_config.NumberColumn("APR 差异", format="%.2f%%"),
                "max_exchange": st.column_config.TextColumn("最高交易所"),
                "max_symbol": st.column_config.TextColumn("最高 Symbol"),
                "max_apr": st.column_config.NumberColumn("最高 APR", format="%.2f%%"),
                "min_exchange": st.column_config.TextColumn("最低交易所"),
                "min_symbol": st.column_config.TextColumn("最低 Symbol"),
                "min_apr": st.column_config.NumberColumn("最低 APR", format="%.2f%%"),
                "exchange_aprs": st.column_config.TextColumn("各交易所 APR"),
            },
        )


def render_apr_comparison(rows: list[DashboardFundingRow], selected_symbols: list[str]) -> None:
    if not rows:
        st.warning("暂无可比较的数据。")
        return
    if selected_symbols:
        rows = [row for row in rows if row.canonical_symbol in set(selected_symbols)]
    tabs = st.tabs([label for label, _ in APR_COMPARISON_WINDOWS])
    for tab, (label, apr_field) in zip(tabs, APR_COMPARISON_WINDOWS):
        with tab:
            render_comparison_table(build_apr_comparison_rows(rows, apr_field), label)


def as_rh_table_rows(rows: list[RhPoolRow], selected_window: str) -> list[dict[str, object]]:
    window_field = f"window_{selected_window}_percent"
    rank_field = f"rank_{selected_window}"
    fee_income_field = f"fee_income_{selected_window}_usd"
    return [
        {
            "token": row.token,
            "token_name": row.token_name,
            "pool_name": row.pool_name,
            "pool_address": row.pool_address,
            "tvl_usd": float(row.tvl_usd) if row.tvl_usd is not None else None,
            "volume_24h_usd": float(row.volume_24h_usd) if row.volume_24h_usd is not None else None,
            "is_new_issue": row.is_new_issue,
            "fee_income_usd": float(getattr(row, fee_income_field)) if getattr(row, fee_income_field) is not None else None,
            "fee_apr_percent": float(row.fee_apr_percent) if row.fee_apr_percent is not None else None,
            "window_apr_percent": float(getattr(row, window_field)) if getattr(row, window_field) is not None else None,
            "rank": getattr(row, rank_field),
            "last_metric_time_utc": row.last_metric_time_iso,
            "synced_at_utc": row.synced_at_iso,
        }
        for row in rows
    ]


def render_rh_pool_table(frame: pd.DataFrame, selected_window: str) -> None:
    with st.container(border=True):
        st.markdown("#### :material/table_chart: Pool surface")
        st.caption(f"当前按 {RH_WINDOW_LABELS[selected_window]}年化收益率排序；空值表示源表没有足够数据计算该窗口收益率。")
        st.dataframe(
            frame,
            width="stretch",
            hide_index=True,
            column_order=["token", "pool_name", "pool_address", "is_new_issue", "tvl_usd", "volume_24h_usd", "fee_income_usd", "fee_apr_percent", "window_apr_percent", "rank", "last_metric_time_utc", "synced_at_utc"],
            column_config={
                "token": st.column_config.TextColumn("代币", pinned=True),
                "pool_name": st.column_config.TextColumn("Pool"),
                "pool_address": st.column_config.TextColumn("Pool address", pinned=True),
                "is_new_issue": st.column_config.CheckboxColumn("新股票"),
                "tvl_usd": st.column_config.NumberColumn("Pool size proxy (USD)", format="$%,.0f"),
                "volume_24h_usd": st.column_config.NumberColumn("24h Swap volume (USD)", format="$%,.0f"),
                "fee_income_usd": st.column_config.NumberColumn("窗口手续费收入 (USD)", format="$%,.0f"),
                "fee_apr_percent": st.column_config.NumberColumn("当前年化收益率", format="%.2f%%"),
                "window_apr_percent": st.column_config.NumberColumn(f"{selected_window} 年化收益率", format="%.2f%%"),
                "rank": st.column_config.NumberColumn(f"{selected_window} rank", format="%d"),
                "last_metric_time_utc": st.column_config.TextColumn("Metric time (UTC)"),
                "synced_at_utc": st.column_config.TextColumn("Sync time (UTC)"),
            },
        )


def render_rh_pools_page() -> tuple[list[str], list[str]]:
    render_hero("RH Pools", "RH 链 RWA 池子查询原型：基于公开窗口排名表，按交易对、收益窗口和 pool address 快速定位池子。")
    window_query = str(st.query_params.get(RH_WINDOW_QUERY_KEY, "2h"))
    if window_query not in RH_WINDOW_OPTIONS:
        window_query = "2h"
    if RH_WINDOW_KEY not in st.session_state:
        st.session_state[RH_WINDOW_KEY] = RH_WINDOW_LABELS[window_query]
    if RH_NEW_ISSUE_KEY not in st.session_state:
        st.session_state[RH_NEW_ISSUE_KEY] = str(st.query_params.get("rh_new_issue", "false")).lower() == "true"
    new_issue_only = bool(st.session_state[RH_NEW_ISSUE_KEY])
    config = load_config()
    tables: dict[str, Any] = {}
    source_is_demo = config is None
    if source_is_demo:
        rows = demo_rh_pool_rows()
        st.caption("当前为演示数据：配置 Supabase 后将自动读取公开的 RH 窗口排名表。")
    else:
        try:
            tables = get_cached_rh_tables(config, DEFAULT_REFRESH_SECONDS, new_issue_only)
            table_errors = tables.get(RH_TABLE_ERRORS_KEY, {})
            if table_errors:
                st.caption("部分表当前不可读：" + ", ".join(sorted(table_errors)) + "；已使用可读表继续渲染原型。")
            rows = build_rh_pool_rows(tables)
            if not rows:
                if new_issue_only and not table_errors:
                    tables = get_cached_rh_tables(config, DEFAULT_REFRESH_SECONDS, False)
                    rows = build_rh_pool_rows(tables)
                if not rows:
                    st.warning("公开 RH 窗口排名表已连接，但没有解析出带 pool address 的记录；当前展示演示数据以便继续评审原型。")
                    rows = demo_rh_pool_rows()
                    source_is_demo = True
        except (requests.RequestException, DataApiError, ValueError) as exc:
            st.warning(f"RH 公开窗口排名表读取失败，当前展示演示数据。错误类型: {type(exc).__name__}")
            rows = demo_rh_pool_rows()
            source_is_demo = True

    row_table = pd.DataFrame(as_rh_table_rows(rows, "24h"))
    token_options = sorted(
        {
            text_value(row, "token")
            for row in tables.get(RH_TOKEN_UNIVERSE_KEY, [])
            if text_value(row, "token")
        }
    )
    if not token_options:
        token_options = sorted(row_table["token"].dropna().unique().tolist())
    address_options = sorted(row_table["pool_address"].dropna().unique().tolist())
    with st.container(border=True):
        st.markdown("#### :material/filter_alt: Pool filters")
        window_labels = list(RH_WINDOW_LABELS.values())
        selected_window_label = st.segmented_control("统计窗口", options=window_labels, key=RH_WINDOW_KEY)
        selected_window = next((window for window, label in RH_WINDOW_LABELS.items() if label == selected_window_label), "2h")
        prepare_multiselect_state(RH_TOKEN_KEY, "rh_tokens", token_options)
        prepare_multiselect_state(RH_ADDRESS_KEY, "rh_addresses", address_options)
        col_token, col_address, col_new_issue = st.columns([1, 1, 0.7], gap="medium")
        with col_token:
            selected_tokens = st.multiselect("代币（多选）", options=token_options, placeholder="全部代币", key=RH_TOKEN_KEY)
        with col_address:
            selected_addresses = st.multiselect("Pool address（多选）", options=address_options, placeholder="全部池子", key=RH_ADDRESS_KEY)
        with col_new_issue:
            selected_new_issue = st.checkbox("新股票", key=RH_NEW_ISSUE_KEY, help="仅显示池内存在 24 小时内新发行标记的股票池。")
        scope_label = "all_active + is_new_issue=true" if selected_new_issue else "latest20"
        st.caption(f"数据源：{'演示数据' if source_is_demo else f'Supabase / rh_pool_dashboard + 4h rankings（{scope_label}）'} · 支持通过 URL 参数保存筛选结果。")
        st.query_params[RH_WINDOW_QUERY_KEY] = selected_window
        st.query_params["rh_new_issue"] = "true" if selected_new_issue else "false"

    display_frame = pd.DataFrame(as_rh_table_rows(rows, selected_window))
    if selected_tokens:
        display_frame = display_frame[display_frame["token"].isin(set(selected_tokens))]
    if selected_addresses:
        display_frame = display_frame[display_frame["pool_address"].isin(set(selected_addresses))]
    if selected_new_issue:
        display_frame = display_frame[display_frame["is_new_issue"]]
    display_frame = display_frame.sort_values(by=["window_apr_percent", "tvl_usd"], ascending=[False, False], na_position="last").reset_index(drop=True)
    if selected_new_issue:
        st.caption(f"新股票池命中：{len(display_frame):,} / {len(rows):,}")
    if display_frame.empty:
        message = "当前数据库没有 `is_new_issue=true` 的池子。" if selected_new_issue else "当前筛选条件下没有池子。请减少代币或 pool address 的选择。"
        st.warning(message)
        return selected_tokens, selected_addresses

    render_rh_pool_table(display_frame, selected_window)
    with st.expander("原型说明 / 四表职责", icon=":material/info:"):
        st.markdown(
            "- `rh_pool_dashboard`：前端主查询视图，提供池名、24h 成交量、Pool size proxy、2h/24h 年化收益率、排名和同步时间。\n"
            "- `rh_pool_window_rankings`：仅补充视图没有的 4h 年化收益率、手续费和排名。\n"
            "- `rh_pool_hourly_metrics`、`rh_rwa_assets`、`rh_sync_checkpoints`：文档标记为 Worker service role 专用，前端不直接读取。\n\n"
            "查询固定使用 `chain_id=4663`；未勾选使用 `asset_scope=latest20`，勾选新股票使用 `asset_scope=all_active` + `is_new_issue=true`；ranking 补充查询额外使用 `is_public=true`，并按 `annualized_yield_percent desc nulls last` 排序。池子名称使用视图的 `pool`（如 `GLXY/USDG`）。"
        )
    return selected_tokens, selected_addresses


def render_missing_config() -> None:
    st.error("缺少后台数据读取配置。请在 Streamlit Secrets 中配置 SUPABASE_URL 和 SUPABASE_PUBLISHABLE_KEY。")


def render_default_refresh_note() -> None:
    st.caption(f"默认数据刷新频率：{DEFAULT_REFRESH_SECONDS} 秒")


def query_param_values(param_key: str) -> list[str]:
    if param_key not in st.query_params:
        return []
    if hasattr(st.query_params, "get_all"):
        values = st.query_params.get_all(param_key)
    else:
        value = st.query_params.get(param_key, [])
        values = value if isinstance(value, list) else [value]
    return [str(value) for value in values if str(value)]


def query_param_selection(param_key: str, options: list[str], default: list[str]) -> list[str]:
    if param_key not in st.query_params:
        return list(default)
    raw_values = query_param_values(param_key)
    if EMPTY_QUERY_SELECTION in raw_values:
        return []
    option_set = set(options)
    return [value for value in raw_values if value in option_set]


def prepare_multiselect_state(
    widget_key: str,
    param_key: str,
    options: list[str],
    default: list[str] | None = None,
) -> None:
    default_values = default or []
    raw_query_values = query_param_values(param_key)
    query_values = query_param_selection(param_key, options, default_values)
    pending_query_key = f"{widget_key}_pending_query_values"
    if widget_key not in st.session_state:
        st.session_state[widget_key] = query_values
        if raw_query_values and not query_values and not options:
            st.session_state[pending_query_key] = raw_query_values
        else:
            st.session_state.pop(pending_query_key, None)
        return
    pending_query_values = st.session_state.get(pending_query_key, [])
    if pending_query_values and not st.session_state.get(widget_key):
        option_set = set(options)
        restored_values = [value for value in pending_query_values if value in option_set]
        if restored_values:
            st.session_state[widget_key] = restored_values
            st.session_state.pop(pending_query_key, None)
            return
        if options:
            st.session_state.pop(pending_query_key, None)
    option_set = set(options)
    current_values = st.session_state.get(widget_key, [])
    st.session_state[widget_key] = [value for value in current_values if value in option_set]


def set_query_param_selection(param_key: str, values: list[str] | None) -> None:
    if values is None:
        return
    current_values = query_param_values(param_key)
    if current_values == values or (not values and current_values == [EMPTY_QUERY_SELECTION]):
        return
    if not values:
        st.query_params[param_key] = EMPTY_QUERY_SELECTION
        return
    st.query_params[param_key] = values


def sync_symbol_options(options_key: str, rows: list[DashboardFundingRow]) -> bool:
    symbol_options = sorted({row.canonical_symbol for row in rows if row.canonical_symbol})
    if st.session_state.get(options_key) == symbol_options:
        return False
    st.session_state[options_key] = symbol_options
    return True


def main() -> None:
    st.set_page_config(page_title="美股资金费套利", page_icon=":material/query_stats:", layout="wide")
    inject_style()
    needs_symbol_options_refresh = False
    rh_selected_tokens: list[str] = []
    rh_selected_addresses: list[str] = []
    top_tab_key = str(st.query_params.get("tab", "home"))
    if top_tab_key not in TOP_TAB_OPTIONS:
        top_tab_key = "home"
    if TOP_TAB_STATE_KEY not in st.session_state:
        st.session_state[TOP_TAB_STATE_KEY] = TOP_TAB_OPTIONS[top_tab_key]
    selected_top_tab = st.segmented_control(
        "页面",
        options=list(TOP_TAB_OPTIONS.values()),
        key=TOP_TAB_STATE_KEY,
        label_visibility="collapsed",
    )
    selected_top_tab_key = next(
        (key for key, label in TOP_TAB_OPTIONS.items() if label == selected_top_tab),
        top_tab_key,
    )
    if str(st.query_params.get("tab", "")) != selected_top_tab_key:
        st.query_params["tab"] = selected_top_tab_key

    exchanges: list[str] = []
    asset_type_filters: list[str] = []
    compare_exchanges: list[str] = []
    compare_asset_type_filters: list[str] = []
    compare_selected_symbols: list[str] = []
    compare_symbol_options: list[str] = []

    if selected_top_tab_key == "home":
        render_hero("美股资金费套利", "深色实时资金费仪表盘，融合 latest / next / rolling APR、OI 与 24h 成交量，快速发现跨交易所错位。")
        with st.container(border=True):
            st.markdown("#### :material/tune: Controls")
            col_exchange, col_type = st.columns([1.35, 1.1])
            with col_exchange:
                prepare_multiselect_state(HOME_EXCHANGES_KEY, "exchanges", EXCHANGE_OPTIONS, DEFAULT_EXCHANGES)
                exchanges = st.multiselect("交易所", options=EXCHANGE_OPTIONS, key=HOME_EXCHANGES_KEY)
            with col_type:
                prepare_multiselect_state(HOME_ASSET_TYPES_KEY, "asset_types", ASSET_TYPE_OPTIONS, DEFAULT_ASSET_TYPE_FILTERS)
                asset_type_filters = st.multiselect("类型（多选）", options=ASSET_TYPE_OPTIONS, placeholder="全部", key=HOME_ASSET_TYPES_KEY)
            render_default_refresh_note()
        if not exchanges:
            st.warning("请至少选择一个交易所。")
        elif load_config() is None:
            render_missing_config()
        else:
            try:
                rows, loaded_at = get_cached_rows(exchanges, asset_type_filters, DEFAULT_REFRESH_SECONDS)
                render_dashboard_rows(rows, exchanges, loaded_at)
            except (requests.RequestException, DataApiError, ValueError) as exc:
                st.error(f"后台数据读取失败，请稍后重试。错误类型: {type(exc).__name__}")
        set_query_param_selection("exchanges", exchanges)
        set_query_param_selection("asset_types", asset_type_filters)

    elif selected_top_tab_key == "rh_pools":
        rh_selected_tokens, rh_selected_addresses = render_rh_pools_page()
        set_query_param_selection("rh_tokens", rh_selected_tokens)
        set_query_param_selection("rh_addresses", rh_selected_addresses)

    else:
        render_hero("APR spread lab", "按统一 Symbol 对齐同标的，分别比较多交易所 24h / 7d / 15d / 30d APR 差异。")
        with st.container(border=True):
            st.markdown("#### :material/tune: Controls")
            col_exchange, col_type, col_symbol = st.columns([1.35, 1.1, 1.45])
            with col_exchange:
                prepare_multiselect_state(COMPARE_EXCHANGES_KEY, "compare_exchanges", EXCHANGE_OPTIONS, DEFAULT_EXCHANGES)
                compare_exchanges = st.multiselect("交易所", options=EXCHANGE_OPTIONS, key=COMPARE_EXCHANGES_KEY)
            with col_type:
                prepare_multiselect_state(COMPARE_ASSET_TYPES_KEY, "compare_asset_types", ASSET_TYPE_OPTIONS, DEFAULT_ASSET_TYPE_FILTERS)
                compare_asset_type_filters = st.multiselect("类型（多选）", options=ASSET_TYPE_OPTIONS, placeholder="全部", key=COMPARE_ASSET_TYPES_KEY)
            with col_symbol:
                compare_symbol_options = st.session_state.get("rwa_compare_symbol_options", [])
                prepare_multiselect_state(COMPARE_SYMBOLS_KEY, "compare_symbols", compare_symbol_options)
                compare_selected_symbols = st.multiselect("Symbol（多选）", options=compare_symbol_options, placeholder="全部", key=COMPARE_SYMBOLS_KEY)
            render_default_refresh_note()
        if len(compare_exchanges) < 2:
            st.warning("APR 比较至少需要选择两个交易所。")
        elif load_config() is None:
            render_missing_config()
        else:
            try:
                compare_rows, _ = get_cached_rows(compare_exchanges, compare_asset_type_filters, DEFAULT_REFRESH_SECONDS)
                needs_symbol_options_refresh = sync_symbol_options("rwa_compare_symbol_options", compare_rows) or needs_symbol_options_refresh
                render_apr_comparison(compare_rows, compare_selected_symbols)
            except (requests.RequestException, DataApiError, ValueError) as exc:
                st.error(f"后台数据读取失败，请稍后重试。错误类型: {type(exc).__name__}")
        set_query_param_selection("compare_exchanges", compare_exchanges)
        set_query_param_selection("compare_asset_types", compare_asset_type_filters)
        set_query_param_selection("compare_symbols", compare_selected_symbols if compare_symbol_options else None)

    if needs_symbol_options_refresh:
        st.rerun()


if __name__ == "__main__":
    main()
