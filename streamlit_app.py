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
EXCHANGE_OPTIONS = ["binance", "aster", "bitget", "okx", "bybit", "hyperliquid", "lighter", "extended"]
MILLION_USD = 1_000_000
PAGE_SIZE = 1000
DEFAULT_REFRESH_SECONDS = 300
ZERO_DECIMAL = Decimal("0")
DEFAULT_EXCHANGES = ["okx", "binance"]
DEFAULT_ASSET_TYPE_FILTERS = [ASSET_TYPE_STOCK]
HOME_EXCHANGES_KEY = "rwa_home_exchanges"
HOME_ASSET_TYPES_KEY = "rwa_home_asset_types"
HOME_SYMBOLS_KEY = "rwa_symbol_filter"
COMPARE_EXCHANGES_KEY = "rwa_compare_exchanges_picker"
COMPARE_ASSET_TYPES_KEY = "rwa_compare_asset_types_picker"
COMPARE_SYMBOLS_KEY = "rwa_compare_symbol_filter"
EMPTY_QUERY_SELECTION = "__empty__"


@dataclass(frozen=True, slots=True)
class DashboardFundingRow:
    exchange: str
    instrument: str
    canonical_symbol: str
    asset_type: str
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


def row_from_payload(row: dict[str, Any]) -> DashboardFundingRow:
    return DashboardFundingRow(
        exchange=str(row.get("exchange", "")),
        instrument=str(row.get("instrument", "")),
        canonical_symbol=str(row.get("canonical_symbol", "")),
        asset_type=str(row.get("asset_type", "")),
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
        }
        .stApp {
            background:
                radial-gradient(circle at 12% 0%, rgba(139, 233, 253, 0.14), transparent 32rem),
                radial-gradient(circle at 86% 4%, rgba(189, 147, 249, 0.16), transparent 30rem),
                linear-gradient(135deg, #0f111a 0%, #171923 48%, #10121a 100%);
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
        oi_min_col, oi_max_col, _ = st.columns([1, 1, 2.5])
        with oi_min_col:
            min_oi = st.number_input("最小 OI (M USD)", min_value=0.0, value=None, placeholder="不限制", key="rwa_table_min_oi")
        with oi_max_col:
            max_oi = st.number_input("最大 OI (M USD)", min_value=0.0, value=None, placeholder="不限制", key="rwa_table_max_oi")
        if min_oi is not None and max_oi is not None and min_oi > max_oi:
            st.warning("最小 OI 不能大于最大 OI。")
            return
        table_frame = frame
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
    selected_symbols: list[str],
) -> None:
    loaded_at_iso = datetime.fromtimestamp(loaded_at, tz=timezone.utc).isoformat()
    st.markdown(f'<div class="rwa-status"><strong>数据状态</strong> 后台数据({len(rows):,}) &nbsp; | &nbsp; <strong>交易所</strong> {escape(",".join(exchanges))} &nbsp; | &nbsp; <strong>加载时间</strong> {escape(loaded_at_iso)} &nbsp; | &nbsp; <strong>Now UTC</strong> {datetime.now(timezone.utc).isoformat()}</div>', unsafe_allow_html=True)
    if not rows:
        st.warning("暂无可展示的数据。请确认后台任务已经写入数据。")
        return

    frame = pd.DataFrame(as_table_rows(rows))
    if selected_symbols:
        frame = frame[frame["canonical_symbol"].isin(set(selected_symbols))]
        if frame.empty:
            st.warning("当前 Symbol 筛选下没有可展示的数据。")
            return
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
    home_tab, compare_tab = st.tabs([":material/dashboard: 首页", ":material/compare_arrows: APR比较"])

    with home_tab:
        render_hero("美股资金费套利", "深色实时资金费仪表盘，融合 latest / next / rolling APR、OI 与 24h 成交量，快速发现跨交易所错位。")
        with st.container(border=True):
            st.markdown("#### :material/tune: Controls")
            col_exchange, col_type, col_symbol = st.columns([1.35, 1.1, 1.45])
            with col_exchange:
                prepare_multiselect_state(HOME_EXCHANGES_KEY, "exchanges", EXCHANGE_OPTIONS, DEFAULT_EXCHANGES)
                exchanges = st.multiselect("交易所", options=EXCHANGE_OPTIONS, key=HOME_EXCHANGES_KEY)
            with col_type:
                prepare_multiselect_state(HOME_ASSET_TYPES_KEY, "asset_types", ASSET_TYPE_OPTIONS, DEFAULT_ASSET_TYPE_FILTERS)
                asset_type_filters = st.multiselect("类型（多选）", options=ASSET_TYPE_OPTIONS, placeholder="全部", key=HOME_ASSET_TYPES_KEY)
            with col_symbol:
                symbol_options = st.session_state.get("rwa_symbol_options", [])
                prepare_multiselect_state(HOME_SYMBOLS_KEY, "symbols", symbol_options)
                selected_symbols = st.multiselect("Symbol（多选）", options=symbol_options, placeholder="全部", key=HOME_SYMBOLS_KEY)
            render_default_refresh_note()
        if not exchanges:
            st.warning("请至少选择一个交易所。")
        elif load_config() is None:
            render_missing_config()
        else:
            try:
                rows, loaded_at = get_cached_rows(exchanges, asset_type_filters, DEFAULT_REFRESH_SECONDS)
                needs_symbol_options_refresh = sync_symbol_options("rwa_symbol_options", rows) or needs_symbol_options_refresh
                render_dashboard_rows(rows, exchanges, loaded_at, selected_symbols)
            except (requests.RequestException, DataApiError, ValueError) as exc:
                st.error(f"后台数据读取失败，请稍后重试。错误类型: {type(exc).__name__}")

    with compare_tab:
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

    if needs_symbol_options_refresh:
        st.rerun()
    set_query_param_selection("exchanges", exchanges)
    set_query_param_selection("asset_types", asset_type_filters)
    set_query_param_selection("symbols", selected_symbols if symbol_options else None)
    set_query_param_selection("compare_exchanges", compare_exchanges)
    set_query_param_selection("compare_asset_types", compare_asset_type_filters)
    set_query_param_selection("compare_symbols", compare_selected_symbols if compare_symbol_options else None)


if __name__ == "__main__":
    main()
