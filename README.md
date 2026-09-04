# 美股资金费套利看板

独立部署版 Streamlit 前端，只负责读取后台数据并展示 RWA / 美股资金费套利看板。

## 数据流

```text
交易所 API -> 后台 worker -> Supabase -> Streamlit 前端
```

本仓库不包含 worker、不抓交易所 API、不写 CSV。

## 本地运行

```bash
pip install -r requirements.txt
streamlit run streamlit_app.py --server.port=8501
```

本地可以用环境变量：

```bash
export SUPABASE_URL="https://your-project.supabase.co"
export SUPABASE_PUBLISHABLE_KEY="your-publishable-or-anon-key"
```

也可以复制 `.streamlit/secrets.toml.example` 为 `.streamlit/secrets.toml` 后填写。

## RH Pools 原型

顶部 `RH Pools` Tab 按数据库安全边界读取 `rh_pool_dashboard` 前端视图，并用公开的 `rh_pool_window_rankings` 补充 4h 数据。数据库中的相关表包括：

```text
rh_pool_hourly_metrics
rh_pool_window_rankings
rh_rwa_assets
rh_sync_checkpoints
```

页面固定查询 `chain_id=4663`；未勾选新股票时使用 `asset_scope=latest20`，勾选时使用 `asset_scope=all_active` + `is_new_issue=true`。代币选择框独立读取 `all_active` token universe，因此会列出所有 active pool 股票，不受当前结果 scope 限制。4h ranking 补充查询使用 `is_public=true`，按 `annualized_yield_percent desc nulls last` 排序，支持最近 2h / 4h / 24h 窗口切换，以及代币、pool address 和 `is_new_issue`（新股票）过滤。默认窗口为最近 2 小时；顶部 Tab、统计窗口、代币、pool address 和新股票条件会写入 URL，手动刷新后保持当前页面与筛选状态。`is_new_issue=true` 表示池内存在首次发现时间在 24 小时内的 active RWA 股票；未命中时页面显示空结果提示。`rh_pool_hourly_metrics`、`rh_rwa_assets`、`rh_sync_checkpoints` 仅供 Worker service role 使用，前端不直接读取。未能连接公开视图时，页面会展示标注为“演示数据”的原型样例。

## Streamlit Community Cloud

入口文件：

```text
streamlit_app.py
```

Secrets：

```toml
RWA_DATA_SOURCE = "supabase"
SUPABASE_URL = "https://your-project.supabase.co"
SUPABASE_PUBLISHABLE_KEY = "your-publishable-or-anon-key"
```

不要在前端部署中配置 service role / secret key。
