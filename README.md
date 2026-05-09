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
