import streamlit as st
from streamlit_autorefresh import st_autorefresh
import psycopg2
import pandas as pd

st.set_page_config(page_title="Crypto Transaction Tracker", layout="wide")
st.title("🚀 Blockchain Transaction Dashboard")

# Auto refresh
st_autorefresh(interval=5000, key="refresh")

# DB connection
conn = psycopg2.connect(
    host="localhost",
    database="crypto",
    user="postgres",
    password="postgres"
)

query = """
SELECT * FROM transactions
ORDER BY timestamp DESC
LIMIT 200
"""

df = pd.read_sql(query, conn)

if df.empty:
    st.warning("⚠️ No data yet. Start pipeline.")
    st.stop()

df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")

# Split data
eth_df = df[df["value"] > 0]
token_df = df[df["is_token_tx"] == True]

# ======================
# Metrics
# ======================
latest = df.iloc[0]

col1, col2, col3 = st.columns(3)
col1.metric("Total Transactions", len(df))
col2.metric("Latest Value (ETH)", round(float(latest["value"]), 4))
col3.metric("Token Transactions", int(df["is_token_tx"].sum()))

# ======================
# Charts
# ======================

df_sorted = df.sort_values("timestamp")

# 📈 ETH Volume
st.subheader("📈 ETH Volume (per minute)")
df_grouped = df_sorted.groupby(
    pd.Grouper(key="timestamp", freq="1min")
)["value"].sum()

st.line_chart(df_grouped)

# ⚡ Transactions per minute (NEW)
st.subheader("⚡ Transactions per Minute")
tx_count = df_sorted.groupby(
    pd.Grouper(key="timestamp", freq="1min")
)["tx_hash"].count()

st.line_chart(tx_count)

# ======================
# 🐋 Whale Transactions
# ======================
st.subheader("🐋 Whale Transactions (>50 ETH)")

whales = df[df["value"] > 50]

if whales.empty:
    st.info("No whale transactions found")
else:
    st.dataframe(whales.head(10), use_container_width=True)

# ======================
# 🔍 Filter + Raw Data
# ======================
st.subheader("📄 Filter Transactions")

min_value = st.slider("Minimum ETH value", 0.0, 100.0, 0.0)

filtered_df = df[df["value"] >= min_value]

st.dataframe(filtered_df.head(20), use_container_width=True)