import streamlit as st
import duckdb
import os
import openai
import pandas as pd
from datetime import datetime

# PAGE CONFIG
st.set_page_config(page_title="Crypto AI Analyst", layout="wide")
st.title("🤖 Real-Time Crypto AI Analyst")

# SIDEBAR CONFIG
with st.sidebar:
    st.header("⚙️ Configuration")
    openai_api_key = st.text_input("OpenAI API Key", type="password")
    use_mock_mode = st.checkbox("Demo Mode (No API Key)", value=True)
    
    st.divider()
    st.success("🟢 System Online")
    st.caption("Redpanda • Spark • MinIO • Iceberg")

# DATA CONNECTION
@st.cache_resource
def get_connection():
    con = duckdb.connect()
    con.sql("INSTALL httpfs; LOAD httpfs;")
    con.sql("INSTALL iceberg; LOAD iceberg;")
    con.sql(f"""
        CREATE SECRET secret1 (
            TYPE S3, KEY_ID 'minio', SECRET 'minio123', REGION 'us-east-1',
            ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL 'false'
        );
    """)
    return con

def query_data(query):
    try:
        return get_connection().sql(query).df()
    except Exception as e:
        return None

import altair as alt

# MAIN UI
# 1. Dashboard Header & Metrics
st.markdown("### Live Crypto Market (Lakehouse)")
metrics_placeholder = st.empty()

# 2. Main Layout
col_chart, col_ai = st.columns([2, 1])

# Fetch Data
df = query_data("SELECT * FROM read_parquet('s3://lakehouse/crypto_prices_v2/data/**/*.parquet') ORDER BY timestamp DESC LIMIT 200")

if df is not None and not df.empty:
    # Fix Types
    df['timestamp'] = pd.to_datetime(df['event_time'])
    df['price'] = df['price'].astype(float)
    df['volume'] = df['volume'].astype(float)
    
    # Calculate Technical Indicators
    df = df.sort_values('timestamp')
    df['SMA_5'] = df.groupby('symbol')['price'].transform(lambda x: x.rolling(window=5).mean())
    df['Volatility'] = df.groupby('symbol')['price'].transform(lambda x: x.pct_change().rolling(window=5).std())

    # Top Metrics
    latest = df.iloc[-1]
    with metrics_placeholder.container():
        m1, m2, m3 = st.columns(3)
        # Get latest price for each symbol
        latest_prices = df.groupby('symbol').last()['price']
        m1.metric("Bitcoin", f"${latest_prices.get('BITCOIN', 0):,.2f}")
        m2.metric("Ethereum", f"${latest_prices.get('ETHEREUM', 0):,.2f}")
        m3.metric("Solana", f"${latest_prices.get('SOLANA', 0):,.2f}")

    with col_chart:
        # Create Tabs for different views
        tab1, tab2 = st.tabs(["📉 Price & Trends", "📊 Volatility & Risk"])
        
        with tab1:
            # 1. Price Chart with SMA Overlay
            base = alt.Chart(df).encode(x=alt.X('timestamp', title=None, axis=alt.Axis(format='%H:%M:%S')))
            
            line = base.mark_line().encode(
                y=alt.Y('price', title='Price (USD)', scale=alt.Scale(zero=False)),
                color='symbol',
                tooltip=['timestamp', 'symbol', 'price']
            )
            
            sma = base.mark_line(strokeDash=[5, 5], opacity=0.5).encode(
                y='SMA_5',
                color='symbol',
                tooltip=['timestamp', 'symbol', 'SMA_5']
            )
            
            st.altair_chart((line + sma).interactive(), use_container_width=True)
            
            # 2. Volume Chart
            vol_chart = alt.Chart(df).mark_bar().encode(
                x=alt.X('timestamp', title=None, axis=alt.Axis(format='%H:%M:%S', labels=False)),
                y=alt.Y('volume', title='Volume'),
                color='symbol',
                tooltip=['timestamp', 'symbol', 'volume']
            ).properties(height=150).interactive()
            
            st.altair_chart(vol_chart, use_container_width=True)
            
        with tab2:
            # 3. Volatility Chart
            volatility_chart = alt.Chart(df).mark_line().encode(
                x=alt.X('timestamp', title='Time', axis=alt.Axis(format='%H:%M:%S')),
                y=alt.Y('Volatility', title='Price Volatility (Rolling StdDev)'),
                color='symbol',
                tooltip=['timestamp', 'symbol', 'Volatility']
            ).properties(height=300).interactive()
            
            st.altair_chart(volatility_chart, use_container_width=True)

    with col_ai:
        st.subheader("AI Analyst")
        user_query = st.text_area("Ask about trends:", "Why is Bitcoin moving up?")
        
        if st.button("Analyze Market"):
            if use_mock_mode:
                st.info("Using Demo Mode")
                st.markdown(f"""
                **Analysis based on real-time data:**
                
                The market is showing **volatility**.
                - **Bitcoin** is trading around **${latest_prices.get('BITCOIN', 0):,.0f}**.
                - **Ethereum** is stable.
                
                *This is a generated response demonstrating the UI.*
                """)
            elif not openai_api_key:
                st.error("Please provide an API Key.")
            else:
                # Real RAG
                context_df = df.groupby('symbol').mean()['price'].to_string()
                prompt = f"Analyze these prices: {context_df}. User Question: {user_query}"
                try:
                    client = openai.OpenAI(api_key=openai_api_key)
                    response = client.chat.completions.create(
                        model="gpt-3.5-turbo",
                        messages=[{"role": "user", "content": prompt}]
                    )
                    st.write(response.choices[0].message.content)
                except Exception as e:
                    st.error(f"Error: {e}")

else:
    st.warning("Waiting for data... (Spark is processing)")

