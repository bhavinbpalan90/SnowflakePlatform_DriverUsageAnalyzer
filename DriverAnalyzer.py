import streamlit as st
import pandas as pd
from datetime import datetime

# -------------------------------
# Attempt to get active Snowflake session
# -------------------------------
try:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
except Exception:
    from snowflake.snowpark import Session
    session = Session.builder.getOrCreate()

# -------------------------------
# Page Config
# -------------------------------
st.set_page_config(page_title="❄️🛡️ Driver Compliance Monitor 🚗", layout="wide")

# -------------------------------
# Fetch Snowflake Account Metadata
# -------------------------------
try:
    account_info = session.sql("SELECT CURRENT_ACCOUNT() AS account_name, CURRENT_REGION() AS region").to_pandas().iloc[0]
    ACCOUNT_NAME = account_info["ACCOUNT_NAME"]
    REGION = account_info["REGION"]
except Exception:
    ACCOUNT_NAME = "Unknown"
    REGION = "Unknown"

CURRENT_DATE = datetime.now().strftime("%B %d, %Y")

# -------------------------------
# Header
# -------------------------------
st.markdown(
    f"""
    <div style="text-align:center; padding:10px 0;">
        <h1 style="margin-bottom:0;">❄️🛡️ Driver Compliance Monitor 🚗</h1>
        <p style="color:gray; font-size:16px; margin-top:4px;">
            Account: <b>{ACCOUNT_NAME}</b> | Region: <b>{REGION}</b> | Date: <b>{CURRENT_DATE}</b>
        </p>
    </div>
    """,
    unsafe_allow_html=True
)

# -------------------------------
# Helper
# -------------------------------
def lower_cols(df):
    df.columns = [c.lower() for c in df.columns]
    return df

def safe_str(x):
    return "" if x is None else str(x)

# -------------------------------
# Fetch User History
# -------------------------------
with st.spinner("Fetching recent driver usage (ACCOUNT_USAGE.SESSIONS)..."):
    user_history = session.sql("""
        SELECT CLIENT_APPLICATION_ID,
               SPLIT_PART(CLIENT_APPLICATION_ID,' ',1) AS DRIVER,
               SPLIT_PART(CLIENT_APPLICATION_ID,' ',2) AS VERSION,
               MAX(DATE(CREATED_ON)) AS LAST_ACCESSED_DATE,
               COUNT(SESSION_ID) AS TOTAL_SESSIONS,
               COUNT(DISTINCT USER_NAME) AS UNIQUE_USERS
        FROM SNOWFLAKE.ACCOUNT_USAGE.SESSIONS
        WHERE DATE(CREATED_ON) >= CURRENT_DATE()-30
          AND NOT (CLIENT_APPLICATION_ID ILIKE '%SNOWFLAKE%UI%'
               OR CLIENT_ENVIRONMENT ILIKE '%{"APPLICATION":"%Snowflake%"}'
               OR CLIENT_APPLICATION_ID ILIKE '%SNOWSIGHT%')
        GROUP BY ALL 
    """).to_pandas()

user_history = lower_cols(user_history)
if user_history.empty:
    st.warning("No driver usage rows returned for the last 30 days.")
    st.stop()

# -------------------------------
# Fetch System Driver Metadata
# -------------------------------
with st.spinner("Fetching system driver version metadata..."):
    min_versions = session.sql("""
        WITH x AS (SELECT PARSE_JSON(SYSTEM$CLIENT_VERSION_INFO()) info)
        SELECT
          value:clientAppId::string AS driver_name,
          value:minimumNearingEndOfSupportVersion::string AS end_of_support,
          value:minimumSupportedVersion::string AS min_supported,
          value:recommendedVersion::string AS recommended_version
        FROM x, lateral flatten(input=> info)
    """).to_pandas()

min_versions = lower_cols(min_versions)
if 'driver_name' in min_versions.columns and 'driver' not in min_versions.columns:
    min_versions = min_versions.rename(columns={'driver_name': 'driver'})
if 'driver' not in min_versions.columns:
    st.warning("SYSTEM$CLIENT_VERSION_INFO() did not return expected driver metadata.")
    st.stop()

# -------------------------------
# Merge User + Driver Metadata
# -------------------------------
merged = pd.merge(user_history, min_versions, left_on='driver', right_on='driver', how='inner', sort=False)
merged = merged[
    merged['min_supported'].notna() &
    merged['end_of_support'].notna() &
    (merged['min_supported'].astype(str) != '') &
    (merged['end_of_support'].astype(str) != '')
].reset_index(drop=True)

if merged.empty:
    st.warning("No drivers found that have metadata.")
    st.stop()

# -------------------------------
# Cortex Classification (Cache in Session State)
# -------------------------------
if 'driver_results' not in st.session_state:
    st.session_state.driver_results = []

if not st.session_state.driver_results:
    status_placeholder = st.empty()
    progress_bar = st.progress(0)
    results = []

    with st.spinner("Classifying drivers with Cortex..."):
        total = len(merged)
        for i, row in merged.iterrows():
            client_app = safe_str(row.get('client_application_id'))
            driver = safe_str(row.get('driver'))
            version = safe_str(row.get('version'))
            last_accessed = row.get('last_accessed_date')
            total_sessions = row.get('total_sessions')
            total_unique_users = row.get('unique_users')
            min_supported = safe_str(row.get('min_supported'))
            end_of_support = safe_str(row.get('end_of_support'))
            recommended_version = safe_str(row.get('recommended_version'))

            prompt = (
                "You are a helpful AI assistant that will only respond with a single word. "
                "Determine the support status of a driver version. "
                f"- DRIVER_USED: {client_app}\n"
                f"- MINIMUM_VERSION: {min_supported}\n"
                f"- END_OF_SUPPORT: {end_of_support}\n"
                f"- RECOMMENDED_VERSION: {recommended_version}\n"
                "Rules:\n"
                "1. If DRIVER_USED < MINIMUM_VERSION → respond 'Not Supported'.\n"
                "2. If DRIVER_USED >= MINIMUM_VERSION and <= END_OF_SUPPORT → respond 'Near End of Support'.\n"
                "3. If DRIVER_USED > END_OF_SUPPORT → respond 'Supported'.\n"
                "Output exactly one word from: Supported, Not Supported, Near End of Support. "
            )
            prompt_escaped = prompt.replace("'", "''")
            sql = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('openai-gpt-4.1', '{prompt_escaped}') AS result"

            try:
                res = session.sql(sql).collect()[0][0]
                ai_resp = str(res).strip() if res else "Unknown"
            except:
                ai_resp = "Error"

            status_placeholder.info(f"Processing {i+1}/{total}: {client_app} → {ai_resp}")

            results.append({
                "client_application_id": client_app,
                "driver": driver,
                "version": version,
                "last_accessed_date": last_accessed,
                "total_sessions": total_sessions,
                "total_unique_users": total_unique_users,
                "min_supported": min_supported,
                "end_of_support": end_of_support,
                "recommended_version": recommended_version,
                "ai_response": ai_resp
            })

            progress_bar.progress(int(((i + 1)/total)*100))

    st.session_state.driver_results = results

# -------------------------------
# Use Cached Results
# -------------------------------
results_df = pd.DataFrame(st.session_state.driver_results)
emoji_map = {
    "Supported": "🟢 Supported",
    "Not Supported": "🔴 Not Supported",
    "Near End of Support": "🟠 Near End of Support",
    "Unknown": "⚪️ Unknown",
    "Error": "⚪️ Error"
}
results_df['status'] = results_df['ai_response'].map(lambda x: emoji_map.get(x, f"⚪️ {x}"))

# -------------------------------
# KPI Section
# -------------------------------
total_processed = len(results_df)
supported_count = (results_df['ai_response'] == "Supported").sum()
near_end_count = (results_df['ai_response'] == "Near End of Support").sum()
not_supported_count = (results_df['ai_response'] == "Not Supported").sum()

st.divider()
k1, k2, k3, k4 = st.columns(4)
k1.metric("Drivers Processed", total_processed)
k2.metric("🟢 Supported", supported_count)
k3.metric("🟠 Near End of Support", near_end_count)
k4.metric("🔴 Not Supported", not_supported_count)
st.divider()

# -------------------------------
# Filters
# -------------------------------
st.subheader("🔎 Filter Results")
col1, col2 = st.columns([2,2])
with col1:
    drivers_list = sorted(results_df['driver'].dropna().unique())
    selected_drivers = st.multiselect("Select Driver", drivers_list, default=drivers_list)
with col2:
    status_options = sorted(results_df['ai_response'].dropna().unique())
    selected_status = st.multiselect("Select Status", status_options, default=status_options)

filtered = results_df[
    (results_df['driver'].isin(selected_drivers)) &
    (results_df['ai_response'].isin(selected_status))
].reset_index(drop=True)

# -------------------------------
# Business-Friendly Columns
# -------------------------------
display_cols = {
    "driver": "Driver Name",
    "version": "Driver Version",
    "last_accessed_date": "Last Accessed Date",
    "total_unique_users": "Unique Users",
    "total_sessions": "Total Sessions",
    "min_supported": "Minimum Supported Version",
    "end_of_support": "End of Support Version",
    "recommended_version": "Recommended Version",
    "status": "Support Status"
}
filtered = filtered.rename(columns=display_cols)

# -------------------------------
# Compliance Report
# -------------------------------
st.subheader("⚙️ Driver Version Compliance Report")
st.dataframe(filtered[list(display_cols.values())], use_container_width=True)

# -------------------------------
# CSV Download
# -------------------------------
csv = filtered[list(display_cols.values())].to_csv(index=False).encode("utf-8")
st.download_button("📥 Download Report as CSV", data=csv, file_name="driver_status_report.csv", mime="text/csv")

st.success("Processing complete ✅")
