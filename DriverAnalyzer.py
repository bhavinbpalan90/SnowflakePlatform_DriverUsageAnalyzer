import streamlit as st
import pandas as pd

# Attempt to get an active Snowflake session (works both in SiS and other Snowpark flows)
try:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
except Exception:
    from snowflake.snowpark import Session
    session = Session.builder.getOrCreate()

st.set_page_config(page_title="❄️🛡️ Driver Compliance Monitor 🚗", layout="wide")
st.title("❄️🛡️ Driver Compliance Monitor 🚗")

# helper - normalize columns to lowercase
def lower_cols(df):
    df.columns = [c.lower() for c in df.columns]
    return df

# --- Fetch user history ---
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
    st.warning("No driver usage rows returned for the last day.")
    st.stop()

# --- Fetch system version metadata ---
with st.spinner("Fetching system driver version metadata..."):
    min_versions = session.sql("""
        WITH x AS (SELECT PARSE_JSON(SYSTEM$CLIENT_VERSION_INFO()) info)
        SELECT
          value:clientAppId::string AS DRIVER_NAME,
          value:minimumNearingEndOfSupportVersion::string AS END_OF_SUPPORT,
          value:minimumSupportedVersion::string AS MIN_SUPPORTED,
          value:recommendedVersion::string AS RECOMMENDED_VERSION
        FROM x, lateral flatten(input=> info)
    """).to_pandas()
min_versions = lower_cols(min_versions)
if 'driver_name' in min_versions.columns and 'driver' not in min_versions.columns:
    min_versions = min_versions.rename(columns={'driver_name': 'driver'})
if 'driver' not in min_versions.columns:
    st.warning("SYSTEM$CLIENT_VERSION_INFO() did not return expected driver metadata.")
    st.stop()

# --- Merge user history + metadata ---
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

# --- Initialize session state to cache Cortex results ---
if 'driver_results' not in st.session_state:
    st.session_state.driver_results = []

# --- Process Cortex only if not cached ---
if not st.session_state.driver_results:
    status_placeholder = st.empty()
    progress_bar = st.progress(0)
    results = []

    def safe_str(x):
        return "" if x is None else str(x)

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

            # ---- Correct Cortex prompt ----
            prompt = (
                "You are a helpful AI assistant that will only respond with a single word. "
                "Determine the support status of a driver version. "
                "You will be given:\n"
                f"- DRIVER_USED: {client_app}\n"
                f"- MINIMUM_VERSION: {min_supported}\n"
                f"- END_OF_SUPPORT: {end_of_support}\n"
                f"- RECOMMENDED_VERSION: {recommended_version}\n"
                "Rules:\n"
                "1. If DRIVER_USED < MINIMUM_VERSION → respond 'Not Supported'.\n"
                "2. If DRIVER_USED >= MINIMUM_VERSION and <= END_OF_SUPPORT → respond 'Near End of Support'.\n"
                "3. If DRIVER_USED > END_OF_SUPPORT → respond 'Supported'.\n"
                "Output exactly one word from: Supported, Not Supported, Near End of Support. "
                "Do not include any explanation or extra text."
            )
            prompt_escaped = prompt.replace("'", "''")
            sql = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('openai-gpt-4.1', '{prompt_escaped}') AS result"

            try:
                res = session.sql(sql).collect()[0][0]
                ai_resp = str(res).strip() if res else "Unknown"
            except:
                ai_resp = "Error"

            status_placeholder.info(f"Last processed ({i+1}/{total}): **{client_app}** → **{ai_resp}**")

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

# --- Use cached results ---
results_df = pd.DataFrame(st.session_state.driver_results)

# --- Add emoji mapping ---
emoji_map = {
    "Supported": "🟢 Supported",
    "Not Supported": "🔴 Not Supported",
    "Near End of Support": "🟠 Near End of Support",
    "Unknown": "⚪️ Unknown",
    "Error": "⚪️ Error"
}
results_df['status'] = results_df['ai_response'].map(lambda x: emoji_map.get(x, f"⚪️ {x}"))

# --- KPIs ---
total_processed = len(results_df)
supported_count = (results_df['ai_response'] == "Supported").sum()
near_end_count = (results_df['ai_response'] == "Near End of Support").sum()
not_supported_count = (results_df['ai_response'] == "Not Supported").sum()

st.divider()
k1, k2, k3, k4 = st.columns(4)
k1.metric("Drivers processed", total_processed)
k2.metric("Supported", supported_count)
k3.metric("Near End of Support", near_end_count)
k4.metric("Not Supported", not_supported_count)

st.divider()

# --- Filters ---
st.sidebar.header("Filters")
drivers_list = sorted(results_df['driver'].dropna().unique())
selected_drivers = st.sidebar.multiselect("Driver", drivers_list, default=drivers_list)
status_options = sorted(results_df['ai_response'].dropna().unique())
selected_status = st.sidebar.multiselect("Status", status_options, default=status_options)

filtered = results_df[
    (results_df['driver'].isin(selected_drivers)) &
    (results_df['ai_response'].isin(selected_status))
].reset_index(drop=True)

# --- Driver-level table ---
display_cols = [
    "driver",
    "version",
    "total_unique_users",
    "total_sessions",
    "min_supported",
    "end_of_support",
    "recommended_version",
    "status"  # emoji
]
display_cols = [c for c in display_cols if c in filtered.columns]

st.subheader("⚙️ Driver Version Compliance Report")
st.dataframe(filtered[display_cols], use_container_width=True)

csv = filtered[display_cols].to_csv(index=False).encode("utf-8")
st.download_button("📥 Download Report as CSV", data=csv, file_name="driver_status_report.csv", mime="text/csv")

# --- Expander for Not Supported users ---
with st.expander("Users on Not Supported drivers (details)"):
    not_supported_df = results_df[results_df['ai_response'] == "Not Supported"]
    if not not_supported_df.empty:
        cols = [
            "client_application_id",
            "driver",
            "version",
            "last_accessed_date",
            "total_sessions",
            "total_unique_users",
            "min_supported",
            "end_of_support",
            "recommended_version",
            "status"
        ]
        st.dataframe(not_supported_df[cols].reset_index(drop=True), use_container_width=True)
    else:
        st.write("No users found on Not Supported drivers.")

st.success("Processing complete ✅")
