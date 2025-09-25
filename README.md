# ❄️🛡️ Driver Compliance Monitor 🚗

## Overview
The **Driver Compliance Monitor** is a Streamlit application designed to help Snowflake administrators track and manage the drivers used across their accounts. The app automatically compares the drivers in use with Snowflake’s supported versions to highlight drivers that are **unsupported**, **near end-of-support**, or **fully supported**. It leverages Snowflake’s `ACCOUNT_USAGE` and `SYSTEM$CLIENT_VERSION_INFO()` along with AI-driven classification (via Snowflake Cortex) to deliver actionable compliance insights.

---

## Key Features
- **Real-Time Driver Analysis:** Fetches driver usage from Snowflake sessions for the past 30 days.
- **Support Status Classification:** Classifies drivers as:
  - 🟢 Supported  
  - 🟠 Near End of Support  
  - 🔴 Not Supported  
- **KPI Dashboard:** Displays total drivers processed, counts by support status.
- **Interactive Filters:** Filter results by driver name and support status.
- **Summary and User Level Reporting:** Shows driver name, User, version, last accessed date, unique users, total sessions, minimum supported version, end-of-support version, recommended version, and compliance status.
- **CSV Export:** Download the filtered compliance report as a CSV file.

---

## Use Cases
- Snowflake administrators ensuring all users are on compliant drivers.
- Compliance teams monitoring driver support lifecycle.
- IT teams planning upgrades for near end-of-support drivers.
- Auditing driver usage trends for governance and security.
- Enforcing users to use Supported Drivers

---

## How It Works
1. Fetches recent driver usage from `ACCOUNT_USAGE.SESSIONS`.
2. Retrieves minimum supported, recommended, and end-of-support versions from `SYSTEM$CLIENT_VERSION_INFO()`.
3. Merges usage and system metadata to classify drivers.
4. Uses Snowflake Cortex to determine support status automatically.
5. Displays a business-friendly dashboard with KPIs, filters, and CSV download options.
