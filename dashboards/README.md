# Visualizing GitHub Real-Time Analytics

This directory contains instructions and resources for setting up dashboards to visualize real-time repository activity.

To give you the best of both worlds, we support two methods of visualization:
1. **Streamlit (Local Web Dashboard):** A Python-based real-time dashboard with automatic refresh (built-in and runs at `http://localhost:8501`).
2. **Power BI Desktop:** A enterprise-grade business intelligence dashboard connected directly to the PostgreSQL database.

---

## Option 1: Live Streamlit Dashboard

Our local Docker infrastructure automatically boots up a beautiful, dark-mode Streamlit dashboard. 

- **Access URL:** `http://localhost:8501`
- **Features:** 
  - Real-time auto-refresh (adjustable via sidebar slider).
  - Bar charts for the top 10 most active repositories (grouped by event count).
  - Donut/pie charts showing the distribution of event types.
  - Event timelines showing activity rates over sliding windows.
  - Full search and filtering capabilities.

---

## Option 2: Connecting Power BI Desktop to PostgreSQL

Power BI Desktop connects natively to our local PostgreSQL database. Follow these steps to build a premium visualization:

### 1. Prerequisites
- Download and install [Power BI Desktop](https://powerbi.microsoft.com/desktop/).
- Install the **Npgsql GAC Installer** (PostgreSQL driver for .NET). You can download it from [GitHub (Npgsql releases)](https://github.com/npgsql/npgsql/releases). Ensure you check the option **"Install Npgsql in GAC"** during installation and restart your PC.

### 2. Establish PostgreSQL Connection
1. Launch Power BI Desktop.
2. Select **Get Data** -> **More...** -> **Database** -> **PostgreSQL database**. Click **Connect**.
3. Set the database details:
   - **Server:** `localhost:5432`
   - **Database:** `github_events`
   - **Data Connectivity Mode:** Choose **DirectQuery** to enable real-time queries rather than loading a static snapshot (Import).
4. Click **OK**.
5. Select **Database** credentials on the left side, enter:
   - **User name:** `postgres`
    - **Password:** The value of `POSTGRES_PASSWORD` from your private `.env` file.
6. Click **Connect** (If you get an unencrypted connection warning, click **OK** / **Skip**).

---

### 3. Recommended Power BI SQL Queries (DirectQuery Mode)

Instead of importing the entire table, you can select **Advanced Options** when connecting and write optimized SQL queries to pre-format the datasets:

#### Dataset A: Repository Activity Summary (Main Visuals)
```sql
SELECT 
    window_start, 
    window_end, 
    repository_name, 
    event_type, 
    event_count
FROM repository_activity
WHERE window_start >= NOW() - INTERVAL '1 hour';
```

#### Dataset B: Key Metrics KPI (Card Values)
```sql
SELECT 
    COUNT(DISTINCT repository_name) as total_repositories,
    SUM(event_count) as total_events,
    MAX(window_end) as latest_update
FROM repository_activity;
```

---

### 4. Designing a High-Impact Dashboard (Theme & Layout)

To make your Power BI dashboard look outstanding, follow this design specification:

#### Theme Colors (HEX Palettes)
- **Primary Background:** `#0B0E14` (Deep Space Dark)
- **Card Backgrounds:** `#161B22` (Sleek Slate)
- **Accents:** 
  - PushEvent: `#FF7E5F` (Coral Sunrise)
  - WatchEvent / Star: `#FEB47B` (Amber Gold)
  - ForkEvent / PR: `#86A8E7` (Soft Indigo)
  - CreateEvent: `#91EAE4` (Mint)

#### Page Layout (Grid)
1. **Top Banner (Header):**
   - Dark background header containing the title **"GitHub Real-Time Analytics Pipeline"**.
   - Include a logo or GitHub icon.
2. **KPI Metrics Block (Top Row):**
   - Use **Card** visuals for `total_events` and `total_repositories`.
   - Use a **Card** visual for the `latest_update` timestamp, formatted to show seconds.
3. **Core Insights (Middle Row):**
   - **Left:** *Clustered Bar Chart* showing the top 10 repositories by event volume.
   - **Right:** *Donut Chart* showing the breakdown of event types.
4. **Time-Series Analysis (Bottom Row):**
   - *Line Chart* plotting the `window_end` timestamp on the X-axis, `event_count` on the Y-axis, and `event_type` as the legend. This shows the wave-like stream of incoming metrics!

#### Setting Up Dynamic Page Refresh in Power BI
Since we selected **DirectQuery** mode, we can configure Power BI to auto-refresh the canvas:
1. Click on the canvas (outside any visuals).
2. Go to the **Format** pane (paint roller icon).
3. Expand **Page Refresh**.
4. Turn the toggle **On**.
5. Set the duration to **5 seconds** or **10 seconds**.
6. Now, the dashboard will actively query PostgreSQL and update the charts in real-time, matching the speed of the Spark pipeline!
