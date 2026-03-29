"""
Generate a standalone HTML dashboard from the materialized DST tables.
Run on Databricks or locally with databricks-connect:
    %run ./generate_html_dashboard
    or: python generate_html_dashboard.py
"""
import json
import pandas as pd
import numpy as np
from decimal import Decimal
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------
WINDOW_ORDER = ["PRE_LY", "POST_LY", "PRE_CY", "POST_CY"]
W_LABELS = {"PRE_LY": "Pre (LY)", "POST_LY": "Post (LY)", "PRE_CY": "Pre (CY)", "POST_CY": "Post (CY)"}
W_COLORS = {"PRE_LY": "#a8d8ea", "POST_LY": "#6baed6", "PRE_CY": "#fdcdac", "POST_CY": "#e6550d"}


def to_float_df(pdf):
    out = pdf.copy()
    for col in out.columns:
        try:
            out[col] = pd.to_numeric(out[col], errors="ignore")
        except Exception:
            pass
    return out


def load_table(name):
    return to_float_df(spark.sql(f"SELECT * FROM production.supply_analytics.{name}").toPandas())


def resolve_weekly(name):
    for base in [
        Path.cwd() / "DST_Analysis" / "SQL" / "weekly",
        Path.cwd().parent / "SQL" / "weekly",
        Path.cwd() / "SQL" / "weekly",
        Path("/Workspace/Users/shazeb.asad@getyourguide.com/Pricing-Supply-Analytics/DST_Analysis/SQL/weekly"),
    ]:
        p = base / name
        if p.exists():
            return p
    raise FileNotFoundError(name)


def compute_did(df, metric, wc="period_window"):
    vals = df.set_index(wc)[metric]
    for w in WINDOW_ORDER:
        if w not in vals.index:
            return np.nan
    eps = 1e-12
    cy = (vals["POST_CY"] - vals["PRE_CY"]) / (vals["PRE_CY"] + eps)
    ly = (vals["POST_LY"] - vals["PRE_LY"]) / (vals["PRE_LY"] + eps)
    return float(cy - ly)

# ---------------------------------------------------------------------------
# Chart JSON builders
# ---------------------------------------------------------------------------
_cid = 0
def next_id():
    global _cid; _cid += 1; return f"c{_cid}"


def bar_chart(df, metric, title, pct=False):
    cid = next_id()
    ordered = [w for w in WINDOW_ORDER if w in df["period_window"].values]
    sub = df[df["period_window"].isin(ordered)].set_index("period_window").reindex(ordered)
    vals = []
    for w in ordered:
        v = float(sub.loc[w, metric]) if w in sub.index and pd.notna(sub.loc[w, metric]) else 0
        vals.append(round(v * 100, 2) if pct else round(v, 2))
    labels = [W_LABELS[w] for w in ordered]
    colors = [W_COLORS[w] for w in ordered]
    ds = [{"data": vals, "backgroundColor": colors, "borderWidth": 0, "borderRadius": 4}]
    tick = "value.toFixed(1)+'%'" if pct else "value.toLocaleString()"
    return f"""<div class="chart-box"><canvas id="{cid}"></canvas></div>
<script>new Chart(document.getElementById('{cid}'),{{type:'bar',data:{{labels:{json.dumps(labels)},datasets:{json.dumps(ds)}}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:{json.dumps(title)},font:{{size:13,weight:'bold'}}}},legend:{{display:false}}}},scales:{{y:{{ticks:{{callback:function(value){{return {tick}}}}}}}}}}}}});</script>"""


def line_chart(df, cy_col, ly_col, title, pct=False, wk="week_index", pc="period"):
    cid = next_id()
    cy = df[df[pc] == "CY"].sort_values(wk)
    ly = df[df[pc] == "LY"].sort_values(wk)
    labels = sorted(df[wk].dropna().unique().tolist())
    cv = {int(r[wk]): float(r[cy_col]) if pd.notna(r[cy_col]) else None for _, r in cy.iterrows()}
    lv = {int(r[wk]): float(r[ly_col]) if pd.notna(r[ly_col]) else None for _, r in ly.iterrows()}
    cy_vals = [round(cv.get(l), 4) if cv.get(l) is not None else None for l in labels]
    ly_vals = [round(lv.get(l), 4) if lv.get(l) is not None else None for l in labels]
    ds = [
        {"label": "CY", "data": cy_vals, "borderColor": "#e6550d", "borderWidth": 2, "pointRadius": 3, "tension": 0.15},
        {"label": "LY", "data": ly_vals, "borderColor": "#6baed6", "borderWidth": 1.5, "pointRadius": 2, "borderDash": [5, 3], "tension": 0.15},
    ]
    ann = []
    if 0 in labels:
        ei = labels.index(0)
        ann.append({"type": "line", "xMin": ei, "xMax": ei, "borderColor": "rgba(255,0,0,0.5)", "borderWidth": 2, "borderDash": [4, 4]})
    tick = "(value*100).toFixed(1)+'%'" if pct else "value.toLocaleString()"
    return f"""<div class="chart-box"><canvas id="{cid}"></canvas></div>
<script>new Chart(document.getElementById('{cid}'),{{type:'line',data:{{labels:{json.dumps([str(l) for l in labels])},datasets:{json.dumps(ds)}}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:{json.dumps(title)},font:{{size:13,weight:'bold'}}}},legend:{{position:'top'}},annotation:{{annotations:{json.dumps(ann)}}}}},scales:{{y:{{ticks:{{callback:function(value){{return {tick}}}}}}}}}}}}});</script>"""

# ---------------------------------------------------------------------------
# KPI card
# ---------------------------------------------------------------------------
def data_table(df, title, max_cols=20):
    """Render a pandas DataFrame as a styled HTML table."""
    cols = [c for c in df.columns[:max_cols]]
    hdr = "".join(f"<th>{c}</th>" for c in cols)
    rows = []
    for _, row in df.iterrows():
        cells = []
        for c in cols:
            v = row[c]
            if pd.isna(v):
                cells.append("<td>—</td>")
            elif isinstance(v, (float, np.floating)):
                cells.append(f"<td>{v:,.2f}</td>")
            else:
                cells.append(f"<td>{v}</td>")
        rows.append(f"<tr>{''.join(cells)}</tr>")
    return f"""<div class="table-section">
<h3>{title}</h3>
<div style="overflow-x:auto"><table class="data-tbl"><thead><tr>{hdr}</tr></thead><tbody>{"".join(rows)}</tbody></table></div>
</div>"""


def kpi_card(label, pre, post, did_val, pct=False):
    def fmt(v):
        if pd.isna(v): return "—"
        return f"{v:.1%}" if pct else f"{v:,.0f}"
    chg = ""
    if pd.notna(did_val):
        color = "#34d399" if did_val > 0 else "#f87171" if did_val < -0.005 else "#7c8097"
        chg = f'<div class="kpi-did" style="color:{color}">{did_val:+.1%} DID</div>'
    return f"""<div class="kpi-card">
  <div class="kpi-label">{label}</div>
  <div class="kpi-vals"><span>{fmt(pre)}</span> → <span style="font-weight:700">{fmt(post)}</span></div>
  {chg}
</div>"""

# ---------------------------------------------------------------------------
# Main HTML generator
# ---------------------------------------------------------------------------
def generate_dashboard(bookings_agg, supply_agg, customer_agg, price_agg, cancel_agg, parity_agg,
                       w_book, w_supply, w_cust, w_price, w_cancel, w_parity,
                       output_path="DST_Dashboard.html"):

    kpis = []
    for label, df, m, pct in [
        ("Net Revenue", bookings_agg, "nr", False),
        ("GMV", bookings_agg, "gmv", False),
        ("Bookings", bookings_agg, "bookings", False),
        ("Active Tours", supply_agg, "active_tours", False),
        ("Active Suppliers", supply_agg, "active_suppliers", False),
        ("Conversion Rate", customer_agg, "conversion_rate", True),
        ("Add-to-Cart", customer_agg, "add_to_cart_rate", True),
        ("Cancel Rate", cancel_agg, "cancellation_rate", True),
        ("Overall Parity", parity_agg, "overall_parity_rate", True),
    ]:
        if m not in df.columns:
            continue
        vals = df.set_index("period_window")[m]
        pre = vals.get("PRE_CY", np.nan)
        post = vals.get("POST_CY", np.nan)
        did = compute_did(df, m)
        kpis.append(kpi_card(label, pre, post, did, pct=pct))

    agg_items = [
        (bookings_agg, "nr", "Net Revenue", False),
        (bookings_agg, "gmv", "GMV", False),
        (bookings_agg, "bookings", "Bookings", False),
        (supply_agg, "active_tours", "Active Tours", False),
        (supply_agg, "active_suppliers", "Active Suppliers", False),
        (supply_agg, "share_active_tours", "Share Active Tours", True),
        (customer_agg, "conversion_rate", "Conversion Rate", True),
        (customer_agg, "add_to_cart_rate", "Add-to-Cart Rate", True),
        (customer_agg, "click_through_rate", "Click-Through Rate", True),
        (cancel_agg, "cancellation_rate", "Cancellation Rate", True),
        (cancel_agg, "cancellation_rate_3m", "Cancel Rate (3M travel)", True),
        (cancel_agg, "cancellation_rate_nr", "NR Cancel Rate", True),
        (parity_agg, "overall_parity_rate", "Overall Parity", True),
        (parity_agg, "supplier_parity_rate", "Supplier Parity", True),
        (parity_agg, "viator_parity_rate", "Viator Parity", True),
    ]
    agg_charts = []
    for df, m, t, pct in agg_items:
        if m in df.columns:
            agg_charts.append(bar_chart(df, m, t, pct=pct))

    # Price change aggregated table
    price_table = data_table(price_agg, "Price Change Analysis (Aggregated)")

    weekly_items = [
        (w_book, "nr", "Weekly Net Revenue", False),
        (w_book, "gmv", "Weekly GMV", False),
        (w_book, "bookings", "Weekly Bookings", False),
        (w_supply, "active_tours", "Weekly Active Tours", False),
        (w_supply, "active_suppliers", "Weekly Active Suppliers", False),
        (w_supply, "share_active_tours", "Weekly Share Active Tours", True),
        (w_cust, "conversion_rate", "Weekly Conversion Rate", True),
        (w_cust, "add_to_cart_rate", "Weekly Add-to-Cart Rate", True),
        (w_cust, "click_through_rate", "Weekly CTR", True),
        (w_price, "median_from_red_price", "Weekly Median From-Price (Red)", False),
        (w_price, "median_final_red_3m", "Weekly Final Red Price (3M)", False),
        (w_price, "median_timeslots_3m", "Weekly Timeslots (3M)", False),
        (w_cancel, "cancellation_rate", "Weekly Cancellation Rate", True),
        (w_cancel, "cancellation_rate_3m", "Weekly Cancel Rate (3M)", True),
        (w_parity, "overall_parity_rate", "Weekly Overall Parity", True),
        (w_parity, "supplier_parity_rate", "Weekly Supplier Parity", True),
    ]
    weekly_charts = []
    for df, m, t, pct in weekly_items:
        if m in df.columns:
            weekly_charts.append(line_chart(df, m, m, t, pct=pct))

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    countries = ", ".join(sorted(bookings_agg["country_name"].dropna().unique()))

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>DST Pass-Through — Impact Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-annotation@3.1.0/dist/chartjs-plugin-annotation.min.js"></script>
<style>
:root {{
  --bg:#f8f9fb; --surface:#fff; --card:#fff; --border:#e5e7eb;
  --text:#1a1a2e; --muted:#6b7280; --accent:#e6550d;
  --ly1:#a8d8ea; --ly2:#6baed6; --cy1:#fdcdac; --cy2:#e6550d;
  --green:#059669; --red:#dc2626;
}}
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:var(--bg);color:var(--text);font-size:14px;line-height:1.5}}
.header{{background:var(--surface);border-bottom:1px solid var(--border);padding:24px 40px;display:flex;justify-content:space-between;align-items:center}}
.header h1{{font-size:22px;font-weight:700}}
.header .meta{{color:var(--muted);font-size:12px}}
.tabs{{display:flex;gap:0;background:var(--surface);border-bottom:2px solid var(--border);padding:0 40px}}
.tab{{padding:12px 24px;font-size:14px;font-weight:600;color:var(--muted);cursor:pointer;border-bottom:3px solid transparent;transition:.15s}}
.tab:hover{{color:var(--text)}}
.tab.active{{color:var(--accent);border-bottom-color:var(--accent)}}
.view{{display:none;padding:24px 40px 60px}}
.view.active{{display:block}}
.kpi-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:12px;margin-bottom:28px}}
.kpi-card{{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:16px}}
.kpi-label{{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;font-weight:600}}
.kpi-vals{{font-size:16px;font-weight:600;margin-top:6px}}
.kpi-did{{font-size:13px;font-weight:700;margin-top:4px}}
.chart-grid{{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin-bottom:32px}}
.chart-box{{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:16px;height:320px}}
.section-title{{font-size:18px;font-weight:700;margin:24px 0 16px;padding-bottom:8px;border-bottom:2px solid var(--border)}}
@media(max-width:1024px){{.chart-grid{{grid-template-columns:repeat(2,1fr)}}}}
@media(max-width:640px){{.chart-grid{{grid-template-columns:1fr}}.kpi-grid{{grid-template-columns:repeat(2,1fr)}}}}
.table-section{{margin:24px 0}}
.table-section h3{{font-size:15px;font-weight:600;margin-bottom:8px}}
.data-tbl{{border-collapse:collapse;font-size:12px;width:100%}}
.data-tbl th{{background:#f3f4f6;padding:6px 10px;text-align:left;border:1px solid var(--border);font-weight:600;white-space:nowrap}}
.data-tbl td{{padding:5px 10px;border:1px solid var(--border);white-space:nowrap}}
.data-tbl tr:nth-child(even){{background:#f9fafb}}
</style>
</head>
<body>
<div class="header">
  <div><h1>DST Pass-Through — Impact Dashboard</h1><div class="meta">Markets: {countries}</div></div>
  <div class="meta">Generated: {now}</div>
</div>
<div class="tabs">
  <div class="tab active" onclick="switchTab('summary')">Executive Summary</div>
  <div class="tab" onclick="switchTab('aggregated')">Aggregated Analysis</div>
  <div class="tab" onclick="switchTab('weekly')">Weekly Event Study</div>
</div>

<div id="summary" class="view active">
  <div class="section-title">Key Metrics — YoY-Corrected DID (Pre CY → Post CY vs Pre LY → Post LY)</div>
  <div class="kpi-grid">
    {"".join(kpis)}
  </div>
</div>

<div id="aggregated" class="view">
  <div class="section-title">Aggregated Pre/Post Comparison</div>
  <div class="chart-grid">
    {"".join(agg_charts)}
  </div>
  {price_table}
</div>

<div id="weekly" class="view">
  <div class="section-title">Weekly Event Study — CY vs LY</div>
  <div class="chart-grid">
    {"".join(weekly_charts)}
  </div>
</div>

<script>
function switchTab(id) {{
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
  document.getElementById(id).classList.add('active');
  event.target.classList.add('active');
}}
</script>
</body>
</html>"""

    out = Path(output_path)
    out.write_text(html)
    print(f"Dashboard saved to {out.resolve()} ({len(html):,} bytes)")
    return str(out.resolve())


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
if __name__ == "__main__" or True:
    print("Loading data from Databricks...")
    ba = load_table("dst_booking_dataset")
    sa = load_table("dst_supply_dataset")
    ca = load_table("dst_session_performance_dataset")
    pa = load_table("dst_price_change")
    xa = load_table("dst_cancellation_dataset")
    ra = load_table("dst_price_parity")

    wb = to_float_df(spark.sql(resolve_weekly("weekly_bookings.sql").read_text()).toPandas())
    ws = to_float_df(spark.sql(resolve_weekly("weekly_supplier.sql").read_text()).toPandas())
    wc = to_float_df(spark.sql(resolve_weekly("weekly_customer_data.sql").read_text()).toPandas())
    wp = to_float_df(spark.sql(resolve_weekly("weekly_price_change.sql").read_text()).toPandas())
    wxc = to_float_df(spark.sql(resolve_weekly("weekly_cancellation.sql").read_text()).toPandas())
    wpr = to_float_df(spark.sql(resolve_weekly("weekly_price_parity.sql").read_text()).toPandas())

    generate_dashboard(ba, sa, ca, pa, xa, ra, wb, ws, wc, wp, wxc, wpr,
                       output_path="DST_Dashboard.html")
