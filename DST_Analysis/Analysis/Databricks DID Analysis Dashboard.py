# Databricks DID Analysis Dashboard

```python
# ============================================
# CELL 1: IMPORTS AND SETUP
# ============================================
# Hide this cell in dashboard view

from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import warnings
warnings.filterwarnings('ignore')

# Set plotting style
plt.style.use('default')
plt.rcParams['figure.facecolor'] = 'white'
plt.rcParams['axes.facecolor'] = 'white'

print("✓ Libraries imported successfully")
```

```python
# ============================================
# CELL 2: PLOTTING FUNCTION DEFINITION
# ============================================
# Hide this cell in dashboard view

def plot_did_percent_change_three_comparisons(
    df,
    *,
    period_col="period_window",         
    window_col=None,  # Not used in this version, kept for compatibility
    treat_period="CY",
    control_period="LY",
    comparisons=(("PRE","COMM","Pre vs Comm"),
                 ("PRE","POST","Pre vs Post rollout"),
                 ("COMM","POST","Comm vs Post rollout")),
    metrics=None,
    metric_pretty=None,
    metric_group=None,
    group_order=None,
    eps=1e-12,                
    outfile=None,
    figsize=(12, 6),
    group_question=None,
    strip_left=0.84,
    strip_width=0.16,
    strip_fontsize=8,
):
    """
    Computes DID % change and plots it in a 3-column chart.
    
    For each metric m and comparison (A -> B):
      pct_change(period) = (B - A) / (A + eps)
      DID_pct = pct_change(treat_period) - pct_change(control_period)
    """
    
    if metrics is None:
        exclude = {period_col, "country_name"}
        metrics = [c for c in df.columns if c not in exclude]

    if metric_pretty is None:
        metric_pretty = {m: m for m in metrics}

    if metric_group is None:
        metric_group = {m: "Metrics" for m in metrics}

    if group_order is None:
        seen = []
        for m in metrics:
            g = metric_group.get(m, "Metrics")
            if g not in seen:
                seen.append(g)
        group_order = seen

    if group_question is None:
        group_question = {
            "Extensive Margin": (
                "Extensive margin:\n"
                "Did suppliers or tours\n"
                "enter/exit the platform after\n"
                "the change?"
            ),
            "Intensive Margin": (
                "Intensive margin:\n"
                "Among suppliers/tours that\n"
                "stayed, did supply intensity\n"
                "change\n"
                "(e.g availability, capacity)?"
            ),
            "Customer Response": (
                "Customer response:\n"
                "Did demand shift after\n"
                "the change?"
            ),
        }

    # Parse period_window format: "PRE_LY", "POST_CY", etc.
    df_copy = df.copy()
    
    # Extract window (PRE, POST, COMM) and period (LY, CY) from period_window
    df_copy['window'] = df_copy[period_col].str.split('_').str[0]
    df_copy['period'] = df_copy[period_col].str.split('_').str[1]
    
    # Validation
    needed_windows = {w for a, b, _ in comparisons for w in (a, b)}
    for p in (treat_period, control_period):
        sub = df_copy[df_copy['period'] == p]
        present = set(sub['window'].unique())
        missing = needed_windows - present
        if missing:
            raise ValueError(f"Missing windows for period={p}: {sorted(missing)}")

    treat = df_copy[df_copy['period'] == treat_period].set_index('window')
    ctrl  = df_copy[df_copy['period'] == control_period].set_index('window')

    # Compute DID % change
    did_rows = []
    for m in metrics:
        for a, b, label in comparisons:
            treat_a = treat.loc[a, m] if a in treat.index else 0
            treat_b = treat.loc[b, m] if b in treat.index else 0
            ctrl_a = ctrl.loc[a, m] if a in ctrl.index else 0
            ctrl_b = ctrl.loc[b, m] if b in ctrl.index else 0
            
            treat_pct = (treat_b - treat_a) / (abs(treat_a) + eps)
            ctrl_pct  = (ctrl_b - ctrl_a) / (abs(ctrl_a) + eps)
            did = treat_pct - ctrl_pct
            
            did_rows.append({
                "metric": m,
                "comparison": label,
                "did_pct": did,
                "group": metric_group.get(m, "Metrics"),
                "pretty": metric_pretty.get(m, m),
            })

    did_df = pd.DataFrame(did_rows)

    # Y order: group blocks, then metrics within group
    ordered_metrics = []
    for g in group_order:
        ordered_metrics.extend([m for m in metrics if metric_group.get(m, "Metrics") == g])

    y_map = {m: i for i, m in enumerate(ordered_metrics)}
    y_labels = [metric_pretty.get(m, m) for m in ordered_metrics]

    # Plot
    fig, axes = plt.subplots(1, 3, figsize=figsize, sharey=True)

    for ax, (_, _, title) in zip(axes, comparisons):
        sub = did_df[did_df["comparison"] == title].set_index("metric")
        xs = [sub.loc[m, "did_pct"] if m in sub.index else 0 for m in ordered_metrics]
        ys = [y_map[m] for m in ordered_metrics]

        ax.scatter(xs, ys, s=35, color="black", zorder=3)
        ax.axvline(0, color="red", linestyle="--", linewidth=1.5, zorder=2)
        ax.grid(True, axis="x", color="0.9")
        ax.set_title(title, fontsize=11, fontweight='bold')

    axes[0].set_yticks(range(len(ordered_metrics)))
    axes[0].set_yticklabels(y_labels)
    axes[0].invert_yaxis()

    # Group spans + separators
    group_spans = []
    start = 0
    for g in group_order:
        ms = [m for m in ordered_metrics if metric_group.get(m, "Metrics") == g]
        if not ms:
            continue
        end = start + len(ms) - 1
        group_spans.append((g, start, end))
        start = end + 1

    for ax in axes:
        for _, s, e in group_spans[:-1]:
            ax.axhline(e + 0.5, color="0.85", linewidth=2)

    # Right-side category strip
    strip_ax = fig.add_axes([strip_left, 0.12, strip_width, 0.78])
    strip_ax.set_xlim(0, 1)
    strip_ax.set_ylim(-0.5, len(ordered_metrics) - 0.5)
    strip_ax.invert_yaxis()
    strip_ax.axis("off")

    for g, s, e in group_spans:
        rect = Rectangle((0, s - 0.5), 1, (e - s + 1), facecolor="0.9", edgecolor="0.2")
        strip_ax.add_patch(rect)

        label = group_question.get(g, g)
        strip_ax.text(
            0.5, (s + e) / 2,
            label,
            ha="center", va="center",
            fontsize=strip_fontsize,
            linespacing=1.15,
            wrap=True,
            clip_on=True,
        )

    fig.supxlabel(
        f"DID % change = (({treat_period} (B−A)/A) − ({control_period} (B−A)/A))",
        fontsize=11
    )

    fig.tight_layout(rect=[0.05, 0.05, strip_left - 0.01, 0.95])

    if outfile:
        plt.savefig(outfile, dpi=200, bbox_inches="tight")

    return fig, axes, did_df

print("✓ Plotting function defined")
```

```python
# ============================================
# CELL 3: CREATE DASHBOARD WIDGETS
# ============================================
# Keep this cell visible for user interaction

dbutils.widgets.removeAll()

# Create dropdown for analysis type
dbutils.widgets.dropdown(
    "analysis_type", 
    "Supply Metrics", 
    ["Supply Metrics", "Price Parity", "Both"],
    "Analysis Type"
)

# Create dropdown for country filter (optional)
dbutils.widgets.dropdown(
    "country_filter",
    "All Countries",
    ["All Countries", "United States", "United Kingdom", "Germany", "France", "Spain", "Italy"],
    "Country Filter"
)

# Date range widgets
dbutils.widgets.text("yoy_pre_start", "2025-01-01", "YoY Pre Start")
dbutils.widgets.text("yoy_pre_end", "2025-01-15", "YoY Pre End")
dbutils.widgets.text("yoy_event_start", "2025-01-16", "YoY Event Start")
dbutils.widgets.text("yoy_post_end", "2025-01-30", "YoY Post End")

dbutils.widgets.text("event_pre_start", "2026-01-01", "Event Pre Start")
dbutils.widgets.text("event_pre_end", "2026-01-15", "Event Pre End")
dbutils.widgets.text("event_start", "2026-01-16", "Event Start")
dbutils.widgets.text("event_post_end", "2026-01-30", "Event Post End")

print("✓ Dashboard widgets created")
print(f"  Analysis Type: {dbutils.widgets.get('analysis_type')}")
print(f"  Country Filter: {dbutils.widgets.get('country_filter')}")
```

```python
# ============================================
# CELL 4: LOAD SUPPLY DATA
# ============================================
# Hide this cell in dashboard view

# Get widget values
analysis_type = dbutils.widgets.get("analysis_type")
country_filter = dbutils.widgets.get("country_filter")

# Load supply metrics data
supply_df = spark.table("production.supply_analytics.dst_supply_dataset").toPandas()

# Apply country filter if specified
if country_filter != "All Countries":
    supply_df = supply_df[supply_df['country_name'] == country_filter]

# Convert Decimal columns to float
for col in supply_df.columns:
    if supply_df[col].dtype == 'object':
        try:
            supply_df[col] = supply_df[col].astype(float)
        except (ValueError, TypeError):
            pass

print(f"✓ Supply data loaded: {len(supply_df)} rows")
print(f"  Periods: {sorted(supply_df['period_window'].unique())}")
print(f"  Countries: {sorted(supply_df['country_name'].unique())}")
```

```python
# ============================================
# CELL 5: LOAD PRICE PARITY DATA
# ============================================
# Hide this cell in dashboard view

if analysis_type in ["Price Parity", "Both"]:
    # Load price parity data
    parity_df = spark.table("production.supply_analytics.dst_price_parity").toPandas()
    
    # Apply country filter
    if country_filter != "All Countries":
        parity_df = parity_df[parity_df['country_name'] == country_filter]
    
    # Convert Decimal columns to float
    for col in parity_df.columns:
        if parity_df[col].dtype == 'object':
            try:
                parity_df[col] = parity_df[col].astype(float)
            except (ValueError, TypeError):
                pass
    
    print(f"✓ Price parity data loaded: {len(parity_df)} rows")
    print(f"  Periods: {sorted(parity_df['period_window'].unique())}")
else:
    parity_df = None
    print("⊘ Price parity data not loaded (analysis type: Supply Metrics only)")
```

```python
# ============================================
# CELL 6: PREPARE SUPPLY METRICS
# ============================================
# Hide this cell in dashboard view

# Aggregate supply data across countries if needed
supply_agg = supply_df.groupby('period_window').agg({
    'total_tours': 'sum',
    'active_tours': 'sum',
    'share_active_tours': 'mean',
    'total_suppliers': 'sum',
    'active_suppliers': 'sum',
    'share_active_suppliers': 'mean',
    # Add any other metrics you have
}).reset_index()

# Calculate additional metrics if they exist in your data
# This is a placeholder - adjust based on your actual column names
if 'avg_days_online_per_tour' not in supply_agg.columns:
    supply_agg['avg_days_online_per_tour'] = 0
if 'avg_days_online_per_active_tour' not in supply_agg.columns:
    supply_agg['avg_days_online_per_active_tour'] = 0
if 'avg_days_online_per_supplier' not in supply_agg.columns:
    supply_agg['avg_days_online_per_supplier'] = 0
if 'avg_days_online_per_active_supplier' not in supply_agg.columns:
    supply_agg['avg_days_online_per_active_supplier'] = 0

# Add customer response metrics (set to 0 if not available)
for metric in ['bookings', 'tickets', 'nr', 'gmv']:
    if metric not in supply_agg.columns:
        supply_agg[metric] = 0

print("✓ Supply metrics prepared")
print(f"  Columns: {list(supply_agg.columns)}")
```

```python
# ============================================
# CELL 7: PREPARE PRICE PARITY METRICS  
# ============================================
# Hide this cell in dashboard view

if parity_df is not None:
    # Aggregate parity data across countries if needed
    parity_agg = parity_df.groupby('period_window').agg({
        'total_tours': 'sum',
        'total_suppliers': 'sum',
        'supplier_parity_rate': 'mean',
        'tiqets_parity_rate': 'mean',
        'viator_parity_rate': 'mean',
        'headout_parity_rate': 'mean',
        'overall_parity_rate': 'mean',
        'total_impressions': 'sum',
    }).reset_index()
    
    print("✓ Price parity metrics prepared")
    print(f"  Columns: {list(parity_agg.columns)}")
else:
    parity_agg = None
```

```markdown
# Supply Metrics Analysis

This section shows the Difference-in-Differences (DID) analysis for supply-side metrics, organized by:
- **Extensive Margin**: Changes in the number of tours and suppliers on the platform
- **Intensive Margin**: Changes in supplier activity and availability
- **Customer Response**: Changes in bookings, tickets, revenue, and GMV
```

```python
# ============================================
# CELL 8: PLOT SUPPLY METRICS
# ============================================
# Show output only - this is the main visualization

if analysis_type in ["Supply Metrics", "Both"]:
    
    fig, axes, did_df = plot_did_percent_change_three_comparisons(
        supply_agg,
        period_col="period_window",
        metrics=[
            "total_tours",
            "active_tours",
            "share_active_tours",
            "total_suppliers",
            "active_suppliers",
            "share_active_suppliers",
            "avg_days_online_per_tour",
            "avg_days_online_per_active_tour",
            "avg_days_online_per_supplier",
            "avg_days_online_per_active_supplier",
            "bookings",
            "tickets",
            "nr",
            "gmv"
        ],
        metric_pretty={
            "total_tours": "Total tours",
            "active_tours": "Active tours",
            "share_active_tours": "Share active tours",
            "total_suppliers": "Total suppliers",
            "active_suppliers": "Active suppliers",
            "share_active_suppliers": "Share active suppliers",
            "avg_days_online_per_tour": "Avg days online / tour",
            "avg_days_online_per_active_tour": "Avg days online / active tour",
            "avg_days_online_per_supplier": "Avg days online / supplier",
            "avg_days_online_per_active_supplier": "Avg days online / active supplier",
            "bookings": "Bookings",
            "tickets": "Tickets",
            "nr": "Net Revenue",
            "gmv": "GMV"
        },
        metric_group={
            "total_tours": "Extensive Margin",
            "active_tours": "Extensive Margin",
            "share_active_tours": "Extensive Margin",
            "total_suppliers": "Extensive Margin",
            "active_suppliers": "Extensive Margin",
            "share_active_suppliers": "Extensive Margin",
            "avg_days_online_per_tour": "Intensive Margin",
            "avg_days_online_per_active_tour": "Intensive Margin",
            "avg_days_online_per_supplier": "Intensive Margin",
            "avg_days_online_per_active_supplier": "Intensive Margin",
            "bookings": "Customer Response",
            "tickets": "Customer Response",
            "nr": "Customer Response",
            "gmv": "Customer Response"
        },
        group_order=["Extensive Margin", "Intensive Margin", "Customer Response"],
        strip_width=0.2,
        strip_fontsize=10,
        figsize=(14, 8)
    )
    
    plt.suptitle(
        f"Supply Metrics DID Analysis - {country_filter}",
        fontsize=14,
        fontweight='bold',
        y=0.98
    )
    
    plt.show()
    
    print(f"\n✓ Supply metrics visualization complete")
```

```python
# ============================================
# CELL 9: DISPLAY SUPPLY METRICS TABLE
# ============================================
# Show output only

if analysis_type in ["Supply Metrics", "Both"]:
    # Display the DID results as a table
    did_pivot = did_df.pivot(index='pretty', columns='comparison', values='did_pct')
    did_pivot = did_pivot.round(4)
    
    display(did_pivot)
    
    print("\n📊 Supply Metrics DID Results Table")
    print("Values represent the difference-in-differences percentage change")
```

```markdown
# Price Parity Analysis

This section shows price parity metrics across different competitors:
- **Supplier Parity**: Direct supplier website comparisons
- **Tiqets Parity**: Tiqets platform comparisons
- **Viator Parity**: Viator platform comparisons  
- **Headout Parity**: Headout platform comparisons
- **Overall Parity**: Combined parity across all sources
```

```python
# ============================================
# CELL 10: PLOT PRICE PARITY METRICS
# ============================================
# Show output only

if analysis_type in ["Price Parity", "Both"] and parity_agg is not None:
    
    fig, axes, parity_did_df = plot_did_percent_change_three_comparisons(
        parity_agg,
        period_col="period_window",
        metrics=[
            "total_tours",
            "total_suppliers",
            "supplier_parity_rate",
            "tiqets_parity_rate",
            "viator_parity_rate",
            "headout_parity_rate",
            "overall_parity_rate",
            "total_impressions"
        ],
        metric_pretty={
            "total_tours": "Total Tours",
            "total_suppliers": "Total Suppliers",
            "supplier_parity_rate": "Supplier Parity Rate",
            "tiqets_parity_rate": "Tiqets Parity Rate",
            "viator_parity_rate": "Viator Parity Rate",
            "headout_parity_rate": "Headout Parity Rate",
            "overall_parity_rate": "Overall Parity Rate",
            "total_impressions": "Total Impressions"
        },
        metric_group={
            "total_tours": "Coverage",
            "total_suppliers": "Coverage",
            "supplier_parity_rate": "Price Parity",
            "tiqets_parity_rate": "Price Parity",
            "viator_parity_rate": "Price Parity",
            "headout_parity_rate": "Price Parity",
            "overall_parity_rate": "Price Parity",
            "total_impressions": "Exposure"
        },
        group_order=["Coverage", "Price Parity", "Exposure"],
        group_question={
            "Coverage": "Coverage:\nHow many tours/suppliers\nare included in price\ncomparison?",
            "Price Parity": "Price Parity:\nAre we meeting or\nbeating competitor\nprices?",
            "Exposure": "Exposure:\nHow much visibility\ndo these comparisons\nget?"
        },
        strip_width=0.2,
        strip_fontsize=10,
        figsize=(14, 8)
    )
    
    plt.suptitle(
        f"Price Parity DID Analysis - {country_filter}",
        fontsize=14,
        fontweight='bold',
        y=0.98
    )
    
    plt.show()
    
    print(f"\n✓ Price parity visualization complete")
```

```python
# ============================================
# CELL 11: DISPLAY PRICE PARITY TABLE
# ============================================
# Show output only

if analysis_type in ["Price Parity", "Both"] and parity_agg is not None:
    # Display the DID results as a table
    parity_did_pivot = parity_did_df.pivot(index='pretty', columns='comparison', values='did_pct')
    parity_did_pivot = parity_did_pivot.round(4)
    
    display(parity_did_pivot)
    
    print("\n📊 Price Parity DID Results Table")
    print("Values represent the difference-in-differences percentage change")
```

```markdown
# Summary Statistics

Key statistics from the analysis period.
```

```python
# ============================================
# CELL 12: SUMMARY STATISTICS
# ============================================
# Show output only

print("=" * 60)
print("SUMMARY STATISTICS")
print("=" * 60)

if analysis_type in ["Supply Metrics", "Both"]:
    print("\n📈 SUPPLY METRICS SUMMARY")
    print("-" * 60)
    
    for period in sorted(supply_agg['period_window'].unique()):
        period_data = supply_agg[supply_agg['period_window'] == period].iloc[0]
        print(f"\n{period}:")
        print(f"  Total Tours: {period_data['total_tours']:,.0f}")
        print(f"  Active Tours: {period_data['active_tours']:,.0f}")
        print(f"  Total Suppliers: {period_data['total_suppliers']:,.0f}")
        print(f"  Active Suppliers: {period_data['active_suppliers']:,.0f}")

if analysis_type in ["Price Parity", "Both"] and parity_agg is not None:
    print("\n\n💰 PRICE PARITY SUMMARY")
    print("-" * 60)
    
    for period in sorted(parity_agg['period_window'].unique()):
        period_data = parity_agg[parity_agg['period_window'] == period].iloc[0]
        print(f"\n{period}:")
        print(f"  Total Tours: {period_data['total_tours']:,.0f}")
        print(f"  Overall Parity Rate: {period_data['overall_parity_rate']:.2%}")
        print(f"  Total Impressions: {period_data['total_impressions']:,.0f}")

print("\n" + "=" * 60)
```

```markdown
---

## How to Use This Dashboard

### Dashboard View
1. Click **View** → **Dashboard** in the notebook toolbar to enter presentation mode
2. All code will be hidden, showing only visualizations and widgets
3. Use the widgets at the top to filter and customize the analysis

### Widgets
- **Analysis Type**: Choose between Supply Metrics, Price Parity, or Both
- **Country Filter**: Filter analysis to a specific country or view all countries
- **Date Range**: Adjust the analysis periods (requires re-running the notebook)

### Interpreting DID Results
- **Positive values**: The metric increased more (or decreased less) in the treatment period compared to control
- **Negative values**: The metric decreased more (or increased less) in the treatment period compared to control
- **Red dashed line**: No difference between treatment and control

### Refreshing Data
To update the analysis with new data:
1. Click **Run All** in the notebook toolbar
2. Or schedule this notebook as a Databricks Job for automatic updates

---
**Note**: Hide code cells before sharing by right-clicking each cell → "Hide Code"
```