#!/usr/bin/env python3
"""
Weekend vs Weekday Analysis
Analyzes viewership patterns comparing weekends vs weekdays and generates visualizations.
"""
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import numpy as np

# Set up paths
ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "db" / "twitch.db"
OUTPUT_DIR = ROOT / "outputs" / "plots"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Set style for better-looking plots
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

def analyze_weekend_patterns():
    """Analyze viewership patterns comparing weekends vs weekdays."""
    
    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    
    # Query for weekend vs weekday analysis
    query = """
    SELECT 
        weekday,
        is_weekend,
        COUNT(*) as stream_count,
        AVG(viewer_count) as avg_viewers,
        SUM(viewer_count) as total_viewers,
        MAX(viewer_count) as max_viewers,
        MIN(viewer_count) as min_viewers
    FROM streams 
    WHERE weekday IS NOT NULL AND is_weekend IS NOT NULL
    GROUP BY weekday, is_weekend
    ORDER BY 
        CASE weekday
            WHEN 'Monday' THEN 1
            WHEN 'Tuesday' THEN 2
            WHEN 'Wednesday' THEN 3
            WHEN 'Thursday' THEN 4
            WHEN 'Friday' THEN 5
            WHEN 'Saturday' THEN 6
            WHEN 'Sunday' THEN 7
        END
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        print("No data found for analysis.")
        return
    
    print("=== Weekend vs Weekday Analysis ===")
    print(f"Analyzing viewership patterns across {len(df)} day categories")
    
    # Calculate weekend vs weekday aggregates
    weekend_data = df[df['is_weekend'] == 1]
    weekday_data = df[df['is_weekend'] == 0]
    
    weekend_avg = weekend_data['avg_viewers'].mean()
    weekday_avg = weekday_data['avg_viewers'].mean()
    weekend_total = weekend_data['total_viewers'].sum()
    weekday_total = weekday_data['total_viewers'].sum()
    
    print(f"\nWeekend Average Viewers: {weekend_avg:,.0f}")
    print(f"Weekday Average Viewers: {weekday_avg:,.0f}")
    print(f"Weekend vs Weekday Difference: {((weekend_avg - weekday_avg) / weekday_avg * 100):+.1f}%")
    
    # Create visualizations
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # Plot 1: Average viewers by day of week
    days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    df_ordered = df.set_index('weekday').reindex(days_order).reset_index()
    
    colors = ['#FF6B6B' if not is_weekend else '#4ECDC4' for is_weekend in df_ordered['is_weekend']]
    bars1 = ax1.bar(df_ordered['weekday'], df_ordered['avg_viewers'], color=colors, alpha=0.8, edgecolor='black', linewidth=0.5)
    ax1.set_xlabel('Day of Week', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Average Viewers', fontsize=12, fontweight='bold')
    ax1.set_title('Average Viewers by Day of Week', fontsize=14, fontweight='bold')
    ax1.grid(True, alpha=0.3, axis='y')
    ax1.tick_params(axis='x', rotation=45)
    
    # Add value labels on bars
    for bar, value in zip(bars1, df_ordered['avg_viewers']):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(df_ordered['avg_viewers']) * 0.01, 
                f'{value:,.0f}', ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    # Add legend
    from matplotlib.patches import Patch
    legend_elements = [Patch(facecolor='#FF6B6B', alpha=0.8, label='Weekday'),
                      Patch(facecolor='#4ECDC4', alpha=0.8, label='Weekend')]
    ax1.legend(handles=legend_elements, loc='upper right')
    
    # Plot 2: Total viewers by day of week
    bars2 = ax2.bar(df_ordered['weekday'], df_ordered['total_viewers'], color=colors, alpha=0.8, edgecolor='black', linewidth=0.5)
    ax2.set_xlabel('Day of Week', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Total Viewers', fontsize=12, fontweight='bold')
    ax2.set_title('Total Viewers by Day of Week', fontsize=14, fontweight='bold')
    ax2.grid(True, alpha=0.3, axis='y')
    ax2.tick_params(axis='x', rotation=45)
    
    # Add value labels on bars
    for bar, value in zip(bars2, df_ordered['total_viewers']):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(df_ordered['total_viewers']) * 0.01, 
                f'{value:,.0f}', ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    # Plot 3: Stream count by day of week
    bars3 = ax3.bar(df_ordered['weekday'], df_ordered['stream_count'], color=colors, alpha=0.8, edgecolor='black', linewidth=0.5)
    ax3.set_xlabel('Day of Week', fontsize=12, fontweight='bold')
    ax3.set_ylabel('Number of Streams', fontsize=12, fontweight='bold')
    ax3.set_title('Number of Streams by Day of Week', fontsize=14, fontweight='bold')
    ax3.grid(True, alpha=0.3, axis='y')
    ax3.tick_params(axis='x', rotation=45)
    
    # Add value labels on bars
    for bar, value in zip(bars3, df_ordered['stream_count']):
        ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(df_ordered['stream_count']) * 0.01, 
                f'{value:,.0f}', ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    # Plot 4: Weekend vs Weekday comparison
    categories = ['Weekday', 'Weekend']
    avg_viewers = [weekday_avg, weekend_avg]
    total_viewers = [weekday_total, weekend_total]
    
    x = np.arange(len(categories))
    width = 0.35
    
    bars4a = ax4.bar(x - width/2, avg_viewers, width, label='Average Viewers', color='#FF6B6B', alpha=0.8)
    ax4_twin = ax4.twinx()
    bars4b = ax4_twin.bar(x + width/2, total_viewers, width, label='Total Viewers', color='#4ECDC4', alpha=0.8)
    
    ax4.set_xlabel('Day Type', fontsize=12, fontweight='bold')
    ax4.set_ylabel('Average Viewers', fontsize=12, fontweight='bold', color='#FF6B6B')
    ax4_twin.set_ylabel('Total Viewers', fontsize=12, fontweight='bold', color='#4ECDC4')
    ax4.set_title('Weekend vs Weekday: Average vs Total Viewers', fontsize=14, fontweight='bold')
    ax4.set_xticks(x)
    ax4.set_xticklabels(categories)
    ax4.grid(True, alpha=0.3, axis='y')
    
    # Add value labels
    for bar, value in zip(bars4a, avg_viewers):
        ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(avg_viewers) * 0.01, 
                f'{value:,.0f}', ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    for bar, value in zip(bars4b, total_viewers):
        ax4_twin.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(total_viewers) * 0.01, 
                     f'{value:,.0f}', ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    # Combine legends
    lines1, labels1 = ax4.get_legend_handles_labels()
    lines2, labels2 = ax4_twin.get_legend_handles_labels()
    ax4.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
    
    plt.tight_layout()
    
    # Save the plot
    output_path = OUTPUT_DIR / "weekend_analysis.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"\n✅ Plot saved to: {output_path}")
    
    # Create a detailed day-by-day breakdown
    print(f"\n=== Day-by-Day Breakdown ===")
    day_breakdown = df_ordered[['weekday', 'is_weekend', 'avg_viewers', 'total_viewers', 'stream_count']].copy()
    day_breakdown['day_type'] = day_breakdown['is_weekend'].map({0: 'Weekday', 1: 'Weekend'})
    print(day_breakdown[['weekday', 'day_type', 'avg_viewers', 'total_viewers', 'stream_count']].to_string(
        index=False, 
        formatters={
            'avg_viewers': '{:,.0f}'.format,
            'total_viewers': '{:,.0f}'.format,
            'stream_count': '{:,.0f}'.format
        }
    ))
    
    # Create a summary statistics plot
    fig2, ax5 = plt.subplots(1, 1, figsize=(10, 6))
    
    # Create a comparison chart
    metrics = ['Average Viewers', 'Total Viewers', 'Stream Count']
    weekday_values = [weekday_avg, weekday_total, weekday_data['stream_count'].sum()]
    weekend_values = [weekend_avg, weekend_total, weekend_data['stream_count'].sum()]
    
    x = np.arange(len(metrics))
    width = 0.35
    
    bars5a = ax5.bar(x - width/2, weekday_values, width, label='Weekday', color='#FF6B6B', alpha=0.8)
    bars5b = ax5.bar(x + width/2, weekend_values, width, label='Weekend', color='#4ECDC4', alpha=0.8)
    
    ax5.set_xlabel('Metrics', fontsize=12, fontweight='bold')
    ax5.set_ylabel('Values', fontsize=12, fontweight='bold')
    ax5.set_title('Weekend vs Weekday: Comprehensive Comparison', fontsize=14, fontweight='bold')
    ax5.set_xticks(x)
    ax5.set_xticklabels(metrics)
    ax5.legend()
    ax5.grid(True, alpha=0.3, axis='y')
    
    # Add value labels
    for bars in [bars5a, bars5b]:
        for bar in bars:
            height = bar.get_height()
            ax5.text(bar.get_x() + bar.get_width()/2, height + max(max(weekday_values), max(weekend_values)) * 0.01, 
                    f'{height:,.0f}', ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    plt.tight_layout()
    
    # Save the summary plot
    output_path2 = OUTPUT_DIR / "weekend_vs_weekday_summary.png"
    plt.savefig(output_path2, dpi=300, bbox_inches='tight')
    print(f"✅ Summary plot saved to: {output_path2}")
    
    # Show plots
    plt.show()
    
    # Print insights
    print(f"\n=== Key Insights ===")
    best_weekday = df_ordered.loc[df_ordered[df_ordered['is_weekend'] == 0]['avg_viewers'].idxmax(), 'weekday']
    best_weekend = df_ordered.loc[df_ordered[df_ordered['is_weekend'] == 1]['avg_viewers'].idxmax(), 'weekday']
    worst_weekday = df_ordered.loc[df_ordered[df_ordered['is_weekend'] == 0]['avg_viewers'].idxmin(), 'weekday']
    worst_weekend = df_ordered.loc[df_ordered[df_ordered['is_weekend'] == 1]['avg_viewers'].idxmin(), 'weekday']
    
    print(f"• Best weekday for viewership: {best_weekday}")
    print(f"• Best weekend day for viewership: {best_weekend}")
    print(f"• Worst weekday for viewership: {worst_weekday}")
    print(f"• Worst weekend day for viewership: {worst_weekend}")
    print(f"• Weekend advantage: {((weekend_avg - weekday_avg) / weekday_avg * 100):+.1f}% higher average viewers")
    print(f"• Total weekend streams: {weekend_data['stream_count'].sum():,}")
    print(f"• Total weekday streams: {weekday_data['stream_count'].sum():,}")

if __name__ == "__main__":
    analyze_weekend_patterns()
