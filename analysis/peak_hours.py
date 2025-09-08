#!/usr/bin/env python3
"""
Peak Hours Analysis
Analyses viewership patterns by hour of day and generates visualisations.
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

def analyse_peak_hours():
    """Analyse viewership patterns by hour of day."""
    
    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    
    # Query for hourly analysis
    query = """
    SELECT 
        hour_of_day,
        COUNT(*) as stream_count,
        AVG(viewer_count) as avg_viewers,
        SUM(viewer_count) as total_viewers,
        MAX(viewer_count) as max_viewers,
        MIN(viewer_count) as min_viewers
    FROM streams 
    WHERE hour_of_day IS NOT NULL
    GROUP BY hour_of_day 
    ORDER BY hour_of_day
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        print("No data found for analysis.")
        return
    
    print("=== Peak Hours Analysis ===")
    print(f"Analysing viewership patterns across {len(df)} hours")
    
    # Find peak hours
    peak_hour_avg = df.loc[df['avg_viewers'].idxmax()]
    peak_hour_total = df.loc[df['total_viewers'].idxmax()]
    
    print(f"\nPeak hour by average viewers: {int(peak_hour_avg['hour_of_day'])}:00 ({peak_hour_avg['avg_viewers']:,.0f} avg viewers)")
    print(f"Peak hour by total viewers: {int(peak_hour_total['hour_of_day'])}:00 ({peak_hour_total['total_viewers']:,.0f} total viewers)")
    
    # Create visualisations
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # Plot 1: Average viewers by hour
    hours = df['hour_of_day'].astype(int)
    ax1.plot(hours, df['avg_viewers'], marker='o', linewidth=2, markersize=6, color='#2E86AB')
    ax1.fill_between(hours, df['avg_viewers'], alpha=0.3, color='#2E86AB')
    ax1.set_xlabel('Hour of Day (UTC)', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Average Viewers', fontsize=12, fontweight='bold')
    ax1.set_title('Average Viewers by Hour of Day', fontsize=14, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    ax1.set_xticks(range(0, 24, 2))
    
    # Highlight peak hour
    peak_hour_idx = df['avg_viewers'].idxmax()
    ax1.axvline(x=df.loc[peak_hour_idx, 'hour_of_day'], color='red', linestyle='--', alpha=0.7)
    ax1.annotate(f'Peak: {int(df.loc[peak_hour_idx, "hour_of_day"])}:00', 
                xy=(df.loc[peak_hour_idx, 'hour_of_day'], df.loc[peak_hour_idx, 'avg_viewers']),
                xytext=(10, 10), textcoords='offset points',
                bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.7),
                arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0'))
    
    # Plot 2: Total viewers by hour
    ax2.bar(hours, df['total_viewers'], color='#A23B72', alpha=0.7, edgecolor='black', linewidth=0.5)
    ax2.set_xlabel('Hour of Day (UTC)', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Total Viewers', fontsize=12, fontweight='bold')
    ax2.set_title('Total Viewers by Hour of Day', fontsize=14, fontweight='bold')
    ax2.grid(True, alpha=0.3, axis='y')
    ax2.set_xticks(range(0, 24, 2))
    
    # Highlight peak hour
    peak_hour_total_idx = df['total_viewers'].idxmax()
    ax2.axvline(x=df.loc[peak_hour_total_idx, 'hour_of_day'], color='red', linestyle='--', alpha=0.7)
    
    # Plot 3: Stream count by hour
    ax3.bar(hours, df['stream_count'], color='#F18F01', alpha=0.7, edgecolor='black', linewidth=0.5)
    ax3.set_xlabel('Hour of Day (UTC)', fontsize=12, fontweight='bold')
    ax3.set_ylabel('Number of Streams', fontsize=12, fontweight='bold')
    ax3.set_title('Number of Streams by Hour of Day', fontsize=14, fontweight='bold')
    ax3.grid(True, alpha=0.3, axis='y')
    ax3.set_xticks(range(0, 24, 2))
    
    # Plot 4: Heatmap of viewers by hour (if we had more data)
    # Create a simple distribution plot
    ax4.hist(df['avg_viewers'], bins=20, color='#C73E1D', alpha=0.7, edgecolor='black')
    ax4.set_xlabel('Average Viewers', fontsize=12, fontweight='bold')
    ax4.set_ylabel('Frequency (Hours)', fontsize=12, fontweight='bold')
    ax4.set_title('Distribution of Average Viewers Across Hours', fontsize=14, fontweight='bold')
    ax4.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    
    # Save the plot
    output_path = OUTPUT_DIR / "peak_hours_analysis.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"\n✅ Plot saved to: {output_path}")
    
    # Create a detailed hourly breakdown table
    print(f"\n=== Hourly Breakdown (Top 10 Hours by Average Viewers) ===")
    top_hours = df.nlargest(10, 'avg_viewers')[['hour_of_day', 'avg_viewers', 'total_viewers', 'stream_count']]
    top_hours['hour_of_day'] = top_hours['hour_of_day'].astype(int)
    print(top_hours.to_string(index=False, formatters={
        'avg_viewers': '{:,.0f}'.format,
        'total_viewers': '{:,.0f}'.format,
        'stream_count': '{:,.0f}'.format
    }))
    
    # Create a summary plot showing peak vs off-peak
    fig2, ax5 = plt.subplots(1, 1, figsize=(12, 6))
    
    # Categorise hours into peak/off-peak
    df['category'] = df['avg_viewers'].apply(
        lambda x: 'Peak Hours' if x > df['avg_viewers'].quantile(0.75) 
        else 'Off-Peak Hours' if x < df['avg_viewers'].quantile(0.25)
        else 'Normal Hours'
    )
    
    # Create box plot
    categories = ['Off-Peak Hours', 'Normal Hours', 'Peak Hours']
    colors = ['#FF6B6B', '#4ECDC4', '#45B7D1']
    
    data_by_category = [df[df['category'] == cat]['avg_viewers'].values for cat in categories]
    
    bp = ax5.boxplot(data_by_category, labels=categories, patch_artist=True)
    for patch, color in zip(bp['boxes'], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)
    
    ax5.set_ylabel('Average Viewers', fontsize=12, fontweight='bold')
    ax5.set_title('Viewership Distribution: Peak vs Off-Peak Hours', fontsize=14, fontweight='bold')
    ax5.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    
    # Save the summary plot
    output_path2 = OUTPUT_DIR / "peak_vs_offpeak_analysis.png"
    plt.savefig(output_path2, dpi=300, bbox_inches='tight')
    print(f"✅ Summary plot saved to: {output_path2}")
    
    # Show plots
    plt.show()
    
    # Print insights
    print(f"\n=== Key Insights ===")
    print(f"• Peak viewing time: {int(peak_hour_avg['hour_of_day'])}:00 UTC")
    print(f"• Lowest viewing time: {int(df.loc[df['avg_viewers'].idxmin(), 'hour_of_day'])}:00 UTC")
    print(f"• Peak hours (top 25%): {df[df['category'] == 'Peak Hours']['hour_of_day'].astype(int).tolist()}")
    print(f"• Off-peak hours (bottom 25%): {df[df['category'] == 'Off-Peak Hours']['hour_of_day'].astype(int).tolist()}")
    print(f"• Average viewers during peak hours: {df[df['category'] == 'Peak Hours']['avg_viewers'].mean():,.0f}")
    print(f"• Average viewers during off-peak hours: {df[df['category'] == 'Off-Peak Hours']['avg_viewers'].mean():,.0f}")

if __name__ == "__main__":
    analyse_peak_hours()
