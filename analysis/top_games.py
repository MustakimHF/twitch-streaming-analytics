#!/usr/bin/env python3
"""
Top Games Analysis
Analyzes the most popular games by average viewers and generates visualizations.
"""
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Set up paths
ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "db" / "twitch.db"
OUTPUT_DIR = ROOT / "outputs" / "plots"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Set style for better-looking plots
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

def analyze_top_games():
    """Analyze top games by average viewers and generate plots."""
    
    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    
    # Query for top games by average viewers
    query = """
    SELECT 
        game_name,
        COUNT(*) as stream_count,
        AVG(viewer_count) as avg_viewers,
        SUM(viewer_count) as total_viewers,
        MAX(viewer_count) as max_viewers
    FROM streams 
    WHERE game_name != 'Unknown'
    GROUP BY game_name 
    HAVING COUNT(*) >= 5  -- Only games with at least 5 streams
    ORDER BY avg_viewers DESC 
    LIMIT 15
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        print("No data found for analysis.")
        return
    
    print("=== Top Games Analysis ===")
    print(f"Analyzing {len(df)} games with at least 5 streams")
    print("\nTop 10 Games by Average Viewers:")
    print(df[['game_name', 'avg_viewers', 'stream_count']].head(10).to_string(index=False))
    
    # Create the main visualization
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 12))
    
    # Plot 1: Top 10 games by average viewers
    top_10 = df.head(10)
    bars1 = ax1.barh(range(len(top_10)), top_10['avg_viewers'], color='skyblue', alpha=0.8)
    ax1.set_yticks(range(len(top_10)))
    ax1.set_yticklabels(top_10['game_name'], fontsize=10)
    ax1.set_xlabel('Average Viewers', fontsize=12, fontweight='bold')
    ax1.set_title('Top 10 Games by Average Viewers', fontsize=14, fontweight='bold', pad=20)
    ax1.grid(axis='x', alpha=0.3)
    
    # Add value labels on bars
    for i, (bar, value) in enumerate(zip(bars1, top_10['avg_viewers'])):
        ax1.text(value + max(top_10['avg_viewers']) * 0.01, bar.get_y() + bar.get_height()/2, 
                f'{value:,.0f}', va='center', fontsize=9, fontweight='bold')
    
    # Plot 2: Top 10 games by total viewers
    top_10_total = df.nlargest(10, 'total_viewers')
    bars2 = ax2.barh(range(len(top_10_total)), top_10_total['total_viewers'], color='lightcoral', alpha=0.8)
    ax2.set_yticks(range(len(top_10_total)))
    ax2.set_yticklabels(top_10_total['game_name'], fontsize=10)
    ax2.set_xlabel('Total Viewers', fontsize=12, fontweight='bold')
    ax2.set_title('Top 10 Games by Total Viewers', fontsize=14, fontweight='bold', pad=20)
    ax2.grid(axis='x', alpha=0.3)
    
    # Add value labels on bars
    for i, (bar, value) in enumerate(zip(bars2, top_10_total['total_viewers'])):
        ax2.text(value + max(top_10_total['total_viewers']) * 0.01, bar.get_y() + bar.get_height()/2, 
                f'{value:,.0f}', va='center', fontsize=9, fontweight='bold')
    
    plt.tight_layout()
    
    # Save the plot
    output_path = OUTPUT_DIR / "top_games.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"\n✅ Plot saved to: {output_path}")
    
    # Create a summary statistics plot
    fig2, ax3 = plt.subplots(1, 1, figsize=(12, 8))
    
    # Scatter plot: Stream count vs Average viewers
    scatter = ax3.scatter(df['stream_count'], df['avg_viewers'], 
                         s=df['total_viewers']/1000, alpha=0.6, c=df['total_viewers'], 
                         cmap='viridis', edgecolors='black', linewidth=0.5)
    
    ax3.set_xlabel('Number of Streams', fontsize=12, fontweight='bold')
    ax3.set_ylabel('Average Viewers', fontsize=12, fontweight='bold')
    ax3.set_title('Game Popularity: Stream Count vs Average Viewers\n(Bubble size = Total Viewers)', 
                  fontsize=14, fontweight='bold', pad=20)
    ax3.grid(True, alpha=0.3)
    
    # Add colorbar
    cbar = plt.colorbar(scatter, ax=ax3)
    cbar.set_label('Total Viewers', fontsize=12, fontweight='bold')
    
    # Add labels for top games
    for idx, row in df.head(5).iterrows():
        ax3.annotate(row['game_name'], 
                    (row['stream_count'], row['avg_viewers']),
                    xytext=(5, 5), textcoords='offset points',
                    fontsize=8, alpha=0.8)
    
    plt.tight_layout()
    
    # Save the scatter plot
    output_path2 = OUTPUT_DIR / "game_popularity_analysis.png"
    plt.savefig(output_path2, dpi=300, bbox_inches='tight')
    print(f"✅ Scatter plot saved to: {output_path2}")
    
    # Show plots
    plt.show()
    
    # Print summary statistics
    print(f"\n=== Summary Statistics ===")
    print(f"Total unique games analyzed: {len(df)}")
    print(f"Total streams analyzed: {df['stream_count'].sum()}")
    print(f"Total viewers across all streams: {df['total_viewers'].sum():,}")
    print(f"Average viewers per game: {df['avg_viewers'].mean():.0f}")
    print(f"Most popular game by average viewers: {df.iloc[0]['game_name']} ({df.iloc[0]['avg_viewers']:,.0f} avg viewers)")

if __name__ == "__main__":
    analyze_top_games()
