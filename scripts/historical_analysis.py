#!/usr/bin/env python3
"""
Historical Data Analysis for Twitch Streaming Analytics
Provides comprehensive analysis of accumulated historical data.
"""
from pathlib import Path
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "db" / "twitch.db"
OUTPUT_DIR = ROOT / "outputs" / "historical"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

class HistoricalAnalyzer:
    """Analyzes historical Twitch streaming data."""
    
    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH)
    
    def get_data_summary(self):
        """Get comprehensive summary of historical data."""
        # Check if ingested_at column exists
        columns_result = self.conn.execute("PRAGMA table_info(streams)")
        columns = [row[1] for row in columns_result.fetchall()]
        has_ingested_at = 'ingested_at' in columns
        
        if has_ingested_at:
            query = """
            SELECT 
                COUNT(*) as total_streams,
                COUNT(DISTINCT DATE(started_at)) as unique_days,
                MIN(DATE(started_at)) as earliest_date,
                MAX(DATE(started_at)) as latest_date,
                COUNT(DISTINCT user_id) as unique_streamers,
                COUNT(DISTINCT game_id) as unique_games,
                COUNT(DISTINCT language) as unique_languages,
                SUM(viewer_count) as total_viewers,
                AVG(viewer_count) as avg_viewers_per_stream,
                MIN(ingested_at) as first_ingestion,
                MAX(ingested_at) as last_ingestion
            FROM streams
            """
        else:
            query = """
            SELECT 
                COUNT(*) as total_streams,
                COUNT(DISTINCT DATE(started_at)) as unique_days,
                MIN(DATE(started_at)) as earliest_date,
                MAX(DATE(started_at)) as latest_date,
                COUNT(DISTINCT user_id) as unique_streamers,
                COUNT(DISTINCT game_id) as unique_games,
                COUNT(DISTINCT language) as unique_languages,
                SUM(viewer_count) as total_viewers,
                AVG(viewer_count) as avg_viewers_per_stream,
                'N/A' as first_ingestion,
                'N/A' as last_ingestion
            FROM streams
            """
        
        summary = pd.read_sql_query(query, self.conn)
        return summary.iloc[0] if not summary.empty else None
    
    def get_daily_trends(self):
        """Get daily trends over time."""
        query = """
        SELECT 
            DATE(started_at) as date,
            COUNT(*) as daily_streams,
            SUM(viewer_count) as daily_viewers,
            AVG(viewer_count) as avg_viewers,
            COUNT(DISTINCT user_id) as unique_streamers,
            COUNT(DISTINCT game_id) as unique_games
        FROM streams
        GROUP BY DATE(started_at)
        ORDER BY date
        """
        
        return pd.read_sql_query(query, self.conn)
    
    def get_top_games_historical(self, limit=20):
        """Get top games across all historical data."""
        query = """
        SELECT 
            game_name,
            COUNT(*) as total_streams,
            SUM(viewer_count) as total_viewers,
            AVG(viewer_count) as avg_viewers,
            COUNT(DISTINCT user_id) as unique_streamers,
            COUNT(DISTINCT DATE(started_at)) as days_featured
        FROM streams
        WHERE game_name != 'Unknown'
        GROUP BY game_name
        ORDER BY total_viewers DESC
        LIMIT ?
        """
        
        return pd.read_sql_query(query, self.conn, params=[limit])
    
    def get_top_streamers_historical(self, limit=20):
        """Get top streamers across all historical data."""
        query = """
        SELECT 
            user_name,
            user_login,
            COUNT(*) as total_streams,
            SUM(viewer_count) as total_viewers,
            AVG(viewer_count) as avg_viewers,
            MAX(viewer_count) as max_viewers,
            COUNT(DISTINCT DATE(started_at)) as days_active
        FROM streams
        GROUP BY user_id, user_name, user_login
        ORDER BY total_viewers DESC
        LIMIT ?
        """
        
        return pd.read_sql_query(query, self.conn, params=[limit])
    
    def get_hourly_patterns_historical(self):
        """Get hourly patterns across all historical data."""
        query = """
        SELECT 
            hour_of_day,
            COUNT(*) as total_streams,
            SUM(viewer_count) as total_viewers,
            AVG(viewer_count) as avg_viewers
        FROM streams
        WHERE hour_of_day IS NOT NULL
        GROUP BY hour_of_day
        ORDER BY hour_of_day
        """
        
        return pd.read_sql_query(query, self.conn)
    
    def get_weekend_vs_weekday_historical(self):
        """Get weekend vs weekday patterns across all historical data."""
        query = """
        SELECT 
            CASE WHEN is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END as day_type,
            COUNT(*) as total_streams,
            SUM(viewer_count) as total_viewers,
            AVG(viewer_count) as avg_viewers,
            COUNT(DISTINCT user_id) as unique_streamers
        FROM streams
        GROUP BY is_weekend
        ORDER BY day_type
        """
        
        return pd.read_sql_query(query, self.conn)
    
    def get_language_distribution_historical(self):
        """Get language distribution across all historical data."""
        query = """
        SELECT 
            language,
            COUNT(*) as total_streams,
            SUM(viewer_count) as total_viewers,
            AVG(viewer_count) as avg_viewers,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM streams), 2) as percentage
        FROM streams
        WHERE language IS NOT NULL
        GROUP BY language
        ORDER BY total_viewers DESC
        """
        
        return pd.read_sql_query(query, self.conn)
    
    def get_ingestion_history(self):
        """Get history of data ingestion."""
        # Check if ingested_at column exists
        columns_result = self.conn.execute("PRAGMA table_info(streams)")
        columns = [row[1] for row in columns_result.fetchall()]
        has_ingested_at = 'ingested_at' in columns
        
        if has_ingested_at:
            query = """
            SELECT 
                DATE(ingested_at) as ingestion_date,
                COUNT(*) as streams_ingested,
                COUNT(DISTINCT DATE(started_at)) as unique_stream_dates,
                MIN(started_at) as earliest_stream_date,
                MAX(started_at) as latest_stream_date
            FROM streams
            GROUP BY DATE(ingested_at)
            ORDER BY ingestion_date
            """
        else:
            # Return empty DataFrame if no ingestion tracking
            return pd.DataFrame(columns=['ingestion_date', 'streams_ingested', 'unique_stream_dates', 'earliest_stream_date', 'latest_stream_date'])
        
        return pd.read_sql_query(query, self.conn)
    
    def create_historical_visualisations(self):
        """Create comprehensive historical visualisations."""
        plt.style.use('default')
        fig, axes = plt.subplots(2, 3, figsize=(18, 12))
        fig.suptitle('Twitch Streaming Analytics - Historical Overview', fontsize=16, fontweight='bold')
        
        # 1. Daily trends
        daily_trends = self.get_daily_trends()
        if not daily_trends.empty:
            daily_trends['date'] = pd.to_datetime(daily_trends['date'])
            axes[0, 0].plot(daily_trends['date'], daily_trends['daily_viewers'], marker='o', linewidth=2)
            axes[0, 0].set_title('Daily Viewership Trends')
            axes[0, 0].set_xlabel('Date')
            axes[0, 0].set_ylabel('Total Viewers')
            axes[0, 0].tick_params(axis='x', rotation=45)
        
        # 2. Top games
        top_games = self.get_top_games_historical(10)
        if not top_games.empty:
            axes[0, 1].barh(range(len(top_games)), top_games['total_viewers'])
            axes[0, 1].set_yticks(range(len(top_games)))
            axes[0, 1].set_yticklabels(top_games['game_name'], fontsize=8)
            axes[0, 1].set_title('Top Games by Total Viewers')
            axes[0, 1].set_xlabel('Total Viewers')
        
        # 3. Hourly patterns
        hourly = self.get_hourly_patterns_historical()
        if not hourly.empty:
            axes[0, 2].bar(hourly['hour_of_day'], hourly['avg_viewers'])
            axes[0, 2].set_title('Average Viewers by Hour')
            axes[0, 2].set_xlabel('Hour of Day')
            axes[0, 2].set_ylabel('Average Viewers')
            axes[0, 2].set_xticks(range(0, 24, 2))
        
        # 4. Weekend vs Weekday
        weekend_data = self.get_weekend_vs_weekday_historical()
        if not weekend_data.empty:
            axes[1, 0].bar(weekend_data['day_type'], weekend_data['avg_viewers'])
            axes[1, 0].set_title('Average Viewers: Weekend vs Weekday')
            axes[1, 0].set_ylabel('Average Viewers')
        
        # 5. Language distribution
        languages = self.get_language_distribution_historical()
        if not languages.empty and len(languages) > 1:
            top_langs = languages.head(8)
            axes[1, 1].pie(top_langs['total_viewers'], labels=top_langs['language'], autopct='%1.1f%%')
            axes[1, 1].set_title('Viewership by Language')
        
        # 6. Ingestion history
        ingestion = self.get_ingestion_history()
        if not ingestion.empty:
            ingestion['ingestion_date'] = pd.to_datetime(ingestion['ingestion_date'])
            axes[1, 2].bar(ingestion['ingestion_date'], ingestion['streams_ingested'])
            axes[1, 2].set_title('Data Ingestion History')
            axes[1, 2].set_xlabel('Ingestion Date')
            axes[1, 2].set_ylabel('Streams Ingested')
            axes[1, 2].tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        output_file = OUTPUT_DIR / "historical_overview.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"📊 Historical visualisations saved to: {output_file}")
        return output_file
    
    def generate_historical_report(self):
        """Generate comprehensive historical analysis report."""
        print("🔍 Generating Historical Analysis Report...")
        
        # Get summary
        summary = self.get_data_summary()
        if summary is None or summary.empty:
            print("❌ No historical data found in database")
            return
        
        print("\n" + "="*60)
        print("📊 HISTORICAL DATA SUMMARY")
        print("="*60)
        print(f"📅 Date Range: {summary['earliest_date']} to {summary['latest_date']}")
        print(f"📆 Total Days: {summary['unique_days']}")
        print(f"🎮 Total Streams: {summary['total_streams']:,}")
        print(f"👥 Total Viewers: {summary['total_viewers']:,}")
        print(f"📈 Average Viewers per Stream: {summary['avg_viewers_per_stream']:.1f}")
        print(f"👤 Unique Streamers: {summary['unique_streamers']:,}")
        print(f"🎯 Unique Games: {summary['unique_games']:,}")
        print(f"🌍 Unique Languages: {summary['unique_languages']}")
        print(f"⏰ First Ingestion: {summary['first_ingestion']}")
        print(f"⏰ Last Ingestion: {summary['last_ingestion']}")
        
        # Top games
        print("\n" + "="*60)
        print("🏆 TOP GAMES (Historical)")
        print("="*60)
        top_games = self.get_top_games_historical(10)
        if not top_games.empty:
            for i, row in top_games.iterrows():
                print(f"{i+1:2d}. {row['game_name']:<25} | {row['total_viewers']:>8,} viewers | {row['total_streams']:>4} streams")
        
        # Top streamers
        print("\n" + "="*60)
        print("👑 TOP STREAMERS (Historical)")
        print("="*60)
        top_streamers = self.get_top_streamers_historical(10)
        if not top_streamers.empty:
            for i, row in top_streamers.iterrows():
                print(f"{i+1:2d}. {row['user_name']:<20} | {row['total_viewers']:>8,} viewers | {row['total_streams']:>4} streams")
        
        # Weekend vs Weekday
        print("\n" + "="*60)
        print("📅 WEEKEND VS WEEKDAY ANALYSIS")
        print("="*60)
        weekend_data = self.get_weekend_vs_weekday_historical()
        if not weekend_data.empty:
            for _, row in weekend_data.iterrows():
                print(f"{row['day_type']:<8} | {row['total_streams']:>6} streams | {row['total_viewers']:>10,} viewers | {row['avg_viewers']:>6.1f} avg")
        
        # Language distribution
        print("\n" + "="*60)
        print("🌍 LANGUAGE DISTRIBUTION")
        print("="*60)
        languages = self.get_language_distribution_historical()
        if not languages.empty:
            for _, row in languages.head(10).iterrows():
                print(f"{row['language']:<5} | {row['total_viewers']:>8,} viewers | {row['percentage']:>5.1f}%")
        
        # Create visualisations
        print("\n" + "="*60)
        print("📊 GENERATING VISUALISATIONS")
        print("="*60)
        self.create_historical_visualisations()
        
        print("\n✅ Historical analysis complete!")
    
    def close(self):
        """Close database connection."""
        self.conn.close()

def main():
    """Main function to run historical analysis."""
    analyzer = HistoricalAnalyzer()
    try:
        analyzer.generate_historical_report()
    finally:
        analyzer.close()

if __name__ == "__main__":
    main()
