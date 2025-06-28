#!/usr/bin/env python3
"""
Data exploration and analysis for Phase 3 planning.
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime
from typing import Dict, List, Any
from src.utils.database import db_manager
from sqlalchemy import text

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataExplorer:
    """Explore and analyze the current dataset."""
    
    def __init__(self):
        self.db_manager = db_manager
    
    def get_data_summary(self) -> Dict[str, Any]:
        """Get comprehensive summary of all data tables."""
        
        with self.db_manager.get_session() as session:
            summary = {}
            
            # Teams analysis
            teams_query = text("SELECT COUNT(*) as count FROM teams")
            teams_count = session.execute(teams_query).scalar()
            
            teams_sample = session.execute(text("SELECT * FROM teams LIMIT 5")).fetchall()
            summary['teams'] = {
                'count': teams_count,
                'sample': teams_sample,
                'columns': ['team_id', 'team_name', 'team_abbreviation', 'league']
            }
            
            # Players analysis
            players_query = text("SELECT COUNT(*) as count FROM players")
            players_count = session.execute(players_query).scalar()
            
            valid_players_query = text("""
                SELECT COUNT(*) as count 
                FROM players 
                WHERE player_id REGEXP '^[0-9]+$'
            """)
            valid_players_count = session.execute(valid_players_query).scalar()
            
            players_sample = session.execute(text("SELECT * FROM players LIMIT 5")).fetchall()
            summary['players'] = {
                'count': players_count,
                'valid_count': valid_players_count,
                'sample': players_sample,
                'columns': ['player_id', 'full_name', 'team_name', 'position']
            }
            
            # Games analysis
            games_query = text("SELECT COUNT(*) as count FROM games")
            games_count = session.execute(games_query).scalar()
            
            games_sample = session.execute(text("SELECT * FROM games LIMIT 5")).fetchall()
            summary['games'] = {
                'count': games_count,
                'sample': games_sample,
                'columns': ['game_id', 'date', 'home_team_id', 'away_team_id', 'home_score', 'away_score']
            }
            
            # Player stats analysis
            stats_query = text("SELECT COUNT(*) as count FROM player_game_stats")
            stats_count = session.execute(stats_query).scalar() or 0
            
            if stats_count > 0:
                stats_sample = session.execute(text("SELECT * FROM player_game_stats LIMIT 5")).fetchall()
                summary['player_stats'] = {
                    'count': stats_count,
                    'sample': stats_sample,
                    'columns': ['stat_id', 'game_id', 'player_id', 'points', 'rebounds', 'assists']
                }
            else:
                summary['player_stats'] = {
                    'count': 0,
                    'sample': [],
                    'columns': []
                }
            
            # Data quality analysis
            summary['data_quality'] = self._analyze_data_quality(session)
            
        return summary
    
    def _analyze_data_quality(self, session) -> Dict[str, Any]:
        """Analyze data quality issues."""
        
        quality_issues = {}
        
        # Check for missing team relationships in games
        missing_team_games = session.execute(text("""
            SELECT COUNT(*) as count 
            FROM games 
            WHERE home_team_id IS NULL OR away_team_id IS NULL
        """)).scalar()
        
        quality_issues['missing_team_relationships'] = missing_team_games
        
        # Check for players without team information
        players_no_team = session.execute(text("""
            SELECT COUNT(*) as count 
            FROM players 
            WHERE team_name IS NULL OR team_name = ''
        """)).scalar()
        
        quality_issues['players_without_teams'] = players_no_team
        
        # Check for invalid player IDs
        invalid_player_ids = session.execute(text("""
            SELECT COUNT(*) as count 
            FROM players 
            WHERE player_id LIKE 'BR_%' OR player_id = '2024'
        """)).scalar()
        
        quality_issues['invalid_player_ids'] = invalid_player_ids
        
        # Check for games without scores
        games_no_scores = session.execute(text("""
            SELECT COUNT(*) as count 
            FROM games 
            WHERE home_score IS NULL OR away_score IS NULL
        """)).scalar()
        
        quality_issues['games_without_scores'] = games_no_scores
        
        return quality_issues
    
    def get_player_stats_analysis(self) -> Dict[str, Any]:
        """Analyze player statistics data."""
        
        with self.db_manager.get_session() as session:
            analysis = {}
            
            # Get basic stats
            stats_query = text("""
                SELECT 
                    COUNT(*) as total_stats,
                    COUNT(DISTINCT player_id) as unique_players,
                    COUNT(DISTINCT game_id) as unique_games,
                    AVG(points) as avg_points,
                    AVG(rebounds) as avg_rebounds,
                    AVG(assists) as avg_assists,
                    MIN(points) as min_points,
                    MAX(points) as max_points
                FROM player_game_stats
            """)
            
            result = session.execute(stats_query).fetchone()
            if result:
                analysis['basic_stats'] = {
                    'total_stats': result[0],
                    'unique_players': result[1],
                    'unique_games': result[2],
                    'avg_points': result[3],
                    'avg_rebounds': result[4],
                    'avg_assists': result[5],
                    'min_points': result[6],
                    'max_points': result[7]
                }
            
            # Get top performers
            top_scorers = session.execute(text("""
                SELECT player_id, AVG(points) as avg_points, COUNT(*) as games
                FROM player_game_stats
                GROUP BY player_id
                HAVING COUNT(*) >= 2
                ORDER BY avg_points DESC
                LIMIT 10
            """)).fetchall()
            
            analysis['top_scorers'] = top_scorers
            
            # Get stat distributions
            points_dist = session.execute(text("""
                SELECT 
                    CASE 
                        WHEN points < 10 THEN '0-9'
                        WHEN points < 20 THEN '10-19'
                        WHEN points < 30 THEN '20-29'
                        WHEN points < 40 THEN '30-39'
                        ELSE '40+'
                    END as points_range,
                    COUNT(*) as count
                FROM player_game_stats
                GROUP BY points_range
                ORDER BY points_range
            """)).fetchall()
            
            analysis['points_distribution'] = points_dist
            
        return analysis
    
    def get_feature_engineering_opportunities(self) -> Dict[str, List[str]]:
        """Identify opportunities for feature engineering."""
        
        opportunities = {
            'player_features': [
                'Rolling average points (last 5, 10 games)',
                'Season average points',
                'Points trend (increasing/decreasing)',
                'Home vs Away performance',
                'Rest days impact',
                'Back-to-back game performance'
            ],
            'team_features': [
                'Team offensive rating',
                'Team defensive rating',
                'Team recent form (last 10 games)',
                'Head-to-head record',
                'Home court advantage'
            ],
            'game_features': [
                'Game importance (playoff implications)',
                'Season timing (early/mid/late season)',
                'Rest days for both teams',
                'Travel distance',
                'Previous matchup results'
            ],
            'contextual_features': [
                'Player injury status',
                'Team injury impact',
                'Schedule density',
                'Weather conditions (if available)',
                'Fan attendance (if available)'
            ]
        }
        
        return opportunities
    
    def generate_report(self) -> str:
        """Generate a comprehensive data exploration report."""
        
        summary = self.get_data_summary()
        player_analysis = self.get_player_stats_analysis()
        opportunities = self.get_feature_engineering_opportunities()
        
        report = []
        report.append("=" * 60)
        report.append("DATA EXPLORATION REPORT - PHASE 3 PLANNING")
        report.append("=" * 60)
        report.append("")
        
        # Data Summary
        report.append("📊 DATA SUMMARY")
        report.append("-" * 30)
        report.append(f"Teams: {summary['teams']['count']}")
        report.append(f"Players: {summary['players']['count']} (Valid: {summary['players']['valid_count']})")
        report.append(f"Games: {summary['games']['count']}")
        report.append(f"Player Stats: {summary['player_stats']['count']}")
        report.append("")
        
        # Data Quality Issues
        report.append("🔍 DATA QUALITY ISSUES")
        report.append("-" * 30)
        for issue, count in summary['data_quality'].items():
            report.append(f"{issue}: {count}")
        report.append("")
        
        # Player Stats Analysis
        if player_analysis.get('basic_stats'):
            stats = player_analysis['basic_stats']
            report.append("🏀 PLAYER STATISTICS ANALYSIS")
            report.append("-" * 30)
            report.append(f"Total Stats: {stats['total_stats']}")
            report.append(f"Unique Players: {stats['unique_players']}")
            report.append(f"Unique Games: {stats['unique_games']}")
            report.append(f"Average Points: {stats['avg_points']:.1f}")
            report.append(f"Average Rebounds: {stats['avg_rebounds']:.1f}")
            report.append(f"Average Assists: {stats['avg_assists']:.1f}")
            report.append(f"Points Range: {stats['min_points']} - {stats['max_points']}")
            report.append("")
        
        # Feature Engineering Opportunities
        report.append("🚀 FEATURE ENGINEERING OPPORTUNITIES")
        report.append("-" * 30)
        for category, features in opportunities.items():
            report.append(f"\n{category.upper()}:")
            for feature in features:
                report.append(f"  • {feature}")
        report.append("")
        
        # Recommendations
        report.append("📋 PHASE 3 RECOMMENDATIONS")
        report.append("-" * 30)
        report.append("1. Collect more player game stats (current: 11, target: 1000+)")
        report.append("2. Clean invalid player IDs and missing team relationships")
        report.append("3. Implement rolling average calculations for player performance")
        report.append("4. Create team performance metrics")
        report.append("5. Build game context features (home/away, rest days)")
        report.append("6. Develop baseline statistical models")
        report.append("")
        
        report.append("=" * 60)
        
        return "\n".join(report)

def main():
    """Run data exploration and generate report."""
    
    explorer = DataExplorer()
    report = explorer.generate_report()
    
    print(report)
    
    # Save report to file
    with open('data_exploration_report.txt', 'w', encoding='utf-8') as f:
        f.write(report)
    
    logger.info("Data exploration report generated and saved to 'data_exploration_report.txt'")

if __name__ == "__main__":
    main() 