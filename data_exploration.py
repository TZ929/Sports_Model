#!/usr/bin/env python3
"""
Data exploration and analysis for Phase 3 planning.
"""

import pandas as pd
from src.utils.database import db_manager, Teams, Players, Games, PlayerGameStats
import logging
from pathlib import Path
import io

# Setup logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / "data_exploration.log"),
        logging.StreamHandler()
    ]
)

def load_data_from_db():
    """Load all tables from the database into pandas DataFrames."""
    logging.info("Loading data from database...")
    with db_manager.get_session() as session:
        connection = session.connection()
        teams_df = pd.read_sql_table(Teams.__tablename__, connection)
        players_df = pd.read_sql_table(Players.__tablename__, connection)
        games_df = pd.read_sql_table(Games.__tablename__, connection)
        player_stats_df = pd.read_sql_table(PlayerGameStats.__tablename__, connection)
    logging.info("Data loaded successfully.")
    return teams_df, players_df, games_df, player_stats_df

def explore_data(teams_df, players_df, games_df, player_stats_df):
    """Perform initial data exploration and return a report."""
    logging.info("Starting data exploration...")
    
    report_io = io.StringIO()

    # --- Teams Exploration ---
    report_io.write("\n" + "="*20 + " Teams Data " + "="*20 + "\n")
    report_io.write(f"Shape: {teams_df.shape}\n")
    teams_df.info(buf=report_io)
    report_io.write(f"\nMissing Values:\n{teams_df.isnull().sum().to_string()}\n")
    report_io.write(f"\nSample Data:\n{teams_df.head().to_string()}\n")

    # --- Players Exploration ---
    report_io.write("\n" + "="*20 + " Players Data " + "="*20 + "\n")
    report_io.write(f"Shape: {players_df.shape}\n")
    players_df.info(buf=report_io)
    report_io.write(f"\nMissing Values:\n{players_df.isnull().sum().to_string()}\n")
    report_io.write(f"\nSample Data:\n{players_df.head().to_string()}\n")

    # --- Games Exploration ---
    report_io.write("\n" + "="*20 + " Games Data " + "="*20 + "\n")
    report_io.write(f"Shape: {games_df.shape}\n")
    games_df.info(buf=report_io)
    report_io.write(f"\nMissing Values:\n{games_df.isnull().sum().to_string()}\n")
    report_io.write(f"\nSample Data:\n{games_df.head().to_string()}\n")

    # --- Player Stats Exploration ---
    report_io.write("\n" + "="*20 + " Player Stats Data " + "="*20 + "\n")
    report_io.write(f"Shape: {player_stats_df.shape}\n")
    player_stats_df.info(buf=report_io)
    report_io.write(f"\nMissing Values:\n{player_stats_df.isnull().sum().to_string()}\n")
    report_io.write(f"\nSample Data:\n{player_stats_df.head().to_string()}\n")
    
    report = report_io.getvalue()
    logging.info("Data exploration complete. Report generated.")
    return report


if __name__ == '__main__':
    teams, players, games, player_stats = load_data_from_db()
    exploration_report = explore_data(teams, players, games, player_stats)
    
    # Save report to file
    report_path = 'data_exploration_report.txt'
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(exploration_report)
    
    logging.info(f"Data exploration report saved to '{report_path}'")
    print(f"\nData exploration report saved to '{report_path}'")
    print("Please review the report for a summary of the data.") 