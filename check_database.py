"""
Script to check what data we have in the database.
"""

import logging
from src.utils.database import db_manager, Teams, Players, Games, PlayerGameStats
from sqlalchemy import func
from pathlib import Path

# Setup logging to file
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / "database_check.log"),
        logging.StreamHandler()
    ]
)

def check_database():
    """Check what data we have collected."""
    
    logging.info("Starting database check...")

    try:
        with db_manager.get_session() as session:
            # Check tables by trying to query them
            logging.info("Tables in database:")
            for table in [Teams, Players, Games, PlayerGameStats]:
                try:
                    session.query(table).first()
                    logging.info(f"  - {table.__tablename__}")
                except Exception:
                    logging.warning(f"  - {table.__tablename__} (not found or empty)")

            logging.info("\n" + "="*50)

            # Check teams
            team_count = session.query(func.count(Teams.team_id)).scalar()
            logging.info(f"Teams: {team_count}")
            if team_count > 0:
                teams = session.query(Teams).limit(5).all()
                logging.info("Sample teams:")
                for team in teams:
                    logging.info(f"  {team.team_id}, {team.team_name}")

            logging.info("\n" + "="*50)

            # Check players
            player_count = session.query(func.count(Players.player_id)).scalar()
            logging.info(f"Players: {player_count}")
            if player_count > 0:
                players = session.query(Players).limit(10).all()
                logging.info("Sample players:")
                for player in players:
                    logging.info(f"  {player.player_id}, {player.full_name}, {player.team_id}")
            
            logging.info("\n" + "="*50)

            # Check games
            game_count = session.query(func.count(Games.game_id)).scalar()
            logging.info(f"Games: {game_count}")

            # Games per season
            games_by_season = session.query(Games.season, func.count(Games.game_id)).group_by(Games.season).all()
            logging.info("Games per season:")
            for season, count in games_by_season:
                logging.info(f"  - Season {season}: {count} games")

            if game_count > 0:
                games = session.query(Games).limit(5).all()
                logging.info("Sample games:")
                for game in games:
                    logging.info(f"  {game.game_id}, {game.date}, {game.home_team_name} vs {game.away_team_name}")

            logging.info("\n" + "="*50)

            # Check player stats
            stats_count = session.query(func.count(PlayerGameStats.stat_id)).scalar()
            logging.info(f"Player Game Stats: {stats_count}")
            if stats_count > 0:
                stats = session.query(PlayerGameStats).limit(5).all()
                logging.info("Sample player game stats:")
                for stat in stats:
                    logging.info(f"  Player: {stat.player_id}, Game: {stat.game_id}, Points: {stat.points}")

            logging.info("\n" + "="*50)
            
            # Check for players with valid IDs (assuming non-BR is valid)
            valid_player_count = session.query(func.count(Players.player_id)).filter(Players.player_id.notlike('BR_%')).scalar()
            logging.info(f"Players with valid IDs: {valid_player_count}")
            if valid_player_count > 0:
                valid_players = session.query(Players).filter(Players.player_id.notlike('BR_%')).limit(10).all()
                logging.info("Sample valid players:")
                for player in valid_players:
                    logging.info(f"  {player.player_id}, {player.full_name}, {player.team_id}")


    except Exception as e:
        logging.error(f"An error occurred while checking the database: {e}")


if __name__ == "__main__":
    check_database() 