"""
Database utilities for the NBA/WNBA predictive model.
"""

import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.sql import text
from datetime import datetime

from .config import config

# Set up logging
logger = logging.getLogger(__name__)

# Create SQLAlchemy base class
Base = declarative_base()


class Games(Base):
    """Games table to store game information."""
    __tablename__ = 'games'
    
    game_id = Column(String(50), primary_key=True)
    date = Column(DateTime, nullable=False)
    home_team_id = Column(String(10), nullable=False)
    away_team_id = Column(String(10), nullable=False)
    home_team_name = Column(String(50), nullable=False)
    away_team_name = Column(String(50), nullable=False)
    home_score = Column(Integer)
    away_score = Column(Integer)
    season = Column(String(10), nullable=False)
    league = Column(String(10), default='NBA')  # NBA or WNBA
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Players(Base):
    """Players table to store player information."""
    __tablename__ = 'players'
    
    player_id = Column(String(50), primary_key=True)
    full_name = Column(String(100), nullable=False)
    team_id = Column(String(10))
    team_name = Column(String(50))
    position = Column(String(10))
    height = Column(String(10))
    weight = Column(Integer)
    birth_date = Column(DateTime)
    league = Column(String(10), default='NBA')
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class PlayerGameStats(Base):
    """Player game statistics table."""
    __tablename__ = 'player_game_stats'
    
    stat_id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(50), nullable=False)
    player_id = Column(String(50), nullable=False)
    team_id = Column(String(10), nullable=False)
    minutes_played = Column(Integer)
    field_goals_made = Column(Integer)
    field_goals_attempted = Column(Integer)
    three_pointers_made = Column(Integer)
    three_pointers_attempted = Column(Integer)
    free_throws_made = Column(Integer)
    free_throws_attempted = Column(Integer)
    rebounds = Column(Integer)
    offensive_rebounds = Column(Integer)
    defensive_rebounds = Column(Integer)
    assists = Column(Integer)
    steals = Column(Integer)
    blocks = Column(Integer)
    turnovers = Column(Integer)
    personal_fouls = Column(Integer)
    points = Column(Integer)
    plus_minus = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)


class PropOdds(Base):
    """Prop odds table to store betting lines."""
    __tablename__ = 'prop_odds'
    
    prop_id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(50), nullable=False)
    player_id = Column(String(50))
    sportsbook = Column(String(20), nullable=False)  # fanduel, espnbet
    prop_type = Column(String(30), nullable=False)  # points, rebounds, assists, etc.
    line = Column(Float, nullable=False)
    over_odds = Column(Integer)
    under_odds = Column(Integer)
    over_implied_prob = Column(Float)
    under_implied_prob = Column(Float)
    timestamp = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)


class ModelPredictions(Base):
    """Model predictions table."""
    __tablename__ = 'model_predictions'
    
    prediction_id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(50), nullable=False)
    player_id = Column(String(50))
    prop_type = Column(String(30), nullable=False)
    model_name = Column(String(50), nullable=False)
    predicted_probability = Column(Float, nullable=False)
    predicted_outcome = Column(String(10))  # over, under
    confidence_score = Column(Float)
    features_used = Column(Text)  # JSON string of features
    created_at = Column(DateTime, default=datetime.utcnow)


class BetRecommendations(Base):
    """Bet recommendations table."""
    __tablename__ = 'bet_recommendations'
    
    recommendation_id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(50), nullable=False)
    player_id = Column(String(50))
    prop_type = Column(String(30), nullable=False)
    sportsbook = Column(String(20), nullable=False)
    line = Column(Float, nullable=False)
    bet_type = Column(String(10), nullable=False)  # over, under
    model_probability = Column(Float, nullable=False)
    implied_probability = Column(Float, nullable=False)
    edge = Column(Float, nullable=False)
    stake_recommendation = Column(Float)
    confidence_level = Column(String(20))  # high, medium, low
    status = Column(String(20), default='pending')  # pending, won, lost, void
    actual_outcome = Column(String(10))  # over, under
    profit_loss = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Teams(Base):
    """Teams table to store team information."""
    __tablename__ = 'teams'
    
    team_id = Column(String(10), primary_key=True)
    team_name = Column(String(50), nullable=False)
    team_abbreviation = Column(String(10), nullable=False)
    league = Column(String(10), default='NBA')
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class DatabaseManager:
    """Database manager for the sports model."""
    
    def __init__(self):
        """Initialize database connection."""
        self.engine = None
        self.SessionLocal = None
        self._setup_connection()
    
    def _setup_connection(self):
        """Set up database connection."""
        try:
            database_url = config.get_database_url()
            self.engine = create_engine(database_url, echo=False)
            self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
            logger.info(f"Database connection established: {database_url}")
        except Exception as e:
            logger.error(f"Failed to establish database connection: {e}")
            raise
    
    def create_tables(self):
        """Create all database tables."""
        try:
            Base.metadata.create_all(bind=self.engine)
            logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Failed to create database tables: {e}")
            raise
    
    def get_session(self) -> Session:
        """Get database session."""
        return self.SessionLocal()
    
    def close_session(self, session: Session):
        """Close database session."""
        session.close()
    
    def insert_game(self, game_data: Dict[str, Any]) -> bool:
        """Insert a new game record.
        
        Args:
            game_data: Dictionary containing game information
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.get_session() as session:
                game = Games(**game_data)
                session.add(game)
                session.commit()
                logger.info(f"Game inserted: {game_data.get('game_id')}")
                return True
        except Exception as e:
            logger.error(f"Failed to insert game: {e}")
            return False
    
    def insert_player_stats(self, stats_data: Dict[str, Any]) -> bool:
        """Insert player game statistics.
        
        Args:
            stats_data: Dictionary containing player statistics
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.get_session() as session:
                stats = PlayerGameStats(**stats_data)
                session.add(stats)
                session.commit()
                return True
        except Exception as e:
            logger.error(f"Failed to insert player stats: {e}")
            return False
    
    def insert_prop_odds(self, odds_data: Dict[str, Any]) -> bool:
        """Insert prop odds data.
        
        Args:
            odds_data: Dictionary containing odds information
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.get_session() as session:
                odds = PropOdds(**odds_data)
                session.add(odds)
                session.commit()
                return True
        except Exception as e:
            logger.error(f"Failed to insert prop odds: {e}")
            return False
    
    def get_games_by_date_range(self, start_date: datetime, end_date: datetime, league: str = 'NBA') -> List[Dict]:
        """Get games within a date range.
        
        Args:
            start_date: Start date for query
            end_date: End date for query
            league: League to filter by (NBA or WNBA)
            
        Returns:
            List of game dictionaries
        """
        try:
            with self.get_session() as session:
                games = session.query(Games).filter(
                    Games.date >= start_date,
                    Games.date <= end_date,
                    Games.league == league
                ).all()
                
                return [self._row_to_dict(game) for game in games]
        except Exception as e:
            logger.error(f"Failed to get games by date range: {e}")
            return []
    
    def get_player_stats_by_game(self, game_id: str) -> List[Dict]:
        """Get all player statistics for a specific game.
        
        Args:
            game_id: Game identifier
            
        Returns:
            List of player statistics dictionaries
        """
        try:
            with self.get_session() as session:
                stats = session.query(PlayerGameStats).filter(
                    PlayerGameStats.game_id == game_id
                ).all()
                
                return [self._row_to_dict(stat) for stat in stats]
        except Exception as e:
            logger.error(f"Failed to get player stats by game: {e}")
            return []
    
    def get_latest_prop_odds(self, game_id: str, sportsbook: str) -> List[Dict]:
        """Get latest prop odds for a game from a specific sportsbook.
        
        Args:
            game_id: Game identifier
            sportsbook: Sportsbook name
            
        Returns:
            List of prop odds dictionaries
        """
        try:
            with self.get_session() as session:
                # Get the latest odds for each prop type
                latest_odds = session.query(PropOdds).filter(
                    PropOdds.game_id == game_id,
                    PropOdds.sportsbook == sportsbook
                ).order_by(PropOdds.timestamp.desc()).all()
                
                return [self._row_to_dict(odds) for odds in latest_odds]
        except Exception as e:
            logger.error(f"Failed to get latest prop odds: {e}")
            return []
    
    def _row_to_dict(self, row) -> Dict:
        """Convert SQLAlchemy row to dictionary."""
        return {column.name: getattr(row, column.name) for column in row.__table__.columns}

    def insert_team(self, team_data: Dict[str, Any]) -> bool:
        """Insert a new team record.
        Args:
            team_data: Dictionary containing team information
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.get_session() as session:
                team = Teams(**team_data)
                session.add(team)
                session.commit()
                logger.info(f"Team inserted: {team_data.get('team_id')}")
                return True
        except Exception as e:
            logger.error(f"Failed to insert team: {e}")
            return False

    def insert_player(self, player_data: Dict[str, Any]) -> bool:
        """Insert a new player record.
        Args:
            player_data: Dictionary containing player information
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.get_session() as session:
                player = Players(**player_data)
                session.add(player)
                session.commit()
                logger.info(f"Player inserted: {player_data.get('player_id')}")
                return True
        except Exception as e:
            logger.error(f"Failed to insert player: {e}")
            return False


# Global database manager instance
db_manager = DatabaseManager() 