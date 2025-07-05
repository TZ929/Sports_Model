from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

Base = declarative_base()

class MlbTeam(Base):
    __tablename__ = 'mlb_teams'
    
    id = Column(Integer, primary_key=True)
    team_id = Column(String, unique=True, nullable=False)
    team_name = Column(String, nullable=False)
    team_abbreviation = Column(String, nullable=False)

class MlbPlayer(Base):
    __tablename__ = 'mlb_players'
    
    id = Column(Integer, primary_key=True)
    player_id = Column(String, unique=True, nullable=False)
    full_name = Column(String, nullable=False)
    team_id = Column(String, ForeignKey('mlb_teams.team_id'))
    position = Column(String)
    
    team = relationship("MlbTeam")

class MlbGame(Base):
    __tablename__ = 'mlb_games'
    
    id = Column(Integer, primary_key=True)
    game_id = Column(String, unique=True, nullable=False)
    game_date = Column(DateTime, nullable=False)
    season = Column(Integer)
    
    home_team_id = Column(String, ForeignKey('mlb_teams.team_id'))
    away_team_id = Column(String, ForeignKey('mlb_teams.team_id'))
    
    home_team_score = Column(Integer)
    away_team_score = Column(Integer)
    
    home_team = relationship("MlbTeam", foreign_keys=[home_team_id])
    away_team = relationship("MlbTeam", foreign_keys=[away_team_id])


class MlbBatterStats(Base):
    __tablename__ = 'mlb_batter_stats'

    id = Column(Integer, primary_key=True)
    game_id = Column(String, ForeignKey('mlb_games.game_id'), nullable=False)
    player_id = Column(String, ForeignKey('mlb_players.player_id'), nullable=False)
    team_id = Column(String, ForeignKey('mlb_teams.team_id'), nullable=False)
    
    at_bats = Column(Integer)
    runs = Column(Integer)
    hits = Column(Integer)
    rbi = Column(Integer)
    home_runs = Column(Integer)
    walks = Column(Integer)
    strikeouts = Column(Integer)
    stolen_bases = Column(Integer)
    batting_avg = Column(Float)
    on_base_plus_slugging = Column(Float) # OPS

    game = relationship("MlbGame")
    player = relationship("MlbPlayer")
    team = relationship("MlbTeam")


class MlbPitcherStats(Base):
    __tablename__ = 'mlb_pitcher_stats'

    id = Column(Integer, primary_key=True)
    game_id = Column(String, ForeignKey('mlb_games.game_id'), nullable=False)
    player_id = Column(String, ForeignKey('mlb_players.player_id'), nullable=False)
    team_id = Column(String, ForeignKey('mlb_teams.team_id'), nullable=False)
    
    innings_pitched = Column(Float)
    hits_allowed = Column(Integer)
    runs_allowed = Column(Integer)
    earned_runs = Column(Integer)
    walks = Column(Integer)
    strikeouts = Column(Integer)
    home_runs_allowed = Column(Integer)
    era = Column(Float) # Earned Run Average
    win = Column(Boolean)
    loss = Column(Boolean)
    save = Column(Boolean)
    
    game = relationship("MlbGame")
    player = relationship("MlbPlayer")
    team = relationship("MlbTeam") 