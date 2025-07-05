import logging
import sys
from pathlib import Path
import pandas as pd
from sqlalchemy.orm import sessionmaker, Session
from typing import Dict
import click

# Add project root to path to allow imports
sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.utils.database import DatabaseManager
from src.utils.mlb_database_models import MlbBatterStats, MlbPitcherStats, MlbGame, MlbPlayer

# Configure logging
log_file_path = Path(__file__).resolve().parents[3] / 'logs' / 'mlb_feature_engineering.log'
log_file_path.parent.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)

def get_game_dates(session: Session) -> Dict[int, pd.Timestamp]:
    """Fetches all game dates from the database."""
    logging.info("Fetching game dates...")
    games = session.query(MlbGame.game_id, MlbGame.game_date).all()
    game_dates = {game.game_id: pd.to_datetime(game.game_date) for game in games}
    logging.info(f"Loaded dates for {len(game_dates)} games.")
    return game_dates

def get_home_teams(session: Session) -> Dict[int, int]:
    """Fetches a mapping of game_id to home_team_id."""
    logging.info("Fetching home team data...")
    games = session.query(MlbGame.game_id, MlbGame.home_team_id).all()
    home_teams = {game.game_id: game.home_team_id for game in games}
    logging.info(f"Loaded home team info for {len(home_teams)} games.")
    return home_teams

def get_starting_pitchers(session: Session) -> Dict[int, Dict[str, int]]:
    """Fetches a mapping of game_id to starting pitchers."""
    logging.info("Fetching starting pitcher data...")
    games = session.query(MlbGame.game_id, MlbGame.home_starting_pitcher_id, MlbGame.away_starting_pitcher_id).all()
    starting_pitchers = {
        game.game_id: {
            'home': game.home_starting_pitcher_id,
            'away': game.away_starting_pitcher_id
        } for game in games
    }
    logging.info(f"Loaded starting pitcher info for {len(starting_pitchers)} games.")
    return starting_pitchers

def get_player_handedness(session: Session) -> Dict[int, str]:
    """Fetches a mapping of player_id to their throwing hand."""
    logging.info("Fetching player handedness data...")
    players = session.query(MlbPlayer.player_id, MlbPlayer.throws).all()
    handedness = {player.player_id: player.throws for player in players}
    logging.info(f"Loaded handedness info for {len(handedness)} players.")
    return handedness

@click.command()
@click.option('--player-type', type=click.Choice(['batters', 'pitchers', 'all']), default='all', help='Type of players to process.')
def calculate_rolling_player_features(player_type: str):
    """
    Calculates rolling averages for MLB player stats.
    """
    logging.info("Starting MLB player feature engineering.")
    
    db_manager = DatabaseManager()
    session = db_manager.get_session()

    try:
        game_dates = get_game_dates(session)
        home_teams = get_home_teams(session)
        starting_pitchers = get_starting_pitchers(session)
        player_handedness = get_player_handedness(session)
        
        if player_type in ['batters', 'all']:
            # Load batter stats
            batter_query = session.query(MlbBatterStats).statement
            df_batters = pd.read_sql(batter_query, session.bind)
            logging.info(f"Loaded {len(df_batters)} batter stat entries.")

            if not df_batters.empty:
                df_batters['game_date'] = df_batters['game_id'].apply(lambda x: game_dates.get(x))
                df_batters['is_home'] = df_batters.apply(lambda row: 1 if home_teams.get(row['game_id']) == row['team_id'] else 0, axis=1)
                
                # Get opposing starting pitcher
                def get_opp_sp(row):
                    pitchers = starting_pitchers.get(row['game_id'])
                    if not pitchers:
                        return None
                    return pitchers['away'] if row['is_home'] else pitchers['home']

                df_batters['opp_sp_id'] = df_batters.apply(get_opp_sp, axis=1)
                df_batters['opp_sp_hand'] = df_batters['opp_sp_id'].apply(lambda x: player_handedness.get(x) if pd.notnull(x) else None)


                df_batters['is_night'] = df_batters['game_date'].apply(lambda x: 1 if x.hour >= 18 else 0)
                df_batters.dropna(subset=['game_date'], inplace=True)
                df_batters = df_batters.sort_values(by=['player_id', 'game_date'])
                
                # Calculate rate stats
                df_batters['obp'] = (df_batters['hits'] + df_batters['walks']) / (df_batters['at_bats'] + df_batters['walks'])
                df_batters['slg'] = (df_batters['hits'] + df_batters['home_runs']) / df_batters['at_bats'] # Simplified SLG
                df_batters['ops'] = df_batters['obp'] + df_batters['slg']
                df_batters.fillna({'obp': 0, 'slg': 0, 'ops': 0}, inplace=True)

                # BvP Calculation
                logging.info("Starting BvP calculation...")
                bvp_features = []
                # Create a copy for historical lookups
                historical_batter_stats = df_batters.copy()

                for _, row in df_batters.iterrows():
                    batter_id = row['player_id']
                    pitcher_id = row['opp_sp_id']
                    game_date = row['game_date']
                    
                    if pitcher_id is None:
                        bvp_features.append({'bvp_avg': 0, 'bvp_obp': 0, 'bvp_slg': 0, 'bvp_ops': 0, 'bvp_ab': 0})
                        continue

                    # Find historical matchups
                    historical_matchups = historical_batter_stats[
                        (historical_batter_stats['player_id'] == batter_id) &
                        (historical_batter_stats['opp_sp_id'] == pitcher_id) &
                        (historical_batter_stats['game_date'] < game_date)
                    ]
                    
                    if historical_matchups.empty:
                        bvp_features.append({'bvp_avg': 0, 'bvp_obp': 0, 'bvp_slg': 0, 'bvp_ops': 0, 'bvp_ab': 0})
                    else:
                        at_bats = historical_matchups['at_bats'].sum()
                        hits = historical_matchups['hits'].sum()
                        walks = historical_matchups['walks'].sum()
                        home_runs = historical_matchups['home_runs'].sum()
                        
                        bvp_ab = at_bats
                        bvp_avg = hits / at_bats if at_bats > 0 else 0
                        bvp_obp = (hits + walks) / (at_bats + walks) if (at_bats + walks) > 0 else 0
                        bvp_slg = (hits + home_runs) / at_bats if at_bats > 0 else 0 # Simplified
                        bvp_ops = bvp_obp + bvp_slg
                        
                        bvp_features.append({
                            'bvp_avg': bvp_avg, 'bvp_obp': bvp_obp, 'bvp_slg': bvp_slg, 
                            'bvp_ops': bvp_ops, 'bvp_ab': bvp_ab
                        })
                
                df_bvp = pd.DataFrame(bvp_features, index=df_batters.index)
                df_batters = pd.concat([df_batters, df_bvp], axis=1)
                logging.info("Finished BvP calculation.")

                batter_stats_to_roll = ['at_bats', 'runs', 'hits', 'rbi', 'home_runs', 'walks', 'strikeouts', 'obp', 'slg', 'ops']
                for stat in batter_stats_to_roll:
                    # Overall rolling average
                    df_batters[f'{stat}_roll_avg_10g'] = df_batters.groupby('player_id')[stat].transform(lambda x: x.rolling(window=10, min_periods=1).mean())
                    # Home/Away rolling average
                    df_batters[f'{stat}_roll_avg_10g_home_away'] = df_batters.groupby(['player_id', 'is_home'])[stat].transform(lambda x: x.rolling(window=10, min_periods=1).mean())
                    # Day/Night rolling average
                    df_batters[f'{stat}_roll_avg_10g_day_night'] = df_batters.groupby(['player_id', 'is_night'])[stat].transform(lambda x: x.rolling(window=10, min_periods=1).mean())
                    # Platoon rolling average
                    df_batters[f'{stat}_roll_avg_10g_platoon'] = df_batters.groupby(['player_id', 'opp_sp_hand'])[stat].transform(lambda x: x.rolling(window=10, min_periods=1).mean())

                df_batters = df_batters.set_index('game_date')
                logging.info("Calculated rolling averages for batters.")
                print("Sample of batter features:")
                print(df_batters.head())
                df_batters.to_csv(Path(__file__).resolve().parents[3] / 'data' / 'processed' / 'mlb' / 'batter_features.csv')
                logging.info("Saved batter features to data/processed/mlb/batter_features.csv")

        if player_type in ['pitchers', 'all']:
            # Load pitcher stats
            pitcher_query = session.query(MlbPitcherStats).statement
            df_pitchers = pd.read_sql(pitcher_query, session.bind)
            logging.info(f"Loaded {len(df_pitchers)} pitcher stat entries.")

            if not df_pitchers.empty:
                df_pitchers['game_date'] = df_pitchers['game_id'].apply(lambda x: game_dates.get(x))
                df_pitchers['is_home'] = df_pitchers.apply(lambda row: 1 if home_teams.get(row['game_id']) == row['team_id'] else 0, axis=1)
                df_pitchers['is_night'] = df_pitchers['game_date'].apply(lambda x: 1 if x.hour >= 18 else 0)
                df_pitchers.dropna(subset=['game_date'], inplace=True)
                df_pitchers = df_pitchers.sort_values(by=['player_id', 'game_date'])

                # Calculate rate stats
                df_pitchers['era'] = (df_pitchers['earned_runs'] / df_pitchers['innings_pitched']) * 9
                df_pitchers['whip'] = (df_pitchers['walks'] + df_pitchers['hits_allowed']) / df_pitchers['innings_pitched']
                df_pitchers.fillna({'era': 0, 'whip': 0}, inplace=True)

                pitcher_stats_to_roll = ['innings_pitched', 'hits_allowed', 'runs_allowed', 'earned_runs', 'walks', 'strikeouts', 'era', 'whip']
                for stat in pitcher_stats_to_roll:
                    df_pitchers[f'{stat}_roll_avg_10g'] = df_pitchers.groupby('player_id')[stat].transform(lambda x: x.rolling(window=10, min_periods=1).mean())
                    df_pitchers[f'{stat}_roll_avg_10g_home_away'] = df_pitchers.groupby(['player_id', 'is_home'])[stat].transform(lambda x: x.rolling(window=10, min_periods=1).mean())
                    df_pitchers[f'{stat}_roll_avg_10g_day_night'] = df_pitchers.groupby(['player_id', 'is_night'])[stat].transform(lambda x: x.rolling(window=10, min_periods=1).mean())
                
                df_pitchers = df_pitchers.set_index('game_date')
                logging.info("Calculated rolling averages for pitchers.")
                print("Sample of pitcher features:")
                print(df_pitchers.head())
                df_pitchers.to_csv(Path(__file__).resolve().parents[3] / 'data' / 'processed' / 'mlb' / 'pitcher_features.csv')
                logging.info("Saved pitcher features to data/processed/mlb/pitcher_features.csv")

        logging.info("Successfully calculated and saved rolling features.")

    except Exception as e:
        logging.error(f"An error occurred during player feature engineering: {e}")
    finally:
        session.close()
        logging.info("Player feature engineering finished.")

if __name__ == '__main__':
    calculate_rolling_player_features() 