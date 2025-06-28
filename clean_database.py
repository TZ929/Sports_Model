#!/usr/bin/env python3
"""
Clean up database to ensure Phase 2 completion with valid ESPN data only.
"""

import logging
import requests
import time
from src.utils.database import db_manager
from src.data_collection.espn_api import ESPNAPICollector
from sqlalchemy import text

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_database():
    """Clean up database and ensure valid ESPN data."""
    
    with db_manager.get_session() as session:
        try:
            # 1. Remove invalid players (those with BR_ prefix)
            logger.info("Removing invalid players with BR_ prefix...")
            result = session.execute(
                text("DELETE FROM players WHERE player_id LIKE 'BR_%'")
            )
            session.commit()
            logger.info(f"Removed invalid players")
            
            # 2. Check current valid players
            result = session.execute(text("SELECT COUNT(*) FROM players"))
            valid_count = result.scalar() or 0
            logger.info(f"Valid players remaining: {valid_count}")
            
            # 3. If we don't have enough valid players, collect more from ESPN
            if valid_count < 100:
                logger.info("Collecting additional players from ESPN API...")
                collector = ESPNAPICollector()
                
                # Get all teams first
                teams = collector.get_teams("2024")
                logger.info(f"Found {len(teams)} teams")
                
                # Collect players from all teams (not just first 5)
                all_players = []
                for i, team in enumerate(teams):
                    try:
                        team_abbr = team['team_abbreviation']
                        url = f"{collector.base_url}/teams/{team_abbr}/roster"
                        
                        response = requests.get(url, headers=collector.headers, timeout=30)
                        response.raise_for_status()
                        data = response.json()
                        
                        athletes = data.get('athletes', [])
                        for athlete in athletes:
                            player_info = {
                                'player_id': athlete.get('id', ''),
                                'full_name': athlete.get('fullName', ''),
                                'team_name': team['team_name'],
                                'position': athlete.get('position', {}).get('abbreviation', ''),
                                'season': '2024'
                            }
                            all_players.append(player_info)
                        
                        logger.info(f"Collected {len(athletes)} players from {team['team_name']} ({i+1}/{len(teams)})")
                        
                        # Be respectful with API calls
                        time.sleep(0.5)
                        
                    except Exception as e:
                        logger.error(f"Error getting players for team {team['team_name']}: {e}")
                        continue
                
                # Insert new players
                for player in all_players:
                    try:
                        session.execute(
                            text("""
                                INSERT OR IGNORE INTO players (player_id, full_name, team_name, position, season)
                                VALUES (:player_id, :full_name, :team_name, :position, :season)
                            """),
                            player
                        )
                    except Exception as e:
                        logger.error(f"Error inserting player {player['full_name']}: {e}")
                        continue
                
                session.commit()
                logger.info(f"Added {len(all_players)} new players")
            
            # 4. Final count
            result = session.execute(text("SELECT COUNT(*) FROM players"))
            final_count = result.scalar() or 0
            logger.info(f"Final player count: {final_count}")
            
            # 5. Show sample of valid players
            result = session.execute(
                text("SELECT player_id, full_name, team_name FROM players LIMIT 10")
            )
            sample_players = result.fetchall()
            logger.info("Sample valid players:")
            for player in sample_players:
                logger.info(f"  {player[0]} - {player[1]} ({player[2]})")
            
            return True
            
        except Exception as e:
            logger.error(f"Error cleaning database: {e}")
            session.rollback()
            return False

def verify_phase2_completion():
    """Verify that Phase 2 is 100% complete."""
    
    with db_manager.get_session() as session:
        try:
            # Check all required data
            checks = {
                'teams': "SELECT COUNT(*) FROM teams",
                'players': "SELECT COUNT(*) FROM players WHERE player_id NOT LIKE 'BR_%'",
                'games': "SELECT COUNT(*) FROM games",
                'valid_player_ids': "SELECT COUNT(*) FROM players WHERE player_id REGEXP '^[0-9]+$'"
            }
            
            results = {}
            for name, query in checks.items():
                result = session.execute(text(query))
                results[name] = result.scalar() or 0
            
            logger.info("=== PHASE 2 COMPLETION VERIFICATION ===")
            logger.info(f"Teams: {results['teams']}")
            logger.info(f"Valid Players: {results['players']}")
            logger.info(f"Games: {results['games']}")
            logger.info(f"Players with numeric IDs: {results['valid_player_ids']}")
            
            # Phase 2 completion criteria
            phase2_complete = (
                results['teams'] >= 30 and
                results['players'] >= 100 and
                results['games'] >= 50 and
                results['valid_player_ids'] >= 100
            )
            
            if phase2_complete:
                logger.info("✅ PHASE 2 IS 100% COMPLETE!")
                logger.info("All required data is available for modeling.")
            else:
                logger.info("❌ PHASE 2 NOT COMPLETE")
                logger.info("Missing required data for modeling.")
            
            return phase2_complete
            
        except Exception as e:
            logger.error(f"Error verifying Phase 2: {e}")
            return False

if __name__ == "__main__":
    logger.info("Starting database cleanup for Phase 2 completion...")
    
    # Clean the database
    if clean_database():
        logger.info("Database cleanup completed successfully")
        
        # Verify Phase 2 completion
        if verify_phase2_completion():
            logger.info("🎉 PHASE 2 IS NOW 100% COMPLETE!")
        else:
            logger.error("Phase 2 completion verification failed")
    else:
        logger.error("Database cleanup failed") 