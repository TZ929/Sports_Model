#!/usr/bin/env python3
"""
Check team associations for players in the database.
"""

from src.utils.database import db_manager
from sqlalchemy import text

def check_player_teams():
    """Check what teams players are associated with."""
    
    with db_manager.get_session() as session:
        # Check all unique team names in players table
        query = text("""
            SELECT DISTINCT team_name, COUNT(*) as player_count
            FROM players 
            WHERE team_name IS NOT NULL AND team_name != ''
            GROUP BY team_name
            ORDER BY player_count DESC
        """)
        
        result = session.execute(query)
        teams = result.fetchall()
        
        print("Teams associated with players:")
        print("-" * 40)
        for team_name, count in teams:
            print(f"{team_name}: {count} players")
        
        print(f"\nTotal teams with players: {len(teams)}")
        
        # Check players without team info
        no_team_query = text("""
            SELECT COUNT(*) as count
            FROM players 
            WHERE team_name IS NULL OR team_name = ''
        """)
        
        no_team_count = session.execute(no_team_query).scalar()
        print(f"Players without team info: {no_team_count}")
        
        # Check for OKC specifically
        okc_query = text("""
            SELECT player_id, full_name, team_name
            FROM players 
            WHERE team_name LIKE '%Thunder%' OR team_name LIKE '%OKC%'
        """)
        
        okc_players = session.execute(okc_query).fetchall()
        print(f"\nOKC Thunder players found: {len(okc_players)}")
        for player_id, name, team in okc_players:
            print(f"  - {name} ({player_id}): {team}")

if __name__ == "__main__":
    check_player_teams() 