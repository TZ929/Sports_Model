"""
Script to check what data we have in the database.
"""

import sqlite3
import json

def check_database():
    """Check what data we have collected."""
    
    conn = sqlite3.connect('data/sports_model.db')
    cursor = conn.cursor()
    
    # Check tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    print("Tables in database:")
    for table in tables:
        print(f"  - {table[0]}")
    
    if not tables:
        print("No tables found in database.")
        conn.close()
        return
    
    print("\n" + "="*50)
    
    # Check teams
    if any('teams' in table[0] for table in tables):
        cursor.execute("SELECT COUNT(*) FROM teams")
        team_count = cursor.fetchone()[0]
        print(f"Teams: {team_count}")
        
        if team_count > 0:
            cursor.execute("SELECT * FROM teams LIMIT 5")
            teams = cursor.fetchall()
            print("Sample teams:")
            for team in teams:
                print(f"  {team}")
    else:
        print("Teams table not found")
    
    print("\n" + "="*50)
    
    # Check players
    if any('players' in table[0] for table in tables):
        cursor.execute("SELECT COUNT(*) FROM players")
        player_count = cursor.fetchone()[0]
        print(f"Players: {player_count}")
        
        if player_count > 0:
            cursor.execute("SELECT player_id, full_name, team_id FROM players LIMIT 10")
            players = cursor.fetchall()
            print("Sample players:")
            for player in players:
                print(f"  {player}")
    else:
        print("Players table not found")
    
    print("\n" + "="*50)
    
    # Check games
    if any('games' in table[0] for table in tables):
        cursor.execute("SELECT COUNT(*) FROM games")
        game_count = cursor.fetchone()[0]
        print(f"Games: {game_count}")
        
        if game_count > 0:
            cursor.execute("SELECT * FROM games LIMIT 5")
            games = cursor.fetchall()
            print("Sample games:")
            for game in games:
                print(f"  {game}")
    else:
        print("Games table not found")
    
    print("\n" + "="*50)
    
    # Check player stats
    if any('player_game_stats' in table[0] for table in tables):
        cursor.execute("SELECT COUNT(*) FROM player_game_stats")
        stats_count = cursor.fetchone()[0]
        print(f"Player Game Stats: {stats_count}")
        if stats_count > 0:
            cursor.execute("SELECT player_id, game_id, points, assists, rebounds FROM player_game_stats LIMIT 5")
            stats = cursor.fetchall()
            print("Sample player game stats:")
            for stat in stats:
                print(f"  {stat}")
    else:
        print("Player game stats table not found")
    
    print("\n" + "="*50)
    
    # Check for players with valid IDs
    if any('players' in table[0] for table in tables):
        cursor.execute("SELECT COUNT(*) FROM players WHERE player_id NOT LIKE 'BR_%'")
        valid_player_count = cursor.fetchone()[0]
        print(f"Players with valid IDs: {valid_player_count}")
        
        if valid_player_count > 0:
            cursor.execute("SELECT player_id, full_name, team_id FROM players WHERE player_id NOT LIKE 'BR_%' LIMIT 10")
            valid_players = cursor.fetchall()
            print("Sample valid players:")
            for player in valid_players:
                print(f"  {player}")
        else:
            print("No players with valid Basketball Reference IDs found")
    
    conn.close()

if __name__ == "__main__":
    check_database() 