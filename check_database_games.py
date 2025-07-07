import sqlite3
import pandas as pd

# Connect to database
conn = sqlite3.connect('data/sports_model.db')

# Check what tables exist
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = cursor.fetchall()
print("All tables in database:")
for table in tables:
    print(f"  - {table[0]}")

print("\n" + "="*50)

# Check mlb_games table structure
cursor.execute("PRAGMA table_info(mlb_games)")
columns = cursor.fetchall()
print("mlb_games table structure:")
for col in columns:
    print(f"  - {col[1]} ({col[2]})")

print("\n" + "="*50)

# Check game data availability
cursor.execute("SELECT COUNT(*) FROM mlb_games")
total_games = cursor.fetchone()[0]
print(f"Total games in mlb_games: {total_games}")

cursor.execute("SELECT COUNT(*) FROM mlb_games WHERE home_team_score IS NOT NULL")
games_with_scores = cursor.fetchone()[0]
print(f"Games with scores: {games_with_scores}")

cursor.execute("SELECT COUNT(*) FROM mlb_games WHERE home_team_score IS NOT NULL AND away_team_score IS NOT NULL")
complete_games = cursor.fetchone()[0]
print(f"Complete games (both scores): {complete_games}")

print("\n" + "="*50)

# Check date range
cursor.execute("SELECT MIN(game_date), MAX(game_date) FROM mlb_games WHERE home_team_score IS NOT NULL")
date_range = cursor.fetchone()
print(f"Date range of games with scores: {date_range[0]} to {date_range[1]}")

print("\n" + "="*50)

# Show sample of games with scores
print("Sample games with scores:")
cursor.execute("""
    SELECT game_date, home_team_id, away_team_id, home_team_score, away_team_score 
    FROM mlb_games 
    WHERE home_team_score IS NOT NULL 
    AND away_team_score IS NOT NULL
    ORDER BY game_date DESC 
    LIMIT 10
""")
sample_games = cursor.fetchall()
for game in sample_games:
    print(f"  {game[0]}: {game[1]} vs {game[2]} -> {game[3]}-{game[4]}")

conn.close() 