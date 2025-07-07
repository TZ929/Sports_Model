import sqlite3
from datetime import datetime

# Connect to database
conn = sqlite3.connect('data/sports_model.db')
cursor = conn.cursor()

# Get current date
today = datetime.now().strftime('%Y-%m-%d')
print(f"Today's date: {today}")

# Check all games after today
cursor.execute(f"""
    SELECT game_date, home_team_id, away_team_id, home_team_score, away_team_score
    FROM mlb_games 
    WHERE game_date > '{today}'
    ORDER BY game_date
    LIMIT 20
""")
future_games = cursor.fetchall()

print(f"\n🔍 Future Games (first 20):")
for game_date, home, away, home_score, away_score in future_games:
    print(f"   • {game_date}: {away} @ {home} (Score: {home_score}-{away_score})")

# Check games from recent past to see pattern
cursor.execute(f"""
    SELECT game_date, home_team_id, away_team_id, home_team_score, away_team_score
    FROM mlb_games 
    WHERE game_date BETWEEN '2025-06-01' AND '2025-07-10'
    ORDER BY game_date DESC
    LIMIT 20
""")
recent_games = cursor.fetchall()

print(f"\n📅 Recent Games (June-July 2025):")
for game_date, home, away, home_score, away_score in recent_games:
    print(f"   • {game_date}: {away} @ {home} (Score: {home_score}-{away_score})")

# Check for games with 0-0 scores
cursor.execute("""
    SELECT COUNT(*) 
    FROM mlb_games 
    WHERE home_team_score = 0 AND away_team_score = 0
""")
zero_zero_games = cursor.fetchone()[0]

# Check 0-0 games by date
cursor.execute(f"""
    SELECT game_date, home_team_id, away_team_id
    FROM mlb_games 
    WHERE home_team_score = 0 AND away_team_score = 0
    AND game_date > '2025-06-01'
    ORDER BY game_date
    LIMIT 10
""")
zero_zero_recent = cursor.fetchall()

print(f"\n🚨 Games with 0-0 scores: {zero_zero_games:,}")
if zero_zero_recent:
    print(f"Recent 0-0 games:")
    for game_date, home, away in zero_zero_recent:
        print(f"   • {game_date}: {away} @ {home}")

# Check for any NULL values in different ways
cursor.execute("""
    SELECT COUNT(*) 
    FROM mlb_games 
    WHERE home_team_score IS NULL
""")
null_home = cursor.fetchone()[0]

cursor.execute("""
    SELECT COUNT(*) 
    FROM mlb_games 
    WHERE away_team_score IS NULL
""")
null_away = cursor.fetchone()[0]

cursor.execute("""
    SELECT COUNT(*) 
    FROM mlb_games 
    WHERE home_team_score = '' OR away_team_score = ''
""")
empty_scores = cursor.fetchone()[0]

print(f"\n🔍 NULL/Empty Score Analysis:")
print(f"   • NULL home scores: {null_home}")
print(f"   • NULL away scores: {null_away}")
print(f"   • Empty string scores: {empty_scores}")

# Check the database schema for the scores columns
cursor.execute("PRAGMA table_info(mlb_games)")
columns = cursor.fetchall()

print(f"\n📋 Database Schema for mlb_games:")
for col in columns:
    if 'score' in col[1].lower():
        print(f"   • {col[1]}: {col[2]} (nullable: {col[3] == 0})")

# Check for actual real vs future games pattern
cursor.execute("""
    SELECT 
        CASE 
            WHEN game_date <= date('now') THEN 'Past/Today'
            ELSE 'Future'
        END as time_category,
        COUNT(*) as count,
        AVG(CAST(home_team_score AS FLOAT)) as avg_home_score,
        AVG(CAST(away_team_score AS FLOAT)) as avg_away_score,
        SUM(CASE WHEN home_team_score = 0 AND away_team_score = 0 THEN 1 ELSE 0 END) as zero_zero_count
    FROM mlb_games
    GROUP BY time_category
""")
time_analysis = cursor.fetchall()

print(f"\n⏰ Past vs Future Game Analysis:")
for category, count, avg_home, avg_away, zero_count in time_analysis:
    print(f"   • {category}: {count:,} games, avg scores: {avg_home:.1f}-{avg_away:.1f}, 0-0 games: {zero_count}")

conn.close() 