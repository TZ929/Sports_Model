import sqlite3
from datetime import datetime

# Connect to database
conn = sqlite3.connect('data/sports_model.db')
cursor = conn.cursor()

# Get current date
today = datetime.now().strftime('%Y-%m-%d')
print(f"Today's date: {today}")

# Check for games with NULL scores
cursor.execute("""
    SELECT COUNT(*) 
    FROM mlb_games 
    WHERE home_team_score IS NULL OR away_team_score IS NULL
""")
null_scores = cursor.fetchone()[0]

# Check for games in the future
cursor.execute(f"""
    SELECT COUNT(*) 
    FROM mlb_games 
    WHERE game_date > '{today}'
""")
future_games = cursor.fetchone()[0]

# Check for games with NULL scores by year
cursor.execute("""
    SELECT SUBSTR(game_date, 1, 4) as year, 
           COUNT(*) as total_games,
           SUM(CASE WHEN home_team_score IS NULL OR away_team_score IS NULL THEN 1 ELSE 0 END) as null_score_games,
           SUM(CASE WHEN home_team_score IS NOT NULL AND away_team_score IS NOT NULL THEN 1 ELSE 0 END) as completed_games
    FROM mlb_games 
    GROUP BY year 
    ORDER BY year
""")
yearly_breakdown = cursor.fetchall()

# Check recent games with NULL scores
cursor.execute("""
    SELECT game_date, home_team_id, away_team_id, home_team_score, away_team_score
    FROM mlb_games 
    WHERE home_team_score IS NULL OR away_team_score IS NULL
    ORDER BY game_date DESC
    LIMIT 10
""")
recent_null_games = cursor.fetchall()

print(f"\n📊 Data Quality Check:")
print(f"   • Games with NULL scores: {null_scores:,}")
print(f"   • Future games: {future_games:,}")

print(f"\n📅 Games by Year:")
for year, total, null_games, completed in yearly_breakdown:
    completion_rate = (completed / total * 100) if total > 0 else 0
    print(f"   • {year}: {total:,} total, {completed:,} completed ({completion_rate:.1f}%), {null_games:,} incomplete")

if recent_null_games:
    print(f"\n🚫 Recent games with NULL scores:")
    for game_date, home, away, home_score, away_score in recent_null_games[:5]:
        print(f"   • {game_date}: {away} @ {home} ({home_score}-{away_score})")

# Check the latest completed game
cursor.execute("""
    SELECT MAX(game_date), home_team_id, away_team_id, home_team_score, away_team_score
    FROM mlb_games 
    WHERE home_team_score IS NOT NULL AND away_team_score IS NOT NULL
""")
latest_completed = cursor.fetchone()

print(f"\n✅ Latest completed game:")
if latest_completed[0]:
    print(f"   • {latest_completed[0]}: {latest_completed[2]} @ {latest_completed[1]} ({latest_completed[4]}-{latest_completed[3]})")

conn.close() 