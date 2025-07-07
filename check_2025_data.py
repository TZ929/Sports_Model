import sqlite3

# Connect to database
conn = sqlite3.connect('data/sports_model.db')
cursor = conn.cursor()

# Check for 2025 games
cursor.execute("SELECT COUNT(*) FROM mlb_games WHERE game_date >= '2025-01-01'")
count_2025 = cursor.fetchone()[0]

# Get date range for 2025
cursor.execute("SELECT MIN(game_date), MAX(game_date) FROM mlb_games WHERE game_date >= '2025-01-01'")
date_range_2025 = cursor.fetchone()

# Get all years available
cursor.execute("SELECT DISTINCT SUBSTR(game_date, 1, 4) as year FROM mlb_games ORDER BY year")
years = [row[0] for row in cursor.fetchall()]

print(f"2025 Games: {count_2025}")
if date_range_2025[0]:
    print(f"2025 Date Range: {date_range_2025[0]} to {date_range_2025[1]}")
else:
    print("No 2025 data found")

print(f"Available years: {years}")

# Check total games by year
cursor.execute("SELECT SUBSTR(game_date, 1, 4) as year, COUNT(*) FROM mlb_games GROUP BY year ORDER BY year")
yearly_counts = cursor.fetchall()
print("\nGames by year:")
for year, count in yearly_counts:
    print(f"  {year}: {count:,} games")

conn.close() 