import sqlite3

conn = sqlite3.connect('data/sports_model.db')
cursor = conn.cursor()

cursor.execute("SELECT player_id, full_name, team_id FROM players WHERE player_id NOT LIKE 'BR_%' LIMIT 10")
valid_players = cursor.fetchall()

print("Valid ESPN players:")
if not valid_players:
    print("  None found.")
for player in valid_players:
    print(f"  {player}")

conn.close() 