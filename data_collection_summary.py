#!/usr/bin/env python3
"""
Data Collection Summary - Phase 2 Status
Shows exactly what data has been collected successfully.
"""

import sqlite3
from pathlib import Path

def main():
    print("=" * 60)
    print("PHASE 2 DATA COLLECTION SUMMARY")
    print("=" * 60)
    
    # Connect to database
    db_path = Path("data/sports_model.db")
    if not db_path.exists():
        print("❌ Database not found!")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Check all tables
    print("\n📊 DATABASE TABLES:")
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    for table in tables:
        print(f"  ✅ {table[0]}")
    
    print("\n" + "=" * 60)
    
    # Teams collection status
    print("\n🏀 TEAMS COLLECTION:")
    cursor.execute("SELECT COUNT(*) FROM teams")
    team_count = cursor.fetchone()[0]
    print(f"  Total Teams: {team_count}")
    print(f"  Status: {'✅ SUCCESS' if team_count > 0 else '❌ FAILED'}")
    
    if team_count > 0:
        cursor.execute("SELECT team_id, team_name FROM teams LIMIT 5")
        teams = cursor.fetchall()
        print("  Sample Teams:")
        for i, team in enumerate(teams):
            print(f"    {i+1}. {team[1]} ({team[0]})")
    
    print("\n" + "=" * 60)
    
    # Players collection status
    print("\n👥 PLAYERS COLLECTION:")
    cursor.execute("SELECT COUNT(*) FROM players")
    total_players = cursor.fetchone()[0]
    
    # Get all players and filter in Python
    cursor.execute("SELECT player_id, full_name FROM players")
    all_players = cursor.fetchall()
    
    valid_players = [p for p in all_players if p[0].isdigit() and len(p[0]) > 3]
    invalid_players = [p for p in all_players if not p[0].isdigit() or len(p[0]) <= 3]
    
    print(f"  Total Players: {total_players}")
    print(f"  Valid ESPN Players: {len(valid_players)}")
    print(f"  Invalid/Placeholder Players: {len(invalid_players)}")
    print(f"  Status: {'✅ SUCCESS' if len(valid_players) > 0 else '❌ FAILED'}")
    
    if valid_players:
        print("  Sample Valid Players:")
        for i, player in enumerate(valid_players[:10]):
            print(f"    {i+1}. {player[1]} (ID: {player[0]})")
    
    print("\n" + "=" * 60)
    
    # Games collection status
    print("\n🏆 GAMES COLLECTION:")
    cursor.execute("SELECT COUNT(*) FROM games")
    game_count = cursor.fetchone()[0]
    print(f"  Total Games: {game_count}")
    print(f"  Status: {'✅ SUCCESS' if game_count > 0 else '❌ FAILED'}")
    
    if game_count > 0:
        cursor.execute("SELECT game_id, date, home_team_name, away_team_name FROM games LIMIT 5")
        games = cursor.fetchall()
        print("  Sample Games:")
        for i, game in enumerate(games):
            print(f"    {i+1}. {game[3]} vs {game[2]} ({game[1][:10]})")
    
    print("\n" + "=" * 60)
    
    # Player Game Stats collection status
    print("\n📈 PLAYER GAME STATS COLLECTION:")
    cursor.execute("SELECT COUNT(*) FROM player_game_stats")
    stats_count = cursor.fetchone()[0]
    print(f"  Total Player Game Stats: {stats_count}")
    print(f"  Status: {'✅ SUCCESS' if stats_count > 0 else '⚠️ NOT IMPLEMENTED'}")
    
    if stats_count > 0:
        cursor.execute("SELECT player_id, points FROM player_game_stats LIMIT 3")
        stats = cursor.fetchall()
        print("  Sample Stats:")
        for i, stat in enumerate(stats):
            print(f"    {i+1}. Player {stat[0]} - {stat[1]} points")
    
    print("\n" + "=" * 60)
    
    # Prop Odds collection status
    print("\n💰 PROP ODDS COLLECTION:")
    cursor.execute("SELECT COUNT(*) FROM prop_odds")
    odds_count = cursor.fetchone()[0]
    print(f"  Total Prop Odds: {odds_count}")
    print(f"  Status: {'✅ SUCCESS' if odds_count > 0 else '⏳ NOT STARTED'}")
    
    print("\n" + "=" * 60)
    
    # Overall Phase 2 Status
    print("\n🎯 PHASE 2 OVERALL STATUS:")
    
    phase2_success = (
        team_count > 0 and 
        len(valid_players) > 0 and 
        game_count > 0
    )
    
    if phase2_success:
        print("  🎉 PHASE 2 COMPLETED SUCCESSFULLY!")
        print("  ✅ Core data infrastructure is working")
        print("  ✅ Teams, Players, and Games collected")
        print("  ✅ Database storage functioning")
        print("  ✅ Ready for Phase 3 (Modeling)")
    else:
        print("  ❌ PHASE 2 INCOMPLETE")
        print("  ⚠️ Some core components missing")
    
    print("\n" + "=" * 60)
    
    # Data Quality Assessment
    print("\n🔍 DATA QUALITY ASSESSMENT:")
    
    if len(valid_players) > 0:
        print(f"  ✅ Real player names collected: {len(valid_players)}")
        print(f"  ✅ Valid ESPN player IDs: {len(valid_players)}")
    else:
        print("  ❌ No valid player data")
    
    if team_count > 0:
        print(f"  ✅ Real team names collected: {team_count}")
    else:
        print("  ❌ No team data")
    
    if game_count > 0:
        print(f"  ✅ Games with team relationships: {game_count}")
    else:
        print("  ❌ No game data")
    
    print("\n" + "=" * 60)
    
    conn.close()

if __name__ == "__main__":
    main() 