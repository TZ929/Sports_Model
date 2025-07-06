#!/usr/bin/env python3
"""
ESPN Complete 2023-24 Season Collector - Get the full 2023-24 NBA season data.
"""

import requests
import logging
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Dict, Any, Optional
import re
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ESPNComplete202324Collector:
    """Collect complete 2023-24 NBA season data with proper filtering."""
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # NBA 2023-24 Season dates
        self.REGULAR_SEASON_START = datetime(2023, 10, 24)
        self.REGULAR_SEASON_END = datetime(2024, 4, 14)
        self.PLAYOFFS_START = datetime(2024, 4, 20)
        self.PLAYOFFS_END = datetime(2024, 6, 30)
    
    def get_complete_2023_24_season(self, player_id: str, player_name: str) -> Dict[str, Any]:
        """Get complete 2023-24 season data with proper categorization."""
        
        print(f"\n{'='*80}")
        print(f"COLLECTING COMPLETE 2023-24 SEASON: {player_name} ({player_id})")
        print(f"{'='*80}")
        
        # Try multiple approaches to get complete season data
        all_games = []
        seen_game_ids = set()
        
        # Approach 1: Multi-table extraction
        print("\n🔍 APPROACH 1: Multi-table extraction...")
        multi_table_games = self._extract_from_all_tables(player_id, player_name)
        for game in multi_table_games:
            if game['game_id'] not in seen_game_ids:
                all_games.append(game)
                seen_game_ids.add(game['game_id'])
        print(f"Multi-table games: {len(multi_table_games)}")
        
        # Approach 2: Alternative URL patterns
        print("\n🔍 APPROACH 2: Alternative URL patterns...")
        url_pattern_games = self._try_url_patterns(player_id, player_name)
        for game in url_pattern_games:
            if game['game_id'] not in seen_game_ids:
                all_games.append(game)
                seen_game_ids.add(game['game_id'])
        print(f"URL pattern games: {len(url_pattern_games)}")
        
        # Approach 3: Try season-specific URLs
        print("\n🔍 APPROACH 3: Season-specific URLs...")
        season_specific_games = self._try_season_specific_urls(player_id, player_name)
        for game in season_specific_games:
            if game['game_id'] not in seen_game_ids:
                all_games.append(game)
                seen_game_ids.add(game['game_id'])
        print(f"Season-specific games: {len(season_specific_games)}")
        
        # Categorize and filter games
        print(f"\n📊 TOTAL GAMES COLLECTED: {len(all_games)}")
        
        categorized_games = self._categorize_games(all_games)
        
        # Show results
        self._show_results(categorized_games, player_name)
        
        return categorized_games
    
    def _extract_from_all_tables(self, player_id: str, player_name: str) -> List[Dict[str, Any]]:
        """Extract data from all tables on ESPN page."""
        
        player_name_url = player_name.lower().replace(' ', '-')
        url = f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/{player_name_url}"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all tables
            tables = soup.find_all('table')
            print(f"  Found {len(tables)} tables")
            
            all_games = []
            seen_game_ids = set()
            
            for i, table in enumerate(tables):
                table_games = self._extract_games_from_table(table, player_id, f"T{i+1}")
                
                for game in table_games:
                    game_id = game['game_id']
                    if game_id not in seen_game_ids:
                        all_games.append(game)
                        seen_game_ids.add(game_id)
            
            return all_games
            
        except Exception as e:
            print(f"  Error: {e}")
            return []
    
    def _try_url_patterns(self, player_id: str, player_name: str) -> List[Dict[str, Any]]:
        """Try different URL patterns to get more data."""
        
        url_patterns = [
            f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}",
            f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/type/1",
            f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/view/all",
            f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/season/2024/type/1",
        ]
        
        all_games = []
        seen_game_ids = set()
        
        for i, url in enumerate(url_patterns):
            try:
                print(f"  Trying URL {i+1}: {url}")
                response = requests.get(url, headers=self.headers, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                if "Page Not Found" in soup.get_text() or "404" in soup.get_text():
                    print("    Page not found")
                    continue
                
                # Find game log table
                game_log_table = None
                for selector in ['table.Table', 'table[class*="gamelog"]', 'table']:
                    game_log_table = soup.select_one(selector)
                    if game_log_table:
                        break
                
                if game_log_table:
                    table_games = self._extract_games_from_table(game_log_table, player_id, f"URL{i+1}")
                    
                    for game in table_games:
                        game_id = game['game_id']
                        if game_id not in seen_game_ids:
                            all_games.append(game)
                            seen_game_ids.add(game_id)
                
                time.sleep(1)
                
            except Exception as e:
                print(f"    Error: {e}")
                continue
        
        return all_games
    
    def _try_season_specific_urls(self, player_id: str, player_name: str) -> List[Dict[str, Any]]:
        """Try season-specific URLs to get 2023-24 data."""
        
        # Try different season parameters
        season_urls = [
            f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/season/2023",
            f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/season/2024",
            f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/season/2023-24",
            f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/year/2023",
            f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/year/2024",
        ]
        
        all_games = []
        seen_game_ids = set()
        
        for i, url in enumerate(season_urls):
            try:
                print(f"  Trying season URL {i+1}: {url}")
                response = requests.get(url, headers=self.headers, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                if "Page Not Found" in soup.get_text() or "404" in soup.get_text():
                    print("    Page not found")
                    continue
                
                # Find game log table
                game_log_table = None
                for selector in ['table.Table', 'table[class*="gamelog"]', 'table']:
                    game_log_table = soup.select_one(selector)
                    if game_log_table:
                        break
                
                if game_log_table:
                    table_games = self._extract_games_from_table(game_log_table, player_id, f"SEASON{i+1}")
                    
                    for game in table_games:
                        game_id = game['game_id']
                        if game_id not in seen_game_ids:
                            all_games.append(game)
                            seen_game_ids.add(game_id)
                
                time.sleep(1)
                
            except Exception as e:
                print(f"    Error: {e}")
                continue
        
        return all_games
    
    def _extract_games_from_table(self, table, player_id: str, table_id: str) -> List[Dict[str, Any]]:
        """Extract games from a single table with proper 2023-24 filtering."""
        
        games = []
        rows = table.find_all('tr')
        
        # Check if this looks like a game log table
        table_text = table.get_text().lower()
        if 'date' not in table_text or ('opp' not in table_text and 'opponent' not in table_text):
            return games  # Not a game log table
        
        for row in rows:
            # Skip header rows
            if 'thead' in str(row.get('class', [])):
                continue
            
            cells = row.find_all(['td', 'th'])
            if len(cells) < 10:  # Need at least basic stats
                continue
            
            try:
                # Extract game information
                date_str = cells[0].get_text(strip=True)
                game_date = self._parse_espn_date(date_str)
                if not game_date:
                    continue
                
                # Only include 2023-24 season games
                if not self._is_2023_24_season_game(game_date):
                    continue
                
                opponent = cells[1].get_text(strip=True)
                result = cells[2].get_text(strip=True)
                minutes = self._parse_minutes(cells[3].get_text(strip=True))
                
                # Parse shooting stats
                fg_str = cells[4].get_text(strip=True)
                fg_made, fg_attempted = self._parse_shot_attempts(fg_str)
                
                three_pt_str = cells[6].get_text(strip=True)
                three_made, three_attempted = self._parse_shot_attempts(three_pt_str)
                
                ft_str = cells[8].get_text(strip=True)
                ft_made, ft_attempted = self._parse_shot_attempts(ft_str)
                
                # Other stats
                rebounds = int(cells[10].get_text(strip=True)) if cells[10].get_text(strip=True).isdigit() else 0
                assists = int(cells[11].get_text(strip=True)) if cells[11].get_text(strip=True).isdigit() else 0
                steals = int(cells[12].get_text(strip=True)) if cells[12].get_text(strip=True).isdigit() else 0
                blocks = int(cells[13].get_text(strip=True)) if cells[13].get_text(strip=True).isdigit() else 0
                turnovers = int(cells[14].get_text(strip=True)) if cells[14].get_text(strip=True).isdigit() else 0
                personal_fouls = int(cells[15].get_text(strip=True)) if cells[15].get_text(strip=True).isdigit() else 0
                points = int(cells[16].get_text(strip=True)) if cells[16].get_text(strip=True).isdigit() else 0
                
                # Plus/minus
                plus_minus = 0
                if len(cells) > 17:
                    pm_text = cells[17].get_text(strip=True)
                    if pm_text and pm_text != '-':
                        try:
                            plus_minus = int(pm_text)
                        except ValueError:
                            plus_minus = 0
                
                # Determine game type
                game_type = self._determine_game_type(game_date)
                
                # Create game record
                game = {
                    'game_id': f"ESPN_{table_id}_{player_id}_{game_date.strftime('%Y%m%d')}",
                    'player_id': player_id,
                    'game_date': game_date,
                    'opponent': opponent,
                    'result': result,
                    'game_type': game_type,
                    'minutes_played': int(minutes) if minutes else 0,
                    'field_goals_made': fg_made,
                    'field_goals_attempted': fg_attempted,
                    'three_pointers_made': three_made,
                    'three_pointers_attempted': three_attempted,
                    'free_throws_made': ft_made,
                    'free_throws_attempted': ft_attempted,
                    'rebounds': rebounds,
                    'assists': assists,
                    'steals': steals,
                    'blocks': blocks,
                    'turnovers': turnovers,
                    'personal_fouls': personal_fouls,
                    'points': points,
                    'plus_minus': plus_minus
                }
                
                games.append(game)
                
            except (ValueError, IndexError):
                continue
        
        return games
    
    def _is_2023_24_season_game(self, game_date: datetime) -> bool:
        """Check if a game is from the 2023-24 NBA season."""
        
        # 2023-24 season: October 24, 2023 to June 2024
        if game_date.year == 2023:
            return game_date.month >= 10 and game_date.day >= 24
        elif game_date.year == 2024:
            return game_date.month <= 6
        else:
            return False
    
    def _determine_game_type(self, game_date: datetime) -> str:
        """Determine if this is a regular season or playoff game."""
        
        if self.REGULAR_SEASON_START <= game_date <= self.REGULAR_SEASON_END:
            return "regular_season"
        elif self.PLAYOFFS_START <= game_date <= self.PLAYOFFS_END:
            return "playoffs"
        else:
            return "other"
    
    def _categorize_games(self, games: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Categorize games by type and show detailed breakdown."""
        
        regular_season_games = [g for g in games if g['game_type'] == 'regular_season']
        playoff_games = [g for g in games if g['game_type'] == 'playoffs']
        other_games = [g for g in games if g['game_type'] == 'other']
        
        # Sort by date
        regular_season_games.sort(key=lambda x: x['game_date'])
        playoff_games.sort(key=lambda x: x['game_date'])
        
        return {
            'total_games': len(games),
            'regular_season_games': regular_season_games,
            'playoff_games': playoff_games,
            'other_games': other_games,
            'regular_season_count': len(regular_season_games),
            'playoff_count': len(playoff_games),
            'other_count': len(other_games)
        }
    
    def _show_results(self, categorized_games: Dict[str, Any], player_name: str):
        """Show detailed results of the collection."""
        
        print(f"\n📊 FINAL RESULTS FOR {player_name}:")
        print(f"{'='*60}")
        
        print(f"Total games collected: {categorized_games['total_games']}")
        print(f"Regular season games: {categorized_games['regular_season_count']}")
        print(f"Playoff games: {categorized_games['playoff_count']}")
        print(f"Other games: {categorized_games['other_count']}")
        
        # Show regular season details
        if categorized_games['regular_season_games']:
            reg_games = categorized_games['regular_season_games']
            print("\n✅ 2023-24 REGULAR SEASON:")
            print(f"Date range: {reg_games[0]['game_date'].strftime('%Y-%m-%d')} to {reg_games[-1]['game_date'].strftime('%Y-%m-%d')}")
            
            # Show monthly breakdown
            months = {}
            for game in reg_games:
                month_key = f"{game['game_date'].year}-{game['game_date'].month:02d}"
                months[month_key] = months.get(month_key, 0) + 1
            
            print("Monthly breakdown:")
            for month, count in sorted(months.items()):
                print(f"  {month}: {count} games")
        
        # Show playoff details
        if categorized_games['playoff_games']:
            playoff_games = categorized_games['playoff_games']
            print("\n🏆 2023-24 PLAYOFFS:")
            print(f"Date range: {playoff_games[0]['game_date'].strftime('%Y-%m-%d')} to {playoff_games[-1]['game_date'].strftime('%Y-%m-%d')}")
        
        # Show expected vs actual
        print("\n📈 EXPECTED VS ACTUAL:")
        print("Expected regular season: 60-82 games")
        print(f"Actual regular season: {categorized_games['regular_season_count']} games")
        
        if categorized_games['regular_season_count'] < 60:
            print("⚠️ WARNING: Missing early season games (Oct-Dec 2023)")
        elif categorized_games['regular_season_count'] > 82:
            print("⚠️ WARNING: More games than expected - may include duplicates")
        else:
            print("✅ Regular season count looks reasonable")
    
    def _parse_espn_date(self, date_str: str) -> Optional[datetime]:
        """Parse ESPN date string to datetime object."""
        if not date_str or date_str == '' or date_str.lower() == 'date':
            return None
        
        try:
            # ESPN format: "Oct 25, 2023"
            return datetime.strptime(date_str, '%b %d, %Y')
        except ValueError:
            try:
                # Alternative format: "10/25/2023"
                return datetime.strptime(date_str, '%m/%d/%Y')
            except ValueError:
                try:
                    # Current ESPN format: "Tue 4/15" (assume 2024)
                    match = re.search(r'(\d{1,2})/(\d{1,2})', date_str)
                    if match:
                        month = int(match.group(1))
                        day = int(match.group(2))
                        return datetime(2024, month, day)
                except (ValueError, AttributeError):
                    pass
                
                return None
    
    def _parse_minutes(self, minutes_str: str) -> Optional[float]:
        """Parse minutes played string to total minutes."""
        if not minutes_str or minutes_str == '':
            return None
        
        try:
            parts = minutes_str.split(':')
            if len(parts) == 2:
                minutes = int(parts[0])
                seconds = int(parts[1])
                return minutes + (seconds / 60)
            return None
        except (ValueError, IndexError):
            return None
    
    def _parse_shot_attempts(self, shot_str: str) -> tuple:
        """Parse shot attempts string (e.g., "8-15") to made and attempted."""
        if not shot_str or shot_str == '' or shot_str == '-':
            return (0, 0)
        
        try:
            if '-' in shot_str:
                made, attempted = shot_str.split('-')
                return (int(made), int(attempted))
            else:
                return (int(shot_str), 0)
        except (ValueError, IndexError):
            return (0, 0)

def main():
    """Test the complete 2023-24 season collector."""
    
    collector = ESPNComplete202324Collector()
    
    # Test with players we know have data
    test_players = [
        ("3138196", "Cameron Johnson"),
        ("4432174", "Cam Thomas"),
    ]
    
    for player_id, player_name in test_players:
        results = collector.get_complete_2023_24_season(player_id, player_name)
        
        print(f"\n{'='*80}")
        print(f"SUMMARY FOR {player_name}:")
        print(f"{'='*80}")
        print(f"Regular season: {results['regular_season_count']} games")
        print(f"Playoffs: {results['playoff_count']} games")
        print(f"Total 2023-24: {results['regular_season_count'] + results['playoff_count']} games")

if __name__ == "__main__":
    main() 