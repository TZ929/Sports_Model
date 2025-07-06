#!/usr/bin/env python3
"""
Verify Season Data - Check season filtering and compare against published games played counts.
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

class SeasonDataVerifier:
    """Verify season data and compare against published counts."""
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
    
    def verify_player_season_data(self, player_id: str, player_name: str) -> Dict[str, Any]:
        """Verify season data for a player and compare against published counts."""
        
        print(f"\n{'='*80}")
        print(f"VERIFYING SEASON DATA: {player_name} ({player_id})")
        print(f"{'='*80}")
        
        # Get all games from ESPN
        all_games = self._collect_all_games(player_id, player_name)
        
        # Analyze and categorize games
        analysis = self._analyze_games(all_games, player_name)
        
        # Get published games played count
        published_count = self._get_published_games_played(player_name)
        
        # Compare results
        self._compare_results(analysis, published_count, player_name)
        
        return analysis
    
    def _collect_all_games(self, player_id: str, player_name: str) -> List[Dict[str, Any]]:
        """Collect all games using the final collector approach."""
        
        print("🔍 Collecting all games from ESPN...")
        
        # Method 1: Multi-table extraction
        player_name_url = player_name.lower().replace(' ', '-')
        url = f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/{player_name_url}"
        
        all_games = []
        seen_game_ids = set()
        
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all tables
            tables = soup.find_all('table')
            print(f"Found {len(tables)} tables")
            
            for i, table in enumerate(tables):
                table_games = self._extract_games_from_table(table, player_id, f"T{i+1}")
                
                for game in table_games:
                    game_id = game['game_id']
                    if game_id not in seen_game_ids:
                        all_games.append(game)
                        seen_game_ids.add(game_id)
            
            # Method 2: Try alternative URL patterns
            url_patterns = [
                f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}",
                f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/type/1",
                f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/view/all",
                f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/season/2024/type/1",
            ]
            
            for i, alt_url in enumerate(url_patterns):
                try:
                    response = requests.get(alt_url, headers=self.headers, timeout=30)
                    response.raise_for_status()
                    
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    if "Page Not Found" in soup.get_text() or "404" in soup.get_text():
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
                    
                except Exception:
                    continue
            
            print(f"Total unique games collected: {len(all_games)}")
            return all_games
            
        except Exception as e:
            print(f"Error collecting games: {e}")
            return []
    
    def _extract_games_from_table(self, table, player_id: str, table_id: str) -> List[Dict[str, Any]]:
        """Extract games from a single table with detailed information."""
        
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
                game_type = self._determine_game_type(game_date, opponent, result)
                
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
    
    def _determine_game_type(self, game_date: datetime, opponent: str, result: str) -> str:
        """Determine if this is a regular season, playoff, pre-season, or summer league game."""
        
        # NBA Regular Season 2023-24: October 24, 2023 - April 14, 2024
        # NBA Playoffs 2024: April 20, 2024 - June 2024
        # NBA Pre-season: Usually October (before regular season starts)
        # Summer League: Usually July
        
        year = game_date.year
        month = game_date.month
        day = game_date.day
        
        # 2023-24 Season
        if year == 2023:
            if month == 10 and day >= 24:  # October 24 onwards
                return "regular_season"
            else:
                return "pre_season"
        
        elif year == 2024:
            if month <= 4:  # January to April
                if month == 4 and day >= 20:  # April 20 onwards = playoffs
                    return "playoffs"
                else:
                    return "regular_season"
            elif month >= 7:  # July onwards
                return "summer_league"
            else:
                return "playoffs"  # May-June = playoffs
        
        else:
            return "unknown"
    
    def _analyze_games(self, games: List[Dict[str, Any]], player_name: str) -> Dict[str, Any]:
        """Analyze and categorize the collected games."""
        
        print(f"\n📊 ANALYZING {len(games)} GAMES:")
        
        # Categorize by game type
        game_types = {}
        seasons = {}
        months = {}
        
        for game in games:
            game_type = game['game_type']
            game_date = game['game_date']
            
            # Count by game type
            game_types[game_type] = game_types.get(game_type, 0) + 1
            
            # Count by season (NBA season spans two calendar years)
            if game_date.year == 2023 and game_date.month >= 10:
                season_key = "2023-24"
            elif game_date.year == 2024 and game_date.month <= 6:
                season_key = "2023-24"
            else:
                season_key = f"{game_date.year}-{game_date.year + 1}"
            seasons[season_key] = seasons.get(season_key, 0) + 1
            
            # Count by month
            month_key = f"{game_date.year}-{game_date.month:02d}"
            months[month_key] = months.get(month_key, 0) + 1
        
        # Show breakdown
        print("\nGame Types:")
        for game_type, count in sorted(game_types.items()):
            print(f"  {game_type}: {count} games")
        
        print("\nSeasons:")
        for season, count in sorted(seasons.items()):
            print(f"  {season}: {count} games")
        
        print("\nMonths:")
        for month, count in sorted(months.items()):
            print(f"  {month}: {count} games")
        
        # Filter regular season games only (2023-24 season)
        regular_season_games = [g for g in games if g['game_type'] == 'regular_season']
        playoff_games = [g for g in games if g['game_type'] == 'playoffs']
        
        print(f"\n✅ 2023-24 REGULAR SEASON GAMES: {len(regular_season_games)}")
        print(f"🏆 2023-24 PLAYOFF GAMES: {len(playoff_games)}")
        
        # Show regular season date range
        if regular_season_games:
            dates = [g['game_date'] for g in regular_season_games]
            dates.sort()
            print(f"Regular season range: {dates[0].strftime('%Y-%m-%d')} to {dates[-1].strftime('%Y-%m-%d')}")
        
        # Show playoff date range
        if playoff_games:
            dates = [g['game_date'] for g in playoff_games]
            dates.sort()
            print(f"Playoff range: {dates[0].strftime('%Y-%m-%d')} to {dates[-1].strftime('%Y-%m-%d')}")
        
        return {
            'total_games': len(games),
            'regular_season_games': len(regular_season_games),
            'playoff_games': len(playoff_games),
            'game_types': game_types,
            'seasons': seasons,
            'months': months,
            'regular_season_games_list': regular_season_games,
            'playoff_games_list': playoff_games
        }
    
    def _get_published_games_played(self, player_name: str) -> Dict[str, Any]:
        """Get published games played count from ESPN or other sources."""
        
        print(f"\n🔍 LOOKING UP PUBLISHED GAMES PLAYED FOR {player_name}...")
        
        # Try to find the player's stats page
        search_url = f"https://www.espn.com/nba/player/_/name/{player_name.lower().replace(' ', '-')}"
        
        try:
            response = requests.get(search_url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for games played in stats
            games_played = None
            
            # Try to find games played in various formats
            text_content = soup.get_text()
            
            # Look for "GP" or "Games Played" patterns
            gp_patterns = [
                r'GP[:\s]*(\d+)',
                r'Games Played[:\s]*(\d+)',
                r'Games[:\s]*(\d+)',
            ]
            
            for pattern in gp_patterns:
                match = re.search(pattern, text_content, re.IGNORECASE)
                if match:
                    games_played = int(match.group(1))
                    print(f"Found games played: {games_played}")
                    break
            
            if not games_played:
                print("Could not find published games played count")
                return {'games_played': None, 'source': 'not_found'}
            
            return {'games_played': games_played, 'source': 'espn'}
            
        except Exception as e:
            print(f"Error looking up published count: {e}")
            return {'games_played': None, 'source': 'error'}
    
    def _compare_results(self, analysis: Dict[str, Any], published: Dict[str, Any], player_name: str):
        """Compare our results with published counts."""
        
        print(f"\n📈 COMPARISON FOR {player_name}:")
        
        our_regular_count = analysis['regular_season_games']
        our_playoff_count = analysis['playoff_games']
        published_count = published.get('games_played')
        
        print(f"Our 2023-24 regular season games: {our_regular_count}")
        print(f"Our 2023-24 playoff games: {our_playoff_count}")
        print(f"Published games played (regular season): {published_count}")
        
        if published_count:
            difference = our_regular_count - published_count
            percentage = (our_regular_count / published_count) * 100 if published_count > 0 else 0
            
            print(f"Regular season difference: {difference:+d}")
            print(f"Regular season coverage: {percentage:.1f}%")
            
            if abs(difference) <= 2:
                print("✅ EXCELLENT MATCH - Regular season data looks accurate!")
            elif abs(difference) <= 5:
                print("⚠️ GOOD MATCH - Minor differences acceptable")
            else:
                print("❌ SIGNIFICANT DIFFERENCE - Data may need verification")
        else:
            print("⚠️ No published count available for comparison")
        
        # Show expected ranges
        print("\n📋 EXPECTED RANGES:")
        print("Regular season: 60-82 games (most players)")
        print("Playoffs: 0-28 games (depends on team success)")
        print(f"Total 2023-24 season: {our_regular_count + our_playoff_count} games")
    
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
    """Verify season data for sample players."""
    
    verifier = SeasonDataVerifier()
    
    # Test with players we know have data
    test_players = [
        ("3138196", "Cameron Johnson"),
        ("4432174", "Cam Thomas"),
    ]
    
    for player_id, player_name in test_players:
        analysis = verifier.verify_player_season_data(player_id, player_name)
        
        print(f"\n{'='*80}")
        print(f"FINAL SUMMARY FOR {player_name}:")
        print(f"{'='*80}")
        print(f"Total games collected: {analysis['total_games']}")
        print(f"Regular season games: {analysis['regular_season_games']}")
        print(f"Game types: {analysis['game_types']}")
        print(f"Seasons: {analysis['seasons']}")

if __name__ == "__main__":
    main() 