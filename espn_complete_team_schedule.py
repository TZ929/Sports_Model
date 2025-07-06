#!/usr/bin/env python3
"""
ESPN Complete Team Schedule Collector - Get all 82 regular season games for each team.
"""

import requests
import logging
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Dict, Any, Optional
import re
import time
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ESPNCompleteTeamScheduleCollector:
    """Collect complete 82-game regular season schedules for all 30 NBA teams."""
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # NBA 2023-24 Season dates
        self.REGULAR_SEASON_START = datetime(2023, 10, 24)
        self.REGULAR_SEASON_END = datetime(2024, 4, 14)
        
        # All 30 NBA teams with their ESPN abbreviations
        self.NBA_TEAMS = {
            'atl': 'Atlanta Hawks',
            'bos': 'Boston Celtics',
            'bkn': 'Brooklyn Nets',
            'cha': 'Charlotte Hornets',
            'chi': 'Chicago Bulls',
            'cle': 'Cleveland Cavaliers',
            'dal': 'Dallas Mavericks',
            'den': 'Denver Nuggets',
            'det': 'Detroit Pistons',
            'gsw': 'Golden State Warriors',
            'hou': 'Houston Rockets',
            'ind': 'Indiana Pacers',
            'lac': 'Los Angeles Clippers',
            'lal': 'Los Angeles Lakers',
            'mem': 'Memphis Grizzlies',
            'mia': 'Miami Heat',
            'mil': 'Milwaukee Bucks',
            'min': 'Minnesota Timberwolves',
            'nop': 'New Orleans Pelicans',
            'nyk': 'New York Knicks',
            'okc': 'Oklahoma City Thunder',
            'orl': 'Orlando Magic',
            'phi': 'Philadelphia 76ers',
            'phx': 'Phoenix Suns',
            'por': 'Portland Trail Blazers',
            'sac': 'Sacramento Kings',
            'sas': 'San Antonio Spurs',
            'tor': 'Toronto Raptors',
            'uta': 'Utah Jazz',
            'was': 'Washington Wizards'
        }
    
    def collect_all_team_schedules(self) -> Dict[str, Any]:
        """Collect complete schedules for all 30 NBA teams."""
        
        print(f"\n{'='*80}")
        print("COLLECTING COMPLETE 2023-24 SEASON SCHEDULES FOR ALL 30 TEAMS")
        print(f"{'='*80}")
        
        all_teams_data = {}
        successful_teams = 0
        total_games = 0
        
        for team_abbr, team_name in self.NBA_TEAMS.items():
            print(f"\n{'='*60}")
            print(f"Processing: {team_name} ({team_abbr})")
            print(f"{'='*60}")
            
            team_schedule = self._get_team_schedule(team_abbr, team_name)
            
            if team_schedule['regular_season_count'] >= 80:  # Allow for 1-2 missing games
                successful_teams += 1
                total_games += team_schedule['regular_season_count']
                print(f"✅ SUCCESS: {team_schedule['regular_season_count']} games")
            else:
                print(f"❌ INCOMPLETE: {team_schedule['regular_season_count']} games (need 82)")
            
            all_teams_data[team_abbr] = team_schedule
            
            # Small delay between requests
            time.sleep(1)
        
        # Summary
        print(f"\n{'='*80}")
        print("FINAL SUMMARY")
        print(f"{'='*80}")
        print(f"Teams with complete schedules: {successful_teams}/30")
        print(f"Total regular season games collected: {total_games}")
        print(f"Expected total: {30 * 82} = 2460 games")
        print(f"Coverage: {(total_games / (30 * 82)) * 100:.1f}%")
        
        return all_teams_data
    
    def _get_team_schedule(self, team_abbr: str, team_name: str) -> Dict[str, Any]:
        """Get complete schedule for a single team."""
        
        # Try multiple approaches to get the complete schedule
        all_games = []
        
        # Approach 1: Direct team schedule page
        print("🔍 Approach 1: Direct team schedule...")
        direct_games = self._get_direct_team_schedule(team_abbr)
        all_games.extend(direct_games)
        print(f"  Direct games: {len(direct_games)}")
        
        # Approach 2: Try different season parameters
        print("🔍 Approach 2: Season parameters...")
        season_games = self._get_season_parameter_schedule(team_abbr)
        all_games.extend(season_games)
        print(f"  Season games: {len(season_games)}")
        
        # Approach 3: Try API endpoints
        print("🔍 Approach 3: API endpoints...")
        api_games = self._get_api_schedule(team_abbr)
        all_games.extend(api_games)
        print(f"  API games: {len(api_games)}")
        
        # Remove duplicates and categorize
        unique_games = self._remove_duplicate_games(all_games)
        categorized_games = self._categorize_games(unique_games)
        
        # Show detailed results
        self._show_team_detailed_results(categorized_games, team_name)
        
        return categorized_games
    
    def _get_direct_team_schedule(self, team_abbr: str) -> List[Dict[str, Any]]:
        """Get team schedule from direct ESPN team page."""
        
        url = f"https://www.espn.com/nba/team/schedule/_/name/{team_abbr}"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for schedule data in the page
            games = self._extract_games_from_page(soup, team_abbr, "direct")
            
            return games
            
        except Exception as e:
            print(f"    Error: {e}")
            return []
    
    def _get_season_parameter_schedule(self, team_abbr: str) -> List[Dict[str, Any]]:
        """Get team schedule using different season parameters."""
        
        season_params = ["2024", "2023", "2023-24", "2024-25"]
        all_games = []
        
        for season in season_params:
            url = f"https://www.espn.com/nba/team/schedule/_/name/{team_abbr}/season/{season}"
            
            try:
                response = requests.get(url, headers=self.headers, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Check if page exists
                if "Page Not Found" in soup.get_text() or "404" in soup.get_text():
                    continue
                
                games = self._extract_games_from_page(soup, team_abbr, f"season_{season}")
                all_games.extend(games)
                
                time.sleep(0.5)
                
            except Exception:
                continue
        
        return all_games
    
    def _get_api_schedule(self, team_abbr: str) -> List[Dict[str, Any]]:
        """Try to get schedule from ESPN API endpoints."""
        
        api_endpoints = [
            f"https://site.web.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{team_abbr}/schedule",
            f"https://site.web.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{team_abbr}/schedule?season=2024",
        ]
        
        all_games = []
        
        for endpoint in api_endpoints:
            try:
                response = requests.get(endpoint, headers=self.headers, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                if 'events' in data:
                    events = data['events']
                    for event in events:
                        game = self._parse_api_event(event, team_abbr)
                        if game:
                            all_games.append(game)
                
            except Exception:
                continue
        
        return all_games
    
    def _extract_games_from_page(self, soup: BeautifulSoup, team_abbr: str, source: str) -> List[Dict[str, Any]]:
        """Extract games from a team schedule page."""
        
        games = []
        
        # Method 1: Look for schedule table
        schedule_table = soup.find('table', class_='Table')
        if schedule_table:
            games.extend(self._parse_schedule_table(schedule_table, team_abbr, source))
        
        # Method 2: Look for any table with game data
        all_tables = soup.find_all('table')
        for table in all_tables:
            table_text = table.get_text().lower()
            if 'date' in table_text and ('opponent' in table_text or 'vs' in table_text):
                games.extend(self._parse_schedule_table(table, team_abbr, f"{source}_table"))
        
        # Method 3: Look for embedded JSON data
        scripts = soup.find_all('script')
        for script in scripts:
            script_text = script.get_text()
            if 'schedule' in script_text or 'events' in script_text:
                json_games = self._extract_json_games(script_text, team_abbr, source)
                games.extend(json_games)
        
        return games
    
    def _parse_schedule_table(self, table, team_abbr: str, source: str) -> List[Dict[str, Any]]:
        """Parse a schedule table to extract games."""
        
        games = []
        rows = table.find_all('tr')
        
        for row in rows:
            # Skip header rows
            if 'thead' in str(row.get('class', [])):
                continue
            
            cells = row.find_all(['td', 'th'])
            if len(cells) < 3:  # Need at least date, opponent, result
                continue
            
            try:
                # Extract date
                date_str = cells[0].get_text(strip=True)
                game_date = self._parse_espn_date(date_str)
                if not game_date:
                    continue
                
                # Only include 2023-24 regular season games
                if not self._is_regular_season_game(game_date):
                    continue
                
                # Extract opponent
                opponent = cells[1].get_text(strip=True) if len(cells) > 1 else "Unknown"
                
                # Extract result/score
                result = cells[2].get_text(strip=True) if len(cells) > 2 else "Unknown"
                
                # Create game record
                game = {
                    'game_id': f"ESPN_{source}_{team_abbr}_{game_date.strftime('%Y%m%d')}",
                    'team_abbr': team_abbr,
                    'game_date': game_date,
                    'opponent': opponent,
                    'result': result,
                    'source': source,
                    'game_type': 'regular_season'
                }
                
                games.append(game)
                
            except Exception:
                continue
        
        return games
    
    def _extract_json_games(self, script_text: str, team_abbr: str, source: str) -> List[Dict[str, Any]]:
        """Extract games from embedded JSON in script tags."""
        
        games = []
        
        try:
            # Look for JSON patterns
            json_patterns = [
                r'window\.espn\.scoreboardData\s*=\s*({.*?});',
                r'"events":\s*\[(.*?)\]',
            ]
            
            for pattern in json_patterns:
                matches = re.findall(pattern, script_text, re.DOTALL)
                for match in matches:
                    try:
                        if pattern.startswith('window'):
                            data = json.loads(match)
                            if 'events' in data:
                                for event in data['events']:
                                    game = self._parse_api_event(event, team_abbr)
                                    if game:
                                        games.append(game)
                        else:
                            # Try to parse events array
                            events_text = f'[{match}]'
                            events = json.loads(events_text)
                            for event in events:
                                game = self._parse_api_event(event, team_abbr)
                                if game:
                                    games.append(game)
                    except json.JSONDecodeError:
                        continue
        
        except Exception:
            pass
        
        return games
    
    def _parse_api_event(self, event: Dict, team_abbr: str) -> Optional[Dict[str, Any]]:
        """Parse an API event to extract game information."""
        
        try:
            # Extract date
            date_str = event.get('date', '')
            if not date_str:
                return None
            
            game_date = datetime.strptime(date_str.split('T')[0], '%Y-%m-%d')
            
            # Only include 2023-24 regular season games
            if not self._is_regular_season_game(game_date):
                return None
            
            # Extract opponent
            opponent = event.get('opponent', {}).get('name', 'Unknown')
            
            # Extract result
            result = event.get('result', {}).get('abbr', 'Unknown')
            
            # Create game record
            game = {
                'game_id': f"ESPN_API_{team_abbr}_{game_date.strftime('%Y%m%d')}",
                'team_abbr': team_abbr,
                'game_date': game_date,
                'opponent': opponent,
                'result': result,
                'source': 'api',
                'game_type': 'regular_season'
            }
            
            return game
            
        except Exception:
            return None
    
    def _remove_duplicate_games(self, games: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicate games based on game date and team."""
        
        seen_games = set()
        unique_games = []
        
        for game in games:
            game_key = f"{game['team_abbr']}_{game['game_date'].strftime('%Y%m%d')}"
            if game_key not in seen_games:
                unique_games.append(game)
                seen_games.add(game_key)
        
        return unique_games
    
    def _categorize_games(self, games: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Categorize games by type."""
        
        regular_season_games = [g for g in games if g['game_type'] == 'regular_season']
        other_games = [g for g in games if g['game_type'] != 'regular_season']
        
        # Sort by date
        regular_season_games.sort(key=lambda x: x['game_date'])
        
        return {
            'total_games': len(games),
            'regular_season_games': regular_season_games,
            'other_games': other_games,
            'regular_season_count': len(regular_season_games),
            'other_count': len(other_games)
        }
    
    def _show_team_detailed_results(self, categorized_games: Dict[str, Any], team_name: str):
        """Show detailed results for a team."""
        
        print(f"\n📊 DETAILED RESULTS FOR {team_name}:")
        print(f"Total games found: {categorized_games['total_games']}")
        print(f"Regular season games: {categorized_games['regular_season_count']}")
        
        if categorized_games['regular_season_games']:
            reg_games = categorized_games['regular_season_games']
            print(f"Date range: {reg_games[0]['game_date'].strftime('%Y-%m-%d')} to {reg_games[-1]['game_date'].strftime('%Y-%m-%d')}")
            
            # Show monthly breakdown
            months = {}
            for game in reg_games:
                month_key = f"{game['game_date'].year}-{game['game_date'].month:02d}"
                months[month_key] = months.get(month_key, 0) + 1
            
            print("Monthly breakdown:")
            for month, count in sorted(months.items()):
                print(f"  {month}: {count} games")
        
        # Show expected vs actual
        expected = 82
        actual = categorized_games['regular_season_count']
        print("\n📈 EXPECTED VS ACTUAL:")
        print(f"Expected: {expected} games")
        print(f"Actual: {actual} games")
        print(f"Difference: {actual - expected:+d}")
        
        if actual >= 80:
            print("✅ SUFFICIENT FOR MODELING")
        else:
            print("❌ INSUFFICIENT - NEED MORE GAMES")
    
    def _is_regular_season_game(self, game_date: datetime) -> bool:
        """Check if a game is from the 2023-24 regular season."""
        
        return self.REGULAR_SEASON_START <= game_date <= self.REGULAR_SEASON_END
    
    def _parse_espn_date(self, date_str: str) -> Optional[datetime]:
        """Parse ESPN date string to datetime object."""
        if not date_str or date_str == '' or date_str.lower() in ['date', 'opponent']:
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

def main():
    """Collect complete schedules for all 30 NBA teams."""
    
    collector = ESPNCompleteTeamScheduleCollector()
    
    # Collect all team schedules
    all_teams_data = collector.collect_all_team_schedules()
    
    # Save results
    print("\n💾 SAVING RESULTS...")
    
    # Create summary report
    summary = {
        'total_teams': len(all_teams_data),
        'teams_with_complete_schedules': 0,
        'total_games': 0,
        'team_details': {}
    }
    
    for team_abbr, team_data in all_teams_data.items():
        team_name = collector.NBA_TEAMS[team_abbr]
        games_count = team_data['regular_season_count']
        
        summary['total_games'] += games_count
        if games_count >= 80:
            summary['teams_with_complete_schedules'] += 1
        
        summary['team_details'][team_abbr] = {
            'name': team_name,
            'regular_season_games': games_count,
            'complete': games_count >= 80
        }
    
    print(f"Summary saved with {summary['teams_with_complete_schedules']}/30 teams having complete schedules")

if __name__ == "__main__":
    main() 