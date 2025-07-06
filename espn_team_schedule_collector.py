#!/usr/bin/env python3
"""
ESPN Team Schedule Collector - Use team schedules to get complete 2023-24 season data.
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

class ESPNTeamScheduleCollector:
    """Collect complete 2023-24 season data using team schedules."""
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # NBA 2023-24 Season dates
        self.REGULAR_SEASON_START = datetime(2023, 10, 24)
        self.REGULAR_SEASON_END = datetime(2024, 4, 14)
        self.PLAYOFFS_START = datetime(2024, 4, 20)
        self.PLAYOFFS_END = datetime(2024, 6, 30)
        
        # Team abbreviations
        self.TEAM_ABBREVIATIONS = {
            'bkn': 'Brooklyn Nets',
            'bos': 'Boston Celtics',
            'nyk': 'New York Knicks',
            'phi': 'Philadelphia 76ers',
            'tor': 'Toronto Raptors',
            'chi': 'Chicago Bulls',
            'cle': 'Cleveland Cavaliers',
            'det': 'Detroit Pistons',
            'ind': 'Indiana Pacers',
            'mil': 'Milwaukee Bucks',
            'atl': 'Atlanta Hawks',
            'cha': 'Charlotte Hornets',
            'mia': 'Miami Heat',
            'orl': 'Orlando Magic',
            'was': 'Washington Wizards',
            'den': 'Denver Nuggets',
            'min': 'Minnesota Timberwolves',
            'okc': 'Oklahoma City Thunder',
            'por': 'Portland Trail Blazers',
            'uta': 'Utah Jazz',
            'gsw': 'Golden State Warriors',
            'lac': 'Los Angeles Clippers',
            'lal': 'Los Angeles Lakers',
            'phx': 'Phoenix Suns',
            'sac': 'Sacramento Kings',
            'dal': 'Dallas Mavericks',
            'hou': 'Houston Rockets',
            'mem': 'Memphis Grizzlies',
            'nop': 'New Orleans Pelicans',
            'sas': 'San Antonio Spurs',
        }
    
    def get_team_complete_schedule(self, team_abbr: str, season: str = "2024") -> Dict[str, Any]:
        """Get complete team schedule for the 2023-24 season."""
        
        print(f"\n{'='*80}")
        print(f"COLLECTING TEAM SCHEDULE: {self.TEAM_ABBREVIATIONS.get(team_abbr, team_abbr.upper())} ({team_abbr})")
        print(f"{'='*80}")
        
        # Try different season parameters
        seasons_to_try = ["2024", "2023", "2023-24"]
        
        all_games = []
        
        for season_param in seasons_to_try:
            print(f"\n🔍 Trying season: {season_param}")
            
            url = f"https://www.espn.com/nba/team/schedule/_/name/{team_abbr}/season/{season_param}"
            print(f"URL: {url}")
            
            try:
                response = requests.get(url, headers=self.headers, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Check if page exists
                if "Page Not Found" in soup.get_text() or "404" in soup.get_text():
                    print("  Page not found")
                    continue
                
                # Extract games from this page
                page_games = self._extract_games_from_schedule_page(soup, team_abbr, season_param)
                print(f"  Found {len(page_games)} games")
                
                all_games.extend(page_games)
                
                time.sleep(1)
                
            except Exception as e:
                print(f"  Error: {e}")
                continue
        
        # Remove duplicates and categorize
        unique_games = self._remove_duplicate_games(all_games)
        categorized_games = self._categorize_games(unique_games)
        
        # Show results
        self._show_team_results(categorized_games, team_abbr)
        
        return categorized_games
    
    def _extract_games_from_schedule_page(self, soup: BeautifulSoup, team_abbr: str, season_param: str) -> List[Dict[str, Any]]:
        """Extract games from a team schedule page."""
        
        games = []
        
        # Look for schedule table
        schedule_table = None
        
        # Try different selectors for schedule table
        selectors = [
            'table.Table',
            'table[class*="schedule"]',
            'table[class*="game"]',
            'table'
        ]
        
        for selector in selectors:
            schedule_table = soup.select_one(selector)
            if schedule_table:
                break
        
        if not schedule_table:
            print("  No schedule table found")
            return games
        
        # Find all rows in the schedule table
        rows = schedule_table.find_all('tr')
        print(f"  Found {len(rows)} rows in schedule table")
        
        for row in rows:
            # Skip header rows
            if 'thead' in str(row.get('class', [])):
                continue
            
            cells = row.find_all(['td', 'th'])
            if len(cells) < 5:  # Need at least date, opponent, result
                continue
            
            try:
                # Extract game information
                game_info = self._parse_schedule_row(cells, team_abbr)
                if game_info:
                    games.append(game_info)
                
            except Exception:
                continue
        
        return games
    
    def _parse_schedule_row(self, cells: List, team_abbr: str) -> Optional[Dict[str, Any]]:
        """Parse a schedule row to extract game information."""
        
        try:
            # Extract date
            date_str = cells[0].get_text(strip=True)
            game_date = self._parse_espn_date(date_str)
            if not game_date:
                return None
            
            # Only include 2023-24 season games
            if not self._is_2023_24_season_game(game_date):
                return None
            
            # Extract opponent
            opponent_cell = cells[1] if len(cells) > 1 else None
            opponent = opponent_cell.get_text(strip=True) if opponent_cell else "Unknown"
            
            # Extract result
            result_cell = cells[2] if len(cells) > 2 else None
            result = result_cell.get_text(strip=True) if result_cell else "Unknown"
            
            # Extract score if available
            score_cell = cells[3] if len(cells) > 3 else None
            score = score_cell.get_text(strip=True) if score_cell else "Unknown"
            
            # Determine game type
            game_type = self._determine_game_type(game_date)
            
            # Create game record
            game = {
                'game_id': f"ESPN_SCHEDULE_{team_abbr}_{game_date.strftime('%Y%m%d')}",
                'team_abbr': team_abbr,
                'game_date': game_date,
                'opponent': opponent,
                'result': result,
                'score': score,
                'game_type': game_type,
                'season': "2023-24"
            }
            
            return game
            
        except Exception:
            return None
    
    def _remove_duplicate_games(self, games: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicate games based on game_id."""
        
        seen_game_ids = set()
        unique_games = []
        
        for game in games:
            game_id = game['game_id']
            if game_id not in seen_game_ids:
                unique_games.append(game)
                seen_game_ids.add(game_id)
        
        return unique_games
    
    def _categorize_games(self, games: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Categorize games by type."""
        
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
    
    def _show_team_results(self, categorized_games: Dict[str, Any], team_abbr: str):
        """Show detailed results of the team schedule collection."""
        
        team_name = self.TEAM_ABBREVIATIONS.get(team_abbr, team_abbr.upper())
        
        print(f"\n📊 FINAL RESULTS FOR {team_name}:")
        print(f"{'='*60}")
        
        print(f"Total games found: {categorized_games['total_games']}")
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
        print("Expected regular season: 82 games")
        print(f"Actual regular season: {categorized_games['regular_season_count']} games")
        
        if categorized_games['regular_season_count'] < 80:
            print("⚠️ WARNING: Missing some regular season games")
        elif categorized_games['regular_season_count'] > 82:
            print("⚠️ WARNING: More games than expected - may include duplicates")
        else:
            print("✅ Regular season count looks complete!")
    
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
    """Test the team schedule collector."""
    
    collector = ESPNTeamScheduleCollector()
    
    # Test with Brooklyn Nets (from the URL you provided)
    test_teams = [
        "bkn",  # Brooklyn Nets
        "bos",  # Boston Celtics (for comparison)
    ]
    
    for team_abbr in test_teams:
        results = collector.get_team_complete_schedule(team_abbr)
        
        print(f"\n{'='*80}")
        print(f"SUMMARY FOR {collector.TEAM_ABBREVIATIONS.get(team_abbr, team_abbr.upper())}:")
        print(f"{'='*80}")
        print(f"Regular season: {results['regular_season_count']} games")
        print(f"Playoffs: {results['playoff_count']} games")
        print(f"Total 2023-24: {results['regular_season_count'] + results['playoff_count']} games")

if __name__ == "__main__":
    main() 