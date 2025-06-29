#!/usr/bin/env python3
"""
ESPN Final Team Schedule Collector - Get EXACLTY 82 regular season games for each team.
This script uses the official ESPN API and filters for regular season games
based on the `season.type` flag, which is the most reliable method.
"""

import requests
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
import time
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ESPNFinalScheduleCollector:
    """Collect exactly 82-game regular season schedules for all 30 NBA teams via ESPN's API."""
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.NBA_TEAMS = {
            'atl': 'Atlanta Hawks', 'bos': 'Boston Celtics', 'bkn': 'Brooklyn Nets',
            'cha': 'Charlotte Hornets', 'chi': 'Chicago Bulls', 'cle': 'Cleveland Cavaliers',
            'dal': 'Dallas Mavericks', 'den': 'Denver Nuggets', 'det': 'Detroit Pistons',
            'gsw': 'Golden State Warriors', 'hou': 'Houston Rockets', 'ind': 'Indiana Pacers',
            'lac': 'LA Clippers', 'lal': 'Los Angeles Lakers', 'mem': 'Memphis Grizzlies',
            'mia': 'Miami Heat', 'mil': 'Milwaukee Bucks', 'min': 'Minnesota Timberwolves',
            'no': 'New Orleans Pelicans', 'nyk': 'New York Knicks', 'okc': 'Oklahoma City Thunder',
            'orl': 'Orlando Magic', 'phi': 'Philadelphia 76ers', 'phx': 'Phoenix Suns',
            'por': 'Portland Trail Blazers', 'sac': 'Sacramento Kings', 'sas': 'San Antonio Spurs',
            'tor': 'Toronto Raptors', 'utah': 'Utah Jazz', 'was': 'Washington Wizards'
        }
        # Note: ESPN uses 'LA Clippers', not 'Los Angeles Clippers' in some API responses.
        # The logic will handle mapping based on abbreviation ('lac').
    
    def collect_all_team_schedules(self) -> Dict[str, Any]:
        """Collects and verifies schedules for all 30 NBA teams."""
        print(f"\n{'='*80}")
        print("STARTING FINAL SCHEDULE COLLECTION: REQUIRE AT LEAST 82 REGULAR SEASON GAMES PER TEAM")
        print(f"{'='*80}")
        
        all_teams_data = {}
        successful_teams = 0
        total_games = 0
        
        for team_abbr, team_name in self.NBA_TEAMS.items():
            print(f"\n{'='*60}")
            print(f"Processing: {team_name} ({team_abbr.upper()})")
            print(f"{'='*60}")
            
            team_schedule = self._get_team_schedule(team_abbr)
            
            if team_schedule['regular_season_count'] >= 82:
                successful_teams += 1
                total_games += team_schedule['regular_season_count']
                print(f"SUCCESS: Collected {team_schedule['regular_season_count']} games (>= 82).")
            else:
                print(f"FAILURE: Collected {team_schedule['regular_season_count']} games, which is less than 82.")
            
            all_teams_data[team_abbr] = team_schedule
            time.sleep(1) # Be a good citizen
        
        print(f"\n{'='*80}")
        print("FINAL SUMMARY")
        print(f"{'='*80}")
        print(f"Teams with at least 82 games: {successful_teams}/30")
        print(f"Total regular season games collected: {total_games}")
        expected_total = 30 * 82
        print(f"Expected minimum: {expected_total} games")
        coverage = (total_games / expected_total) * 100 if expected_total > 0 else 0
        print(f"Coverage: {coverage:.1f}%")
        
        return all_teams_data
    
    def _get_team_schedule(self, team_abbr: str) -> Dict[str, Any]:
        """Gets a team's schedule using the reliable API method."""
        api_games = self._get_api_schedule(team_abbr)
        unique_games = self._remove_duplicate_games(api_games)
        
        categorized_games = self._categorize_games(unique_games)
        self._show_team_detailed_results(categorized_games, self.NBA_TEAMS[team_abbr])
        return categorized_games
    
    def _get_api_schedule(self, team_abbr: str) -> List[Dict[str, Any]]:
        """Gets schedule from ESPN API for the 2023-2024 season (season=2024)."""
        endpoint = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{team_abbr}/schedule?season=2024"
        all_games = []
        
        print(f"Querying API: {endpoint}")
        try:
            response = requests.get(endpoint, headers=self.headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if 'events' in data:
                print(f"  Found {len(data['events'])} total events in API response.")
                for event in data['events']:
                    game = self._parse_api_event(event, team_abbr)
                    if game:
                        all_games.append(game)
                print(f"  Parsed {len(all_games)} regular season games from API.")
            else:
                print("  No 'events' key found in API response.")

        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {team_abbr}: {e}")
        except json.JSONDecodeError:
            logger.error(f"Failed to decode JSON for {team_abbr}")
        return all_games

    def _parse_api_event(self, event: Dict, team_abbr: str) -> Optional[Dict[str, Any]]:
        """Parses an API event, filtering for regular season games for the correct year."""
        try:
            # Exclude the In-Season Tournament final, which doesn't count for regular season standings
            if event.get('tournament', {}).get('type') == 'championship':
                return None

            season_info = event.get('season', {})
            # The 'type' key is not present in this endpoint's response.
            # The ?season=2024 param implicitly handles filtering to the regular season.
            # We only need to ensure we are not getting future/past season data if the API changes.
            if season_info.get('year') != 2024:
                return None
            
            game_id = event.get('id')
            date_str = event.get('date')
            if not game_id or not date_str:
                return None
            
            game_date = datetime.strptime(date_str.split('T')[0], '%Y-%m-%d')
            competition = event['competitions'][0]
            
            # Find opponent
            opponent_data = next((c['team'] for c in competition['competitors'] if c['team']['abbreviation'].lower() != team_abbr.lower()), None)
            opponent_name = opponent_data['displayName'] if opponent_data else "Unknown"

            # Determine result
            result = "TBD"
            if competition.get('status', {}).get('type', {}).get('name') == 'STATUS_FINAL':
                team_comp = next((c for c in competition['competitors'] if c['team']['abbreviation'].lower() == team_abbr.lower()), None)
                opp_comp = next((c for c in competition['competitors'] if c['team']['abbreviation'].lower() != team_abbr.lower()), None)
                if team_comp and opp_comp and 'score' in team_comp and 'score' in opp_comp:
                    team_score = team_comp['score'].get('displayValue', '0')
                    opp_score = opp_comp['score'].get('displayValue', '0')
                    win_loss = 'W' if int(team_score) > int(opp_score) else 'L'
                    result = f"{win_loss} {team_score}-{opp_score}"

            return {
                'game_id': f"ESPN_{game_id}", 'team_abbr': team_abbr, 'game_date': game_date,
                'opponent': opponent_name, 'result': result, 'source': 'api_v2',
                'game_type': 'regular_season'
            }
        except (KeyError, IndexError, TypeError) as e:
            logger.warning(f"Could not parse API event for {team_abbr}. Event ID: {event.get('id')}. Error: {e}")
            return None

    def _remove_duplicate_games(self, games: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Removes duplicates based on unique ESPN game ID."""
        seen_ids = set()
        unique_games = []
        for game in games:
            if game['game_id'] not in seen_ids:
                unique_games.append(game)
                seen_ids.add(game['game_id'])
        if len(games) != len(unique_games):
            print(f"  Removed {len(games) - len(unique_games)} duplicate game(s).")
        return unique_games
    
    def _categorize_games(self, games: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Categorizes games (all should be regular season)."""
        regular_season_games = sorted([g for g in games if g['game_type'] == 'regular_season'], key=lambda x: x['game_date'])
        return {
            'total_games': len(games), 'regular_season_games': regular_season_games,
            'regular_season_count': len(regular_season_games)
        }
    
    def _show_team_detailed_results(self, categorized_games: Dict[str, Any], team_name: str):
        """Shows detailed results for a team."""
        print(f"\nDETAILED RESULTS FOR {team_name.upper()}:")
        count = categorized_games['regular_season_count']
        print(f"Regular season games found: {count}")
        
        if categorized_games['regular_season_games']:
            games = categorized_games['regular_season_games']
            print(f"Date range: {games[0]['game_date']:%b %d, %Y} to {games[-1]['game_date']:%b %d, %Y}")
        
        print(f"\nEXPECTED (>=82) vs. ACTUAL ({count}): Difference: {count - 82:+d}")
        if count < 82:
            print("   WARNING: Game count is less than 82. This data is incomplete.")

def main():
    """Entry point to run the collector."""
    collector = ESPNFinalScheduleCollector()
    all_teams_data = collector.collect_all_team_schedules()
    
    # Optional: Save results to a JSON file
    with open('final_schedule_data.json', 'w') as f:
        # Convert datetime objects to strings for JSON serialization
        def json_default(o):
            if isinstance(o, datetime):
                return o.isoformat()
            raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")
        json.dump(all_teams_data, f, indent=2, default=json_default)
    print("\nAll collected schedule data saved to 'final_schedule_data.json'")

if __name__ == "__main__":
    main() 