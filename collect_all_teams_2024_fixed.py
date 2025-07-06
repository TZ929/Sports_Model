#!/usr/bin/env python3
"""
Collect 2023-2024 season game stats for all teams - FIXED VERSION.
Uses correct ESPN season parameter to get full season data.
"""

import requests
import logging
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Dict, Any, Optional
from src.utils.database import db_manager
from sqlalchemy import text
import time
import re
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AllTeamsCollectorFixed:
    """Collect game stats for all teams in the database - FIXED VERSION."""
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.session_stats = {
            'total_players': 0,
            'total_stats': 0,
            'successful_teams': 0,
            'failed_teams': 0,
            'errors': []
        }
    
    def get_all_teams_with_players(self) -> List[tuple]:
        """Get all teams that have players in the database."""
        
        with db_manager.get_session() as session:
            query = text("""
                SELECT DISTINCT team_name, COUNT(*) as player_count
                FROM players 
                WHERE team_name IS NOT NULL 
                AND team_name != '' 
                AND player_id REGEXP '^[0-9]+$'
                AND full_name IS NOT NULL
                GROUP BY team_name
                HAVING player_count > 0
                ORDER BY player_count DESC
            """)
            
            result = session.execute(query)
            teams = result.fetchall()
            
            logger.info(f"Found {len(teams)} teams with players")
            for team_name, count in teams:
                logger.info(f"  - {team_name}: {count} players")
            
            return teams
    
    def get_team_players(self, team_name: str) -> List[tuple]:
        """Get all players from a specific team."""
        
        with db_manager.get_session() as session:
            query = text("""
                SELECT player_id, full_name 
                FROM players 
                WHERE team_name LIKE :team_name
                AND player_id REGEXP '^[0-9]+$'
                AND full_name IS NOT NULL
                ORDER BY full_name
            """)
            
            result = session.execute(query, {'team_name': f'%{team_name}%'})
            players = result.fetchall()
            
            return players
    
    def get_player_game_logs(self, player_id: str, player_name: str, team_name: str) -> List[Dict[str, Any]]:
        """Get game-by-game statistics for a specific player from ESPN web pages - FIXED VERSION."""
        
        # Try multiple ESPN URL formats to get 2023-24 season data
        urls_to_try = [
            # Format 1: Direct 2023-24 season URL
            f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/season/2024",
            # Format 2: Standard URL (might default to current season)
            f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/{player_name.lower().replace(' ', '-')}",
            # Format 3: Alternative format
            f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}",
        ]
        
        for url in urls_to_try:
            try:
                logger.info(f"Trying URL: {url}")
                response = requests.get(url, headers=self.headers, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Check if page exists
                if "Page Not Found" in soup.get_text() or "404" in soup.get_text():
                    logger.warning(f"Page not found for {url}")
                    continue
                
                # Find the game log table
                game_log_table = None
                for selector in ['table.Table', 'table[class*="gamelog"]', 'table']:
                    game_log_table = soup.select_one(selector)
                    if game_log_table:
                        break
                
                if not game_log_table:
                    logger.warning(f"No game log table found for {url}")
                    continue
                
                stats = []
                rows = game_log_table.find_all('tr')
                
                for row in rows:
                    # Skip header rows
                    if 'thead' in str(row.get('class', [])):
                        continue
                    
                    cells = row.find_all(['td', 'th'])
                    if len(cells) < 10:  # Need at least basic stats
                        continue
                    
                    try:
                        # Extract game information
                        game_date = self._parse_espn_date(cells[0].get_text(strip=True))
                        if not game_date:
                            continue
                        
                        # Only collect 2023-2024 season data (Oct 2023 - Apr 2024)
                        if game_date.year == 2023 and game_date.month < 10:
                            continue  # Skip pre-season 2023
                        if game_date.year == 2024 and game_date.month > 4:
                            continue  # Skip post-season 2024
                        if game_date.year not in [2023, 2024]:
                            continue  # Skip other years
                        
                        opponent = cells[1].get_text(strip=True)
                        result = cells[2].get_text(strip=True)
                        minutes = self._parse_minutes(cells[3].get_text(strip=True))
                        
                        # Parse shooting stats (FG, 3PT, FT)
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
                        
                        # Plus/minus (if available)
                        plus_minus = 0
                        if len(cells) > 17:
                            pm_text = cells[17].get_text(strip=True)
                            if pm_text and pm_text != '-':
                                try:
                                    plus_minus = int(pm_text)
                                except ValueError:
                                    plus_minus = 0
                        
                        # Create game stat record
                        game_stat = {
                            'game_id': f"ESPN_{player_id}_{game_date.strftime('%Y%m%d')}",
                            'player_id': player_id,
                            'team_id': team_name[:3].upper(),
                            'minutes_played': int(minutes) if minutes else 0,
                            'field_goals_made': fg_made,
                            'field_goals_attempted': fg_attempted,
                            'three_pointers_made': three_made,
                            'three_pointers_attempted': three_attempted,
                            'free_throws_made': ft_made,
                            'free_throws_attempted': ft_attempted,
                            'rebounds': rebounds,
                            'offensive_rebounds': 0,
                            'defensive_rebounds': 0,
                            'assists': assists,
                            'steals': steals,
                            'blocks': blocks,
                            'turnovers': turnovers,
                            'personal_fouls': personal_fouls,
                            'points': points,
                            'plus_minus': plus_minus
                        }
                        
                        stats.append(game_stat)
                        
                    except (ValueError, IndexError) as e:
                        logger.warning(f"Error parsing game stat row for {player_id}: {e}")
                        continue
                
                if stats:
                    logger.info(f"Successfully collected {len(stats)} games from {url}")
                    return stats
                else:
                    logger.warning(f"No valid games found from {url}")
                
            except Exception as e:
                logger.error(f"Error accessing {url}: {e}")
                continue
        
        logger.warning(f"No valid data found for player {player_id} from any URL")
        return []
    
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
                
                logger.warning(f"Could not parse date: {date_str}")
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
    
    def collect_team_data(self, team_name: str, player_count: int) -> Dict[str, Any]:
        """Collect game stats for all players from a specific team."""
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing team: {team_name} ({player_count} players)")
        logger.info(f"{'='*60}")
        
        players = self.get_team_players(team_name)
        
        if not players:
            logger.warning(f"No players found for {team_name}")
            return {'team': team_name, 'players': 0, 'stats': 0, 'success': False}
        
        team_stats = 0
        successful_players = 0
        
        for i, (player_id, player_name) in enumerate(players):
            try:
                logger.info(f"Collecting game logs for player {i+1}/{len(players)}: {player_name} ({player_id})")
                
                stats = self.get_player_game_logs(player_id, player_name, team_name)
                
                if stats:
                    team_stats += len(stats)
                    successful_players += 1
                    
                    # Save to database
                    for stat in stats:
                        db_manager.insert_player_stats(stat)
                    
                    logger.info(f"  ✅ {len(stats)} game stats collected and saved")
                else:
                    logger.warning(f"  ⚠️ No game stats found for {player_name}")
                
                # Random delay between 1-3 seconds to be respectful
                time.sleep(random.uniform(1, 3))
                
            except Exception as e:
                error_msg = f"Error collecting game logs for {player_name} ({player_id}): {e}"
                logger.error(error_msg)
                self.session_stats['errors'].append(error_msg)
                continue
        
        logger.info(f"Team {team_name} completed: {successful_players} players, {team_stats} game stats")
        return {'team': team_name, 'players': successful_players, 'stats': team_stats, 'success': True}
    
    def collect_all_teams_data(self):
        """Collect game stats for all teams."""
        
        logger.info("Starting comprehensive 2023-2024 season data collection (FIXED VERSION)...")
        
        teams = self.get_all_teams_with_players()
        
        if not teams:
            logger.warning("No teams with players found in database")
            return
        
        results = []
        
        for i, (team_name, player_count) in enumerate(teams):
            try:
                logger.info(f"\nProcessing team {i+1}/{len(teams)}: {team_name}")
                
                result = self.collect_team_data(team_name, player_count)
                results.append(result)
                
                # Update session stats
                self.session_stats['total_players'] += result['players']
                self.session_stats['total_stats'] += result['stats']
                
                if result['success']:
                    self.session_stats['successful_teams'] += 1
                else:
                    self.session_stats['failed_teams'] += 1
                
                # Progress update
                logger.info(f"\nProgress: {i+1}/{len(teams)} teams completed")
                logger.info(f"Total stats collected so far: {self.session_stats['total_stats']}")
                
                # Longer break between teams (5-10 seconds)
                if i < len(teams) - 1:  # Don't sleep after the last team
                    sleep_time = random.uniform(5, 10)
                    logger.info(f"Taking a {sleep_time:.1f} second break...")
                    time.sleep(sleep_time)
                
            except Exception as e:
                error_msg = f"Error processing team {team_name}: {e}"
                logger.error(error_msg)
                self.session_stats['errors'].append(error_msg)
                self.session_stats['failed_teams'] += 1
                continue
        
        # Final summary
        self._print_final_summary(results)
    
    def _print_final_summary(self, results: List[Dict[str, Any]]):
        """Print comprehensive collection summary."""
        
        print(f"\n{'='*80}")
        print("2023-2024 SEASON DATA COLLECTION COMPLETE (FIXED VERSION)")
        print(f"{'='*80}")
        
        print("\n📊 OVERALL STATISTICS:")
        print(f"  Teams processed: {len(results)}")
        print(f"  Successful teams: {self.session_stats['successful_teams']}")
        print(f"  Failed teams: {self.session_stats['failed_teams']}")
        print(f"  Total players: {self.session_stats['total_players']}")
        print(f"  Total game stats: {self.session_stats['total_stats']}")
        
        print("\n🏆 TOP PERFORMING TEAMS:")
        # Sort by stats collected
        sorted_results = sorted(results, key=lambda x: x['stats'], reverse=True)
        for i, result in enumerate(sorted_results[:10]):
            print(f"  {i+1}. {result['team']}: {result['stats']} stats ({result['players']} players)")
        
        print("\n📈 TEAM BREAKDOWN:")
        for result in sorted_results:
            print(f"  {result['team']}: {result['stats']} stats, {result['players']} players")
        
        if self.session_stats['errors']:
            print(f"\n⚠️ ERRORS ENCOUNTERED ({len(self.session_stats['errors'])}):")
            for error in self.session_stats['errors'][:5]:  # Show first 5 errors
                print(f"  - {error}")
            if len(self.session_stats['errors']) > 5:
                print(f"  ... and {len(self.session_stats['errors']) - 5} more errors")

def main():
    """Collect 2023-2024 season data for all teams - FIXED VERSION."""
    
    logger.info("Starting comprehensive 2023-2024 season data collection (FIXED VERSION)...")
    collector = AllTeamsCollectorFixed()
    collector.collect_all_teams_data()

if __name__ == "__main__":
    main() 