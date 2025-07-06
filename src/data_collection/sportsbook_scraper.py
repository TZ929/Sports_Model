"""
Web scraper for collecting prop odds from sportsbooks.
"""

import logging
import time
import re
from datetime import datetime
from typing import Dict, List, Any, Optional

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from ..utils.config import config
from ..utils.database import db_manager

# Set up logging
logger = logging.getLogger(__name__)


class SportsbookScraper:
    """Base class for sportsbook scrapers."""
    
    def __init__(self, headless: bool = True):
        """Initialize the scraper.
        
        Args:
            headless: Whether to run browser in headless mode
        """
        self.driver = None
        self.headless = headless
        self.wait_time = config.get('scraping.timeout', 30)
        self.delay = config.get('scraping.delay_between_requests', 2.0)
        
    def _setup_driver(self):
        """Set up the web driver."""
        chrome_options = Options()
        
        if self.headless:
            chrome_options.add_argument("--headless")
        
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.implicitly_wait(10)
        
    def _wait_for_element(self, by: By, value: str, timeout: int = None) -> Optional[Any]:
        """Wait for an element to be present on the page.
        
        Args:
            by: Selenium By strategy
            value: Element identifier
            timeout: Timeout in seconds
            
        Returns:
            WebElement if found, None otherwise
        """
        timeout = timeout or self.wait_time
        try:
            element = WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((by, value))
            )
            return element
        except TimeoutException:
            logger.warning(f"Element not found: {value}")
            return None
    
    def _safe_find_element(self, by: By, value: str) -> Optional[Any]:
        """Safely find an element without waiting.
        
        Args:
            by: Selenium By strategy
            value: Element identifier
            
        Returns:
            WebElement if found, None otherwise
        """
        try:
            return self.driver.find_element(by, value)
        except NoSuchElementException:
            return None
    
    def _safe_find_elements(self, by: By, value: str) -> List[Any]:
        """Safely find multiple elements.
        
        Args:
            by: Selenium By strategy
            value: Element identifier
            
        Returns:
            List of WebElements
        """
        try:
            return self.driver.find_elements(by, value)
        except NoSuchElementException:
            return []
    
    def _parse_odds(self, odds_text: str) -> Optional[int]:
        """Parse odds text to integer value.
        
        Args:
            odds_text: Odds text (e.g., "-110", "+150")
            
        Returns:
            Odds as integer or None if invalid
        """
        if not odds_text:
            return None
        
        try:
            # Remove any non-numeric characters except + and -
            cleaned = re.sub(r'[^\d+-]', '', odds_text)
            return int(cleaned)
        except ValueError:
            logger.warning(f"Could not parse odds: {odds_text}")
            return None
    
    def _odds_to_probability(self, odds: int) -> Optional[float]:
        """Convert American odds to implied probability.
        
        Args:
            odds: American odds (e.g., -110, +150)
            
        Returns:
            Implied probability as float or None if invalid
        """
        if odds is None:
            return None
        
        try:
            if odds > 0:
                return 100 / (odds + 100)
            else:
                return abs(odds) / (abs(odds) + 100)
        except ZeroDivisionError:
            return None
    
    def close(self):
        """Close the web driver."""
        if self.driver:
            self.driver.quit()


class FanDuelScraper(SportsbookScraper):
    """Scraper for FanDuel sportsbook."""
    
    def __init__(self, headless: bool = True):
        """Initialize FanDuel scraper."""
        super().__init__(headless)
        self.base_url = config.get('data_sources.sportsbooks.fanduel.base_url')
        self.props_url = config.get('data_sources.sportsbooks.fanduel.props_url')
        
    def scrape_props(self, game_date: str = None) -> List[Dict[str, Any]]:
        """Scrape prop bets from FanDuel.
        
        Args:
            game_date: Date to scrape (YYYY-MM-DD format)
            
        Returns:
            List of prop odds dictionaries
        """
        if not self.driver:
            self._setup_driver()
        
        try:
            logger.info("Starting FanDuel props scraping")
            
            # Navigate to NBA props page
            self.driver.get(self.props_url)
            time.sleep(self.delay)
            
            # Wait for page to load
            self._wait_for_element(By.CSS_SELECTOR, "[data-testid='game-card']", 30)
            
            # Find all games
            game_cards = self._safe_find_elements(By.CSS_SELECTOR, "[data-testid='game-card']")
            
            all_props = []
            
            for game_card in game_cards:
                try:
                    game_props = self._scrape_game_props(game_card)
                    all_props.extend(game_props)
                except Exception as e:
                    logger.error(f"Error scraping game props: {e}")
                    continue
            
            logger.info(f"Scraped {len(all_props)} props from FanDuel")
            return all_props
            
        except Exception as e:
            logger.error(f"Error in FanDuel scraping: {e}")
            return []
        finally:
            self.close()
    
    def _scrape_game_props(self, game_card) -> List[Dict[str, Any]]:
        """Scrape props for a specific game.
        
        Args:
            game_card: WebElement for the game card
            
        Returns:
            List of prop odds for the game
        """
        props = []
        
        try:
            # Get game information
            game_info = self._extract_game_info(game_card)
            
            # Click on player props section
            props_button = game_card.find_element(By.CSS_SELECTOR, "[data-testid='player-props-button']")
            props_button.click()
            time.sleep(2)
            
            # Find all player prop markets
            prop_markets = self._safe_find_elements(By.CSS_SELECTOR, "[data-testid='player-prop-market']")
            
            for market in prop_markets:
                market_props = self._extract_market_props(market, game_info)
                props.extend(market_props)
            
        except Exception as e:
            logger.error(f"Error extracting game props: {e}")
        
        return props
    
    def _extract_game_info(self, game_card) -> Dict[str, str]:
        """Extract game information from game card.
        
        Args:
            game_card: WebElement for the game card
            
        Returns:
            Dictionary with game information
        """
        try:
            teams = game_card.find_elements(By.CSS_SELECTOR, "[data-testid='team-name']")
            home_team = teams[0].text if len(teams) > 0 else "Unknown"
            away_team = teams[1].text if len(teams) > 1 else "Unknown"
            
            return {
                'home_team': home_team,
                'away_team': away_team,
                'game_id': f"FD_{home_team}_{away_team}_{datetime.now().strftime('%Y%m%d')}"
            }
        except Exception as e:
            logger.error(f"Error extracting game info: {e}")
            return {'home_team': 'Unknown', 'away_team': 'Unknown', 'game_id': 'Unknown'}
    
    def _extract_market_props(self, market, game_info: Dict[str, str]) -> List[Dict[str, Any]]:
        """Extract props from a market section.
        
        Args:
            market: WebElement for the market
            game_info: Game information dictionary
            
        Returns:
            List of prop odds
        """
        props = []
        
        try:
            # Get market type (points, rebounds, assists, etc.)
            market_title = market.find_element(By.CSS_SELECTOR, "[data-testid='market-title']").text
            
            # Find all player props in this market
            player_props = market.find_elements(By.CSS_SELECTOR, "[data-testid='player-prop']")
            
            for player_prop in player_props:
                try:
                    prop_data = self._extract_player_prop(player_prop, market_title, game_info)
                    if prop_data:
                        props.append(prop_data)
                except Exception as e:
                    logger.error(f"Error extracting player prop: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error extracting market props: {e}")
        
        return props
    
    def _extract_player_prop(self, player_prop, market_type: str, game_info: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """Extract individual player prop data.
        
        Args:
            player_prop: WebElement for the player prop
            market_type: Type of market (points, rebounds, etc.)
            game_info: Game information dictionary
            
        Returns:
            Prop odds dictionary or None if extraction failed
        """
        try:
            # Extract player name
            player_name = player_prop.find_element(By.CSS_SELECTOR, "[data-testid='player-name']").text
            
            # Extract line
            line_element = player_prop.find_element(By.CSS_SELECTOR, "[data-testid='prop-line']")
            line_text = line_element.text
            line = float(re.sub(r'[^\d.]', '', line_text))
            
            # Extract odds
            over_odds_element = player_prop.find_element(By.CSS_SELECTOR, "[data-testid='over-odds']")
            under_odds_element = player_prop.find_element(By.CSS_SELECTOR, "[data-testid='under-odds']")
            
            over_odds = self._parse_odds(over_odds_element.text)
            under_odds = self._parse_odds(under_odds_element.text)
            
            # Calculate implied probabilities
            over_prob = self._odds_to_probability(over_odds)
            under_prob = self._odds_to_probability(under_odds)
            
            return {
                'game_id': game_info['game_id'],
                'player_name': player_name,
                'sportsbook': 'fanduel',
                'prop_type': market_type.lower(),
                'line': line,
                'over_odds': over_odds,
                'under_odds': under_odds,
                'over_implied_prob': over_prob,
                'under_implied_prob': under_prob,
                'timestamp': datetime.now()
            }
            
        except Exception as e:
            logger.error(f"Error extracting player prop data: {e}")
            return None


class ESPNBETScraper(SportsbookScraper):
    """Scraper for ESPNBET sportsbook."""
    
    def __init__(self, headless: bool = True):
        """Initialize ESPNBET scraper."""
        super().__init__(headless)
        self.base_url = config.get('data_sources.sportsbooks.espnbet.base_url')
        self.props_url = config.get('data_sources.sportsbooks.espnbet.props_url')
    
    def scrape_props(self, game_date: str = None) -> List[Dict[str, Any]]:
        """Scrape prop bets from ESPNBET.
        
        Args:
            game_date: Date to scrape (YYYY-MM-DD format)
            
        Returns:
            List of prop odds dictionaries
        """
        # Implementation similar to FanDuel but adapted for ESPNBET's structure
        # This is a placeholder - actual implementation would need to be developed
        # based on ESPNBET's specific HTML structure and JavaScript behavior
        
        logger.info("ESPNBET scraping not yet implemented")
        return []


def scrape_all_sportsbooks(game_date: str = None) -> Dict[str, List[Dict[str, Any]]]:
    """Scrape props from all configured sportsbooks.
    
    Args:
        game_date: Date to scrape (YYYY-MM-DD format)
        
    Returns:
        Dictionary with sportsbook names as keys and prop lists as values
    """
    results = {}
    
    # Scrape FanDuel
    try:
        fd_scraper = FanDuelScraper(headless=True)
        fd_props = fd_scraper.scrape_props(game_date)
        results['fanduel'] = fd_props
        logger.info(f"Scraped {len(fd_props)} props from FanDuel")
    except Exception as e:
        logger.error(f"Error scraping FanDuel: {e}")
        results['fanduel'] = []
    
    # Scrape ESPNBET
    try:
        espn_scraper = ESPNBETScraper(headless=True)
        espn_props = espn_scraper.scrape_props(game_date)
        results['espnbet'] = espn_props
        logger.info(f"Scraped {len(espn_props)} props from ESPNBET")
    except Exception as e:
        logger.error(f"Error scraping ESPNBET: {e}")
        results['espnbet'] = []
    
    return results


# Example usage
if __name__ == "__main__":
    # Test scraping
    results = scrape_all_sportsbooks()
    
    for sportsbook, props in results.items():
        print(f"{sportsbook}: {len(props)} props")
        
        # Save to database
        for prop in props:
            db_manager.insert_prop_odds(prop) 