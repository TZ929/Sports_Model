"""
Main script for the NBA/WNBA predictive model project.
"""

import logging
import click


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@click.group()
def cli():
    """Sports Model CLI for NBA/WNBA predictive modeling."""
    pass

@cli.command()
@click.option('--source', default='basketball-reference', 
              type=click.Choice(['basketball-reference', 'espn']),
              help='Data source to use')
def collect(source):
    """Collect NBA data from various sources."""
    logger.info("Starting data collection...")
    
    if source == 'basketball-reference':
        # Use Basketball Reference
        from src.data_collection.basketball_reference import BasketballReferenceCollector
        collector = BasketballReferenceCollector()
        basic_counts = collector.collect_season_data("2024", save_to_db=True)
    elif source == 'espn':
        # Use ESPN API
        from src.data_collection.espn_api import ESPNAPICollector
        collector = ESPNAPICollector()
        basic_counts = collector.collect_season_data("2024", save_to_db=True)
    
    logger.info(f"Basic data collection completed: {basic_counts}")
    
    # Collect player game statistics
    logger.info("Starting player game statistics collection...")
    from src.data_collection.player_stats_collector import PlayerStatsCollector
    stats_collector = PlayerStatsCollector()
    
    # Get player IDs from database instead of hardcoded ones
    player_stats_result = stats_collector.collect_player_stats_batch(
        player_ids=None,  # This will get IDs from database
        season="2024",
        save_to_db=True,
        limit=5  # Start with 5 players for testing
    )
    
    logger.info(f"Player stats collection completed: {player_stats_result}")
    
    # Update final counts
    final_counts = {
        **basic_counts,
        'player_stats_collected': player_stats_result['total_games'],
        'players_with_stats': player_stats_result['successful_players']
    }
    
    logger.info("Data collection completed successfully!")
    logger.info(f"Final counts: {final_counts}")

@cli.command()
def scrape():
    """Scrape live betting odds."""
    logger.info("Starting odds scraping...")
    
    try:
        from src.data_collection.sportsbook_scraper import SportsbookScraper
        
        scraper = SportsbookScraper()
        odds = scraper.scrape_all_sportsbooks()
        
        logger.info(f"Odds scraping completed: {len(odds)} odds collected")
        return True
        
    except Exception as e:
        logger.error(f"Error in odds scraping: {e}")
        return False

@cli.command()
def train():
    """Train predictive models."""
    logger.info("Starting model training...")
    
    try:
        # Import and run model training
        # This will be implemented in Phase 3
        logger.info("Model training not yet implemented (Phase 3)")
        return True
        
    except Exception as e:
        logger.error(f"Error in model training: {e}")
        return False

@cli.command()
def predict():
    """Make predictions and identify value bets."""
    logger.info("Starting predictions...")
    
    try:
        # Import and run predictions
        # This will be implemented in Phase 3
        logger.info("Predictions not yet implemented (Phase 3)")
        return True
        
    except Exception as e:
        logger.error(f"Error in predictions: {e}")
        return False

if __name__ == "__main__":
    cli() 