"""
Main script for the NBA/WNBA predictive model project.
"""

import logging
import argparse
from pathlib import Path

from src.utils.config import config
from src.utils.database import db_manager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/sports_model.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def collect_data():
    """Collect historical and live data."""
    logger.info("Starting data collection...")
    
    try:
        # Try Basketball Reference first (more reliable)
        from src.data_collection.basketball_reference import BasketballReferenceCollector
        
        collector = BasketballReferenceCollector()
        
        # Collect recent season data
        counts = collector.collect_season_data("2024", save_to_db=True)
        
        logger.info(f"Data collection completed: {counts}")
        return True
        
    except ImportError:
        logger.warning("Basketball Reference collector not available, trying NBA API...")
        try:
            from src.data_collection.nba_api import NBADataCollector
            
            collector = NBADataCollector()
            
            # Collect recent season data
            counts = collector.collect_season_data("2023-24", save_to_db=True)
            
            logger.info(f"Data collection completed: {counts}")
            return True
            
        except Exception as e:
            logger.error(f"Error in data collection: {e}")
            return False
    except Exception as e:
        logger.error(f"Error in data collection: {e}")
        return False


def scrape_odds():
    """Scrape current prop odds."""
    logger.info("Starting odds scraping...")
    
    try:
        from src.data_collection.sportsbook_scraper import scrape_all_sportsbooks
        
        results = scrape_all_sportsbooks()
        
        total_props = sum(len(props) for props in results.values())
        logger.info(f"Odds scraping completed: {total_props} total props")
        
        return True
        
    except Exception as e:
        logger.error(f"Error in odds scraping: {e}")
        return False


def train_models():
    """Train predictive models."""
    logger.info("Starting model training...")
    
    # This will be implemented in the modeling module
    logger.info("Model training not yet implemented")
    return True


def make_predictions():
    """Make predictions for today's games."""
    logger.info("Starting predictions...")
    
    # This will be implemented in the prediction module
    logger.info("Predictions not yet implemented")
    return True


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="NBA/WNBA Predictive Model")
    parser.add_argument(
        "action",
        choices=["collect", "scrape", "train", "predict", "all"],
        help="Action to perform"
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run scrapers in headless mode"
    )
    
    args = parser.parse_args()
    
    logger.info(f"Starting NBA/WNBA Predictive Model - Action: {args.action}")
    
    success = True
    
    if args.action == "collect":
        success = collect_data()
    elif args.action == "scrape":
        success = scrape_odds()
    elif args.action == "train":
        success = train_models()
    elif args.action == "predict":
        success = make_predictions()
    elif args.action == "all":
        # Run all steps
        success = collect_data() and scrape_odds() and train_models() and make_predictions()
    
    if success:
        logger.info("Operation completed successfully!")
    else:
        logger.error("Operation failed!")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main()) 