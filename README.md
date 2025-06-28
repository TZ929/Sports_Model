# NBA/WNBA Predictive Model for Player, Team, & Game Props

A machine learning system to identify valuable betting opportunities for NBA and WNBA props on FanDuel and ESPNBET.

## Project Overview

This project develops a robust, automated system that:
- Acquires historical game/player data and real-time betting odds
- Trains predictive models for various prop bets
- Compares model predictions against sportsbook odds to identify "value" bets
- Presents potential bets in a clear, actionable format

## Target Props

### Player Props
- Points (Over/Under)
- Rebounds (O/U)
- Assists (O/U)
- Three-Pointers Made (O/U)
- Combos (Points+Rebounds+Assists)

### Team Props
- Total Points (O/U)
- Team to Win by a Margin

### Game Props
- Game Total Points (O/U)
- First Basket Scorer (secondary goal)

## Success Metrics

- **Predictive Accuracy**: >55% on held-out test dataset
- **Backtested ROI**: Simulated profit/loss from model recommendations
- **Model Calibration**: Predicted probabilities align with actual outcomes

## Tech Stack

- **Language**: Python
- **Core Libraries**: pandas, NumPy, scikit-learn, XGBoost, LightGBM
- **Web Scraping**: requests, BeautifulSoup4, Selenium/Playwright
- **Database**: PostgreSQL/SQLite
- **Automation**: Cron jobs/Task Scheduler

## Project Structure

```
Sports_Model/
├── data/                   # Data storage
│   ├── raw/               # Raw scraped data
│   ├── processed/         # Cleaned and processed data
│   └── models/            # Trained model files
├── src/                   # Source code
│   ├── data_collection/   # Web scraping and data acquisition
│   ├── preprocessing/     # Data cleaning and feature engineering
│   ├── modeling/          # Model training and evaluation
│   ├── prediction/        # Live prediction pipeline
│   └── utils/             # Utility functions
├── notebooks/             # Jupyter notebooks for analysis
├── config/                # Configuration files
├── tests/                 # Unit tests
└── requirements.txt       # Python dependencies
```

## Installation

1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Set up database configuration
4. Configure API keys and credentials

## Usage

1. **Data Collection**: Run scraping scripts to collect historical and live data
2. **Model Training**: Execute the training pipeline
3. **Predictions**: Run daily prediction workflow
4. **Analysis**: Review results and model performance

## Disclaimer

This project is for educational and technical purposes only. It is not financial advice. All betting involves risk. Web scraping must be done ethically and in accordance with websites' terms of service.

## License

MIT License
