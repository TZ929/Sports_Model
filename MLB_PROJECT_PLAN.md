# MLB Player Prop Prediction Project Plan

This document outlines the phased development plan for creating, training, and deploying a machine learning model to predict MLB player prop bets on FanDuel and ESPN Bet. This project will leverage the existing infrastructure and code from the NBA model wherever possible.

## Guiding Principles & Strategy

- **Leverage Existing Code:** We will adapt existing scripts for data collection, feature engineering, modeling, and prediction to accelerate development. The core `SQLAlchemy` database, `Click` CLI, and pipeline orchestration logic will be reused.
- **Modularity:** MLB-specific code will be kept in separate directories (e.g., `src/data_collection/mlb`, `src/feature_engineering/mlb`) to maintain a clean and organized codebase.
- **Data-Driven Decisions:** All modeling and strategy decisions will be backed by rigorous backtesting and analysis.

## Key Performance Metrics

The success of the MLB model will be measured by:

1.  **Return on Investment (ROI):** The primary metric. A consistently positive ROI over a large sample of backtested bets indicates a profitable model.
2.  **Prediction Accuracy:** The percentage of correctly predicted OVER/UNDER outcomes. While important, this is secondary to ROI, as a model can be profitable without having >50% accuracy if it correctly identifies high-value underdogs.
3.  **Model Calibration:** How well the model's predicted probabilities align with actual outcomes. A well-calibrated model is more reliable.
4.  **Coverage:** The number of different players and prop types the model can confidently make predictions for.

---

## Phase 1: Foundational Setup & Data Source Investigation

**Goal:** Adapt the project structure for MLB and identify reliable data sources.

-   **[ ] Task 1.1: Create MLB Project Structure:**
    -   Create new directories:
        -   `src/data_collection/mlb/`
        -   `src/feature_engineering/mlb/`
        -   `data/models/mlb/`
    -   Create a dedicated `MLB_README.md`.

-   **[ ] Task 1.2: Research Data Sources:**
    -   Investigate and document APIs or web scraping targets for:
        -   **Player Stats:** Comprehensive historical pitching, batting, and fielding stats (e.g., Baseball-Reference, MLB.com API, Retrosheet).
        -   **Game Data:** Schedules, results, starting lineups, and weather.
        -   **Advanced Data (Statcast):** Research the feasibility of integrating advanced metrics like pitch velocity, exit velocity, and launch angle.
        -   **Odds:** Live and historical player prop odds from FanDuel and ESPN Bet.

-   **[ ] Task 1.3: Extend Database Schema:**
    -   Design and document new SQLAlchemy models for MLB data. This will likely include tables for:
        -   `mlb_games`
        -   `mlb_players`
        -   `mlb_teams`
        -   `mlb_pitcher_stats_game_logs`
        -   `mlb_batter_stats_game_logs`

-   **[ ] Task 1.4: Update Configuration:**
    -   Add an `mlb` section to the `config/config.yaml` file for MLB-specific URLs, API keys, and settings.

---

## Phase 2: Data Collection

**Goal:** Implement scripts to collect and store all necessary MLB data.

-   **[ ] Task 2.1:** Implement a script to collect full-season schedules and results.
-   **[ ] Task 2.2:** Implement scripts to collect historical game logs for both pitchers and batters.
-   **[ ] Task 2.3:** Implement a script to populate the `mlb_players` and `mlb_teams` tables.
-   **[ ] Task 2.4:** Execute all collection scripts to seed the database with several seasons of historical data.

---

## Phase 3: Feature Engineering

**Goal:** Develop features specific to predicting MLB performance.

-   **[ ] Task 3.1: Pitcher Features:**
    -   Rolling averages for key stats (ERA, WHIP, K/9, BB/9, IP).
    -   Recent performance (last 1, 3, 5 starts).
    -   Pitcher vs. opponent team history.
    -   Home/Away and Day/Night splits.

-   **[ ] Task 3.2: Batter Features:**
    -   Rolling averages (AVG, OBP, SLG, OPS, Hits, HRs).
    -   Recent performance (last 3, 7, 15 games).
    -   Batter vs. starting pitcher matchup history (BvP).
    -   Platoon splits (vs. LHP/RHP).

-   **[ ] Task 3.3: Game Context Features:**
    -   Ballpark factors (e.g., Park Factor from ESPN or Baseball Savant).
    -   Weather data (temperature, wind, humidity).
    -   Team rest days.

---

## Phase 4: Model Development & Training

**Goal:** Train and evaluate models to predict a variety of MLB player props.

-   **[ ] Task 4.1:** Adapt the `prepare_model_data.py` script for MLB features.
-   **[ ] Task 4.2:** Train a baseline model for a simple prop (e.g., Pitcher Strikeouts).
-   **[ ] Task 4.3:** Train an advanced model (XGBoost/LightGBM) on a wider range of props.
-   **[ ] Task 4.4:** Adapt the hyperparameter tuning and model retraining scripts for MLB.
-   **[ ] Task 4.5:** Version and save trained MLB models to `data/models/mlb/`.

---

## Phase 5: Backtesting & Strategy Evaluation

**Goal:** Rigorously evaluate the model's profitability on historical data.

-   **[ ] Task 5.1:** Collect or simulate historical player prop odds for backtesting.
-   **[ ] Task 5.2:** Adapt the `backtest_strategy.py` script for MLB props.
-   **[ ] Task 5.3:** Run the backtester and analyze the results, focusing on ROI.
-   **[ ] Task 5.4:** Refine the betting strategy (e.g., edge threshold, bankroll allocation) based on backtesting results.

---

## Phase 6: Deployment & Prediction Workflow

**Goal:** Automate the daily prediction process for upcoming MLB games.

-   **[ ] Task 6.1:** Adapt the `scrape_daily_odds.py` script to collect MLB props.
-   **[ ] Task 6.2:** Create a new `mlb_run_prediction.py` script to orchestrate the prediction pipeline for a given set of daily odds.
-   **[ ] Task 6.3:** Create a new `mlb_run_pipeline.py` to be the main entry point for the daily MLB workflow.
-   **[ ] Task 6.4:** Update the main `README.md` with instructions for scheduling and running the MLB prediction pipeline. 