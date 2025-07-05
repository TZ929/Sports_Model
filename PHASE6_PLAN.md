# Phase 6: Deployment & Prediction Workflow

This document outlines the plan to fully automate the prediction workflow, from data collection to identifying value-based betting opportunities.

## Current Status

A foundational `run_prediction.py` script exists. It can take a `game_id` and `player_id` as inputs, load the latest model, and generate a prediction with probabilities.

## Remaining Tasks

### Task 1: Automated Game and Prop Line Scraping
- **Objective:** Scrape daily games and player prop lines from FanDuel and ESPN BET.
- **Implementation:**
    - [ ] Create a new script, `src/data_collection/scrape_daily_odds.py`.
    - [ ] This script will scrape the sportsbooks for all available NBA games for the current day.
    - [ ] For each game, it will scrape all available player prop bets (e.g., points, rebounds, assists).
    - [ ] The scraped data (game info, player, prop type, line, and odds) will be stored in a structured format, likely a new table in our database or a daily JSON file.

### Task 2: Orchestrate the Full Prediction Pipeline
- **Objective:** Modify the main execution script to use the scraped daily data.
- **Implementation:**
    - [ ] Update `run_prediction.py` to no longer require `game_id` and `player_id` as CLI arguments.
    - [ ] Instead, it will read the daily games and props collected by `scrape_daily_odds.py`.
    - [ ] It will loop through each unique game-player-prop combination.
    - [ ] For each combination, it will execute the existing `fetch_prediction_data`, `engineer_features`, and `make_prediction` functions.

### Task 3: Value Calculation and Output
- **Objective:** Compare the model's predictions to live odds and generate a clear output of value opportunities.
- **Implementation:**
    - [ ] Create a new function `calculate_implied_probability(odds)` in a utils module. This will convert American odds (e.g., -110) into a probability.
    - [ ] Create another function `find_value_opportunity(model_prob, implied_prob)`. This will contain the logic to decide if a bet has positive expected value.
    - [ ] In `run_prediction.py`, after a prediction is made, call these new functions.
    - [ ] If a value opportunity is found, log it to a dedicated `predictions.log` file in the specified format: `Player | Prop | Line | Sportsbook | Model Probability | Implied Probability | Bet`.

### Task 4: Automation and Scheduling
- **Objective:** Schedule the entire workflow to run automatically.
- **Implementation:**
    - [ ] We will use a scheduler like `cron` (on Linux/macOS) or Windows Task Scheduler to run the pipeline at a set time each morning.
    - [ ] The final `run_prediction.py` script will be the entry point for this scheduled task. 