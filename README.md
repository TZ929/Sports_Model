# Sports Model: NBA/WNBA Predictive Betting System

This project is a comprehensive machine learning system designed to identify valuable betting opportunities for NBA and WNBA player props. It automates the entire workflow from data collection and feature engineering to model training and prediction.

## ✨ Features

- **Automated Data Pipeline**: Ingests player stats, game data, and betting odds from multiple sources.
- **Advanced Feature Engineering**: Creates sophisticated features like rolling averages, opponent strength, and player matchups.
- **Versioned Model Training**: A robust, automated pipeline to retrain and evaluate models, with versioning for different seasons.
- **Dockerized Environment**: The entire application is containerized with Docker for easy setup and consistent execution.
- **CI/CD Automation**: A GitHub Actions workflow automatically tests, lints, and builds the application on every push.

## 🛠️ Tech Stack

- **Language**: Python 3.11
- **Core Libraries**: pandas, scikit-learn, LightGBM, Optuna
- **Database**: SQLAlchemy with SQLite (default) or PostgreSQL
- **Automation & Orchestration**: Docker, GitHub Actions, Click

---

## 🚀 Docker-Based Quickstart

The recommended way to run this project is with Docker.

### Prerequisites
- Docker installed and running on your system.
- Git for cloning the repository.

### 1. Build the Docker Image
First, build the Docker image. This command packages all the application code and dependencies.

```bash
docker build -t sports-model .
```

### 2. Run the Training Pipeline
To run the full data processing and model training pipeline for a specific season, use the following command. This will generate the necessary data, train the model, and save the versioned outputs inside the container.

```bash
# Example for the 2023-2024 season
docker run --rm -v ./data:/app/data -v ./analysis_results:/app/analysis_results sports-model train --season "2023-2024"
```
- `-v ./data:/app/data`: This mounts your local `data` directory into the container, so the trained models are saved to your machine.
- `-v ./analysis_results:/app/analysis_results`: This mounts the `analysis_results` directory to save the evaluation plots locally.

### 3. Make Predictions
To make predictions using the latest trained model, run:

```bash
# This will use the latest model by default
docker run --rm -v ./data:/app/data sports-model predict
```
*Note: The prediction script `run_prediction.py` will need to be updated to load the versioned model.*

---

## 🤖 CI/CD Pipeline

This project uses GitHub Actions for Continuous Integration and Continuous Deployment. The workflow is defined in `.github/workflows/main.yml` and includes:
- **Linting**: Uses `ruff` to check for code style and quality.
- **Testing**: Runs the test suite with `pytest`.
- **Docker Build**: Builds the Docker image to ensure it's always valid.

This pipeline automatically runs on every push and pull request to the `main` branch, ensuring the project is always in a stable state.

---

## 📂 Project Structure

```
Sports_Model/
├── .github/workflows/    # CI/CD pipeline configuration
├── data/                 # Data storage (ignored by Git)
├── analysis_results/     # Model analysis plots (ignored by Git)
├── src/                  # Source code
├── tests/                # Unit and integration tests
├── Dockerfile            # Docker configuration
├── main.py               # Main CLI entrypoint
└── requirements.txt      # Python dependencies
```

## Disclaimer

This project is for educational and technical purposes only. It is not financial advice. All betting involves risk.

## License

MIT License

## Backtesting

To evaluate the model's performance from a betting perspective, you can run the backtesting script:

```bash
python backtest_strategy.py
```

This will simulate the betting strategy on historical data and print an ROI report.

## Automation

The entire prediction pipeline can be automated to run daily. The main entry point for this is the `run_pipeline.py` script.

### Running the Pipeline Manually

You can execute the full pipeline manually by running:

```bash
python run_pipeline.py
```

This will:
1.  Run the (simulated) daily odds scraper.
2.  Run the prediction model on the scraped odds.
3.  Log any identified value bets to `logs/predictions.log`.

### Scheduling with Cron (Linux/macOS)

You can schedule the pipeline to run every day at a specific time (e.g., 9 AM) using `cron`.

1.  Open your crontab for editing:
    ```bash
    crontab -e
    ```

2.  Add the following line to the file. This example schedules the script to run at 9 AM daily. Make sure to replace `/path/to/your/project` with the absolute path to this project's directory and `/path/to/your/python` with the path to your Python executable (you can find this with `which python`).

    ```cron
    0 9 * * * /path/to/your/python /path/to/your/project/run_pipeline.py >> /path/to/your/project/logs/cron.log 2>&1
    ```

### Scheduling with Windows Task Scheduler

1.  **Open Task Scheduler:** Search for "Task Scheduler" in the Start Menu.
2.  **Create Basic Task:** In the "Actions" pane on the right, click "Create Basic Task...".
3.  **Name and Description:** Give the task a name like "Daily Sports Predictions" and click "Next".
4.  **Trigger:** Choose "Daily" and click "Next". Set a time (e.g., 9:00:00 AM) and click "Next".
5.  **Action:** Select "Start a program" and click "Next".
6.  **Start a Program:**
    *   In the "Program/script" box, enter the full path to your Python executable (e.g., `C:\Python39\python.exe`).
    *   In the "Add arguments (optional)" box, enter the full path to the pipeline script (e.g., `C:\path\to\your\project\run_pipeline.py`).
    *   In the "Start in (optional)" box, enter the full path to the project's root directory (e.g., `C:\path\to\your\project`). This is important so the script can find the other files.
7.  **Finish:** Click "Finish" to create the task. You can right-click the task and run it to test.
