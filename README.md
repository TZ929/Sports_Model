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
