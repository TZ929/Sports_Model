# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install system dependencies for lightgbm and then Python packages
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 git && apt-get clean
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application's code into the container at /app
COPY . .

# Set the PYTHONPATH to include the src directory
ENV PYTHONPATH="/app/src"

# Define the command to run the application
# Replace 'default_game_id' and 'default_player_id' with actual example IDs if available
CMD ["python", "run_prediction.py", "401585437", "3133619"] 