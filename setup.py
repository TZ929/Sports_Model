"""
Setup script for the NBA/WNBA predictive model project.
"""

import subprocess
import sys
import os
from pathlib import Path

def install_requirements():
    """Install required Python packages."""
    print("Installing required packages...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✅ Requirements installed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error installing requirements: {e}")
        return False
    return True

def create_directories():
    """Create necessary directories."""
    print("Creating project directories...")
    directories = [
        "data/raw",
        "data/processed", 
        "data/models",
        "logs",
        "notebooks",
        "tests"
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"✅ Created directory: {directory}")

def setup_database():
    """Set up the database."""
    print("Setting up database...")
    try:
        # Import after requirements are installed
        from src.utils.database import db_manager
        db_manager.create_tables()
        print("✅ Database tables created successfully!")
    except Exception as e:
        print(f"❌ Error setting up database: {e}")
        return False
    return True

def main():
    """Main setup function."""
    print("🚀 Setting up NBA/WNBA Predictive Model Project")
    print("=" * 50)
    
    # Create directories
    create_directories()
    
    # Install requirements
    if not install_requirements():
        print("❌ Setup failed during requirements installation")
        return
    
    # Setup database
    if not setup_database():
        print("❌ Setup failed during database setup")
        return
    
    print("\n🎉 Setup completed successfully!")
    print("\nNext steps:")
    print("1. Configure your settings in config/config.yaml")
    print("2. Run data collection: python -m src.data_collection.nba_api")
    print("3. Start building models: python -m src.modeling.train_models")
    print("\nHappy modeling! 🏀")

if __name__ == "__main__":
    main() 