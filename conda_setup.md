# 🐍 Conda Environment Setup

## Method 1: Direct from requirements.txt

```bash
# Create conda environment with Python 3.12
conda create -n georgia-water python=3.12

# Activate the environment
conda activate georgia-water

# Install packages from requirements.txt
pip install -r requirements.txt
```

## Method 2: Using environment.yml (Recommended)

Create an `environment.yml` file for better conda integration:

```yaml
# environment.yml
name: georgia-water
channels:
  - conda-forge
  - defaults
dependencies:
  - python=3.12
  - pip
  - pip:
    # Core framework
    - streamlit>=1.28.0
    
    # Database
    - sqlalchemy>=2.0.0
    - psycopg2-binary>=2.9.0
    
    # LLM and AI
    - google-generativeai>=0.7.0
    
    # Template engine
    - jinja2>=3.1.0
    
    # Data manipulation
    - pandas>=2.0.0
    
    # Async support
    - asyncio-extras>=1.3.0
    
    # Environment variables
    - python-dotenv>=1.0.0
    
    # Optional: For better error handling
    - tenacity>=8.2.0
```

Then create the environment:

```bash
# Create environment from yml file
conda env create -f environment.yml

# Activate the environment
conda activate georgia-water
```

## Method 3: One-liner with conda-forge

```bash
# Create environment and install core packages via conda
conda create -n georgia-water python=3.12 -c conda-forge \
  pandas sqlalchemy jinja2 python-dotenv tenacity

# Activate environment
conda activate georgia-water

# Install remaining packages via pip
pip install streamlit google-generativeai psycopg2-binary asyncio-extras
```

## Verification

After setup, verify your environment:

```bash
# Check Python version
python --version
# Should output: Python 3.12.x

# Check installed packages
conda list

# Test key imports
python -c "
import streamlit
import sqlalchemy
import google.generativeai
import pandas
import jinja2
print('✅ All imports successful!')
"
```

## Running the Application

```bash
# Make sure you're in the right environment
conda activate georgia-water

# Run the Streamlit app
streamlit run app.py
```

## Environment Management

```bash
# List all conda environments
conda env list

# Export environment for sharing
conda env export > environment.yml

# Remove environment (if needed)
conda env remove -n georgia-water

# Update packages
conda activate georgia-water
pip install --upgrade -r requirements.txt
```

## Troubleshooting

### If psycopg2-binary fails:
```bash
# Install PostgreSQL development headers first
# On macOS:
brew install postgresql

# On Ubuntu/Debian:
sudo apt-get install libpq-dev

# Then retry pip install
```

### If streamlit has issues:
```bash
# Try installing from conda-forge
conda install -c conda-forge streamlit
```

### PostgreSQL Connection Issues:
```bash
# Install PostgreSQL client tools
conda install -c conda-forge postgresql
```

## Quick Start Script

Create a `setup.sh` script for one-command setup:

```bash
#!/bin/bash
# setup.sh

echo "🐍 Setting up Georgia Water Quality Chatbot environment..."

# Create conda environment
conda create -n georgia-water python=3.12 -y

# Activate environment
eval "$(conda shell.bash hook)"
conda activate georgia-water

# Install packages
pip install -r requirements.txt

echo "✅ Environment setup complete!"
echo "🚀 To run the app:"
echo "   conda activate georgia-water"
echo "   streamlit run app.py"
```

Make it executable and run:

```bash
chmod +x setup.sh
./setup.sh
```