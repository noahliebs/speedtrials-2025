# 💧 Georgia Water Quality System

**A modern, AI-powered water quality information system that transforms Georgia's cryptic drinking water data into actionable insights for the public, operators, and regulators.**

## 🚀 What We Built

We've transformed Georgia's outdated water quality viewer into a modern, conversational interface that makes water safety data accessible to everyone. Our solution ingests raw SDWIS (Safe Drinking Water Information System) data and provides multiple ways to explore and understand water quality across Georgia.

### Key Features

#### 🤖 AI-Powered Chat Interface
- Natural language queries about water systems, violations, and safety
- Intelligent SQL generation with automatic retry on errors
- Context-aware responses that explain technical terms in plain language
- Real-time data retrieval from the SDWIS database

#### 🔍 Advanced Search System
- Multi-criteria search for water systems by:
  - System ID, name, county, or city
  - Water system type (Community, Transient, Non-Transient)
  - Source water type (Groundwater, Surface Water, Mixed)
  - Contact type and sample classifications
- Direct SQL queries with fallback to AI assistance
- Results displayed in user-friendly cards with violation status

#### 📊 Interactive Dashboards
- **Statistics View**: Real-time metrics on Georgia's water systems
  - Total systems and population served
  - Health violations and unresolved issues
  - System type breakdowns with explanations
- **CCR Reports**: Consumer Confidence Report generation
- **Schedule Lookup**: Monitoring schedules and sampling requirements

#### 👥 Multi-Stakeholder Design
1. **For the Public**: 
   - Plain-language explanations of violations and health implications
   - Easy search by city or county
   - Visual indicators for system safety status

2. **For Operators**:
   - System-specific violation tracking
   - Monitoring schedule lookups
   - Compliance status overview

3. **For Regulators**:
   - Comprehensive violation summaries
   - Site visit histories
   - Quick access to system details

## 🛠️ Technical Implementation

### Architecture
- **Frontend**: Streamlit with custom CSS for modern UI
- **Backend**: Python 3.12 with async support
- **Database**: PostgreSQL with optimized SDWIS schema
- **AI**: Google Gemini 1.5 Flash with tool calling for SQL generation
- **ETL**: Custom data pipeline handling Georgia's Q1 2025 data

### Data Processing
- Cleaned and ingested 15+ SDWIS data tables
- Handled data quality issues (missing IDs, filler characters, formatting)
- Created optimized indexes for fast queries
- Built comprehensive foreign key relationships

### Key Components
- `app.py`: Main Streamlit application with view management
- `backend/api_manager.py`: API orchestration with caching
- `backend/chat_manager.py`: LLM integration with SQL retry logic
- `backend/sql_manager.py`: Database operations with query validation
- `ui_components.py`: Reusable UI components
- `search_handlers.py`: Search logic and parameter validation

## 🚦 Getting Started

### Prerequisites
- PostgreSQL
- Python 3.12
- Conda (recommended) or pip

### Quick Setup

1. **Clone and setup environment**
```bash
# Using conda (recommended)
conda env create -f environment.yml
conda activate georgia-water
```

2. **Load the data**
```bash
# Create database
psql -c "CREATE DATABASE sdwis_georgia;"

# Run ETL pipeline
cd etl
chmod +x data_load.sh
./data_load.sh
```

3. **Run the application**
```bash
streamlit run app.py
```

4. **Access via browser**
- Local: http://localhost:8501
- Can use ngrok for remote access

## 🎯 How It Works

### Chat Mode
Ask natural questions like:
- "What water systems serve Atlanta?"
- "Show me all lead violations in Fulton County"
- "Which systems have the most health violations?"

The AI understands context and will:
- Generate appropriate SQL queries
- Retry with corrections if queries fail
- Explain results in plain language
- Highlight safety concerns

### Search Mode
Use the enhanced search interface to:
- Filter by multiple criteria simultaneously
- See results instantly with safety indicators
- Fall back to AI assistance for complex queries

### Developer Mode
Toggle developer mode to see:
- Generated SQL queries
- Raw query results
- LLM conversation flow
- Database performance metrics

Built for the Codegen Speed Trials 2025
