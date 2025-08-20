# To Run

python -m uvicorn web_ui.app:app --reload

# SHMDB Uploader

This project synchronizes movie and TV series data from The Movie Database (TMDB) into a PostgreSQL database. It uses the TMDB API to fetch metadata and stores it in structured tables for movies, series, seasons, episodes, cast, crew, genres, languages, countries, and production companies.

---

## 📦 Features

- Fetches and updates movie and TV series data from TMDB
- Inserts new entries and updates existing ones in the database
- Logs all changes (field updates, additions) to an `update_logs` table
- Supports dry-run mode for safe testing

---

## 🛠️ Setup

### 1. Clone the repository

### 2. Create and activate a virtual environment

 - On macOS/Linux:
 - bash setup_env.sh

 - On Windows:
 - setup_env.bat

## Environment Variables (.env file):
TMDB_API_KEY=your_tmdb_api_key
DB_NAME=your_db_name
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_HOST=your_db_host
DB_PORT=5432
DB_OPTIONS=

## Usage:
Run Movie Uploader:
 - python main_movie.py

Run TV Series Uploader:
 - python main_tv.py

## Project Structure:
shmdb_uploader/
├── .env
├── README.md
├── requirements.txt
├── config/
│   └── settings.py
├── db/
│   ├── connection.py
│   └── logger.py
├── tmdb/
│   ├── movie_api.py
│   └── tv_api.py
├── uploader/
│   ├── movie_uploader.py
│   └── tv_uploader.py
├── main_movie.py
├── main_tv.py

Step-by-Step: Set Up a Virtual Environment
1. Open your terminal
Navigate to your project folder:
cd "C:\Users\stephen.harland\OneDrive - NHS\Python\shmdb_uploader"

2. Create the virtual environment
Run:
python -m venv venv

3. Activate the virtual environment
On Windows (Command Prompt):
venv\Scripts\activate
On Windows (PowerShell):
.\venv\Scripts\Activate.ps1

4. Install dependencies
Once activated, install your project’s dependencies:
pip install -r requirements.txt

5. Run your Flask app
Navigate to the web folder and run:
cd shmdb_uploader/web
python app.py