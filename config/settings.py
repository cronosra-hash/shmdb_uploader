import os
from dotenv import load_dotenv

# Load base .env to determine environment
load_dotenv()

ENV = os.getenv("ENV", "development").lower()

# Load environment-specific .env file
if ENV == "production":
    load_dotenv(".env.production", override=True)
else:
    load_dotenv(".env.development", override=True)

# TMDB API Key (shared across environments)
TMDB_API_KEY = os.getenv("TMDB_API_KEY")

# Database Configuration
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
}
DB_OPTIONS = os.getenv("DB_OPTIONS", "")
