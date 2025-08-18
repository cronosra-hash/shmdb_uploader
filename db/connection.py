import psycopg2
from config.settings import DB_CONFIG, DB_OPTIONS

def get_connection():
    if DB_OPTIONS:
        return psycopg2.connect(**DB_CONFIG, options=DB_OPTIONS)
    return psycopg2.connect(**DB_CONFIG)
