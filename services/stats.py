from pathlib import Path
from db.helpers import dict_cursor

QUERIES_DIR = Path(__file__).parent.parent / "queries"

def get_all_stats():
    query = (QUERIES_DIR / "all_stats.sql").read_text()
    with dict_cursor() as cursor:
        cursor.execute(query)
        row = cursor.fetchone()
        return row["stats"]
    