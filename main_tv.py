from db.connection import get_connection  # Import function to establish a database connection
from tmdb.tv_api import fetch_series  # Import function to fetch series data from TMDB
from uploader.tv_uploader import insert_or_update_series_data  # Import function to insert or update series data in the database

# List of TMDB series IDs to be processed
SERIES_IDS = [39435, 90669]

def main():
    """
    Main function to fetch and upload TV series data to the database.
    Iterates over a list of series IDs, fetches their data from TMDB,
    and inserts or updates the data in the database.
    """
    
    # Establish a database connection
    conn = get_connection()
    
    try:
        # Loop through each series ID
        for sid in SERIES_IDS:
            print(f"🔍 Fetching Series ID {sid}")
            try:
                # Fetch series data from TMDB
                series = fetch_series(sid)
                
                # Insert or update the series data in the database
                insert_or_update_series_data(conn, series)
            
            # Handle any errors during fetch or upload
            except Exception as e:
                print(f"❌ Failed to process {sid}: {e}")
                
                # Roll back the transaction in case of failure
                conn.rollback()
    
    # Ensure the database connection is closed after processing
    finally:
        conn.close()
        print("✅ Sync complete. Connection closed.")

# Run the main function if this script is executed directly
if __name__ == "__main__":
    main()