import sqlite3
import os

# --- Configuration ---
# Your database file path
DB_FILEPATH = r"C:\Repos\ks-bi-project\src\db\maven_factory.db"

# --- Verification ---

def verify_database_content():
    """Connects to the database, prints schema info, and counts records."""
    conn = None
    try:
        if not os.path.exists(DB_FILEPATH):
            print(f"❌ ERROR: Database file not found at {DB_FILEPATH}")
            return

        conn = sqlite3.connect(DB_FILEPATH)
        cursor = conn.cursor()
        print(f"--- ✅ Connected to Database: {DB_FILEPATH} ---")

        # Get all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        
        if not tables:
             print("\n❌ WARNING: No tables found in the database. Schema loading may have failed.")
             return

        print("\n--- 📊 Table Information & Record Counts ---")
        
        # Check the count for each of the core tables
        for table in tables:
            # Check the column structure (similar to your original script)
            cursor.execute(f'PRAGMA table_info("{table}");')
            columns = [row[1] for row in cursor.fetchall()]
            
            # Check the record count
            cursor.execute(f'SELECT COUNT(*) FROM "{table}";')
            count = cursor.fetchone()[0]
            
            print(f"\n✅ Table: {table}")
            print(f"   - **Record Count:** {count:,}") # Formats count with thousands separator
            print(f"   - Columns: {', '.join(columns)}")

    except sqlite3.Error as e:
        print(f"\n❌ A database error occurred: {e}")
    except Exception as e:
        print(f"\n❌ An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print("\nConnection closed.")

if __name__ == "__main__":
    verify_database_content()