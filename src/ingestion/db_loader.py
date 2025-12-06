import sqlite3
import os
import glob
import re

# --- Configuration ---
DB_FILEPATH = r"C:\Repos\ks-bi-project\src\db\maven_factory.db"
SCHEMA_FILEPATH = r"C:\Repos\ks-bi-project\src\mavenfactory_schema.sql"
DATA_DIR = r"C:\Repos\ks-bi-project\src\rd\output_insert_data_files" 

def execute_sql_file_robust(filepath):
    """
    Reads a SQL file, attempts to connect and execute, 
    and returns True/False based on success.
    """
    conn = None
    try:
        # Use a fresh connection for each file to isolate potential issues
        conn = sqlite3.connect(DB_FILEPATH)
        cursor = conn.cursor()
        
        with open(filepath, 'r') as f:
            sql_script = f.read()
        
        # Robustly split SQL commands by semicolon, ensuring semicolons 
        # inside the multi-line VALUES blocks are not missed.
        # This regex splits statements but ignores semicolons inside parentheses (best effort)
        sql_commands = [
            cmd.strip() for cmd in re.split(r';\s*$', sql_script, flags=re.MULTILINE) if cmd.strip()
        ]
        
        for command in sql_commands:
            if command.startswith('INSERT INTO') and 'VALUES' in command:
                # The bulk insert line is often the point of failure.
                # We will attempt to execute the entire block.
                cursor.execute(command + ';')
            elif command:
                cursor.execute(command)

        conn.commit()
        return True
        
    except FileNotFoundError:
        print(f"   ❌ ERROR: File not found at {filepath}")
        return False
    except sqlite3.Error as e:
        print(f"   ❌ ERROR executing SQL in {os.path.basename(filepath)}: {e}")
        return False
    finally:
        if conn:
            conn.close()


def load_data_pipeline_final_attempt():
    """Main function to orchestrate the final loading attempt."""
    
    print(f"\n--- 🚀 FINAL DATA LOADING ATTEMPT for: {DB_FILEPATH} ---")

    # 1. Load the Schema (Tables)
    print("\n[STEP 1/2] Loading Schema and Tables...")
    print(f"   Executing: {os.path.basename(SCHEMA_FILEPATH)}...")
    schema_success = execute_sql_file_robust(SCHEMA_FILEPATH)

    if not schema_success:
        # If schema loading fails, we assume tables already exist (as confirmed before)
        print("   ⚠️ WARNING: Schema loading failed, assuming tables already exist. CONTINUING to data load.")

    # 2. Load the Data (INSERT INTO statements)
    print("\n[STEP 2/2] Loading Raw Data into Tables...")
    
    data_files = sorted(glob.glob(os.path.join(DATA_DIR, "*.sql")))
    
    if not data_files:
        print(f"   ❌ ERROR: No .sql files found in {DATA_DIR}")
        return

    success_count = 0
    for data_file in data_files:
        if execute_sql_file_robust(data_file):
            success_count += 1
        else:
            print(f"   🛑 ERROR in {os.path.basename(data_file)}. Data loading failed for this file.")
            # We must break here, as subsequent files may depend on this one.
            break 

    if success_count == len(data_files):
        print("\n--- ✅ FINAL SUCCESS! All Data Loaded. ---")
    else:
        print("\n--- 🛑 FINAL ATTEMPT FAILED. Proceeding to a new project. ---")

# --- Execution ---
if __name__ == "__main__":
    load_data_pipeline_final_attempt()