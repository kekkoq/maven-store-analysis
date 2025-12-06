import os
import re
import glob

DATA_DIR = r"C:\Repos\ks-bi-project\src\rd\output_insert_data_files" 

def clean_data_files():
    """Removes non-SQLite statements and prepares files for bulk loading."""
    
    print("--- 🧼 Cleaning SQL Data Files ---")
    data_files = glob.glob(os.path.join(DATA_DIR, "*.sql"))
    
    if not data_files:
        print(f"❌ ERROR: No .sql files found in {DATA_DIR}")
        return

    for filepath in data_files:
        try:
            with open(filepath, 'r') as f:
                content = f.read()
            
            # 1. Remove SET AUTOCOMMIT=0; line (case-insensitive)
            # This is the most critical removal for SQLite compatibility
            content = re.sub(r'^SET AUTOCOMMIT=0;$', '', content, flags=re.MULTILINE | re.IGNORECASE)
            
            # 2. Remove other common non-SQLite comments/commands (e.g., MySQL specific SET commands)
            content = re.sub(r'^\s*--.*$', '', content, flags=re.MULTILINE) # Remove comment lines
            content = re.sub(r'^\s*COMMIT;$', '', content, flags=re.MULTILINE | re.IGNORECASE)
            
            # 3. Clean up leading/trailing whitespace and excessive blank lines
            content = os.linesep.join([s for s in content.splitlines() if s.strip()])

            # Save the cleaned content back to the same file
            with open(filepath, 'w') as f:
                f.write(content)
                
            print(f"✅ Cleaned: {os.path.basename(filepath)}")
            
        except Exception as e:
            print(f"❌ Error cleaning {os.path.basename(filepath)}: {e}")

if __name__ == "__main__":
    clean_data_files()