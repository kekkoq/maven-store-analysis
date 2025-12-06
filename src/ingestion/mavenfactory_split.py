import re
import os

def split_insert_data(input_filepath, output_dir="output_insert_data_files"):
    """
    Reads a SQL file containing only INSERT INTO statements and splits them 
    into separate files based on the target table name.
    """
    
    print(f"--- 🚀 Starting Data Splitting: {input_filepath} ---")

    # 1. Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")

    # 2. Read the entire content of the raw SQL data file
    try:
        with open(input_filepath, 'r') as f:
            raw_data_content = f.read()
    except FileNotFoundError:
        print(f"❌ Error: Input file not found at {input_filepath}")
        return
    
    # 3. Split and Save the Data Files
    
    # Regex to capture blocks of INSERT INTO statements for a specific table.
    # This pattern captures the block starting with 'INSERT INTO <table_name>' 
    # up until the start of the next 'INSERT INTO' statement for a DIFFERENT table, or the end of the file.
    # It assumes the INSERTs for the same table are sequential (batched).
    # Group 1: The entire block of INSERTs for one table.
    insert_pattern = re.compile(
        r'(INSERT\s+INTO\s+`?(\w+)`?.*?)(?=\nINSERT\s+INTO\s+`?(\w+)`?|\Z)', 
        re.DOTALL | re.IGNORECASE
    )

    data_sqls = {}

    for match in re.finditer(insert_pattern, raw_data_content):
        sql_block = match.group(1).strip()
        
        # Extract the target table name from the start of the block
        match_table_name = re.search(r'INSERT\s+INTO\s+`?(\w+)`?', sql_block, re.IGNORECASE)
        
        if match_table_name:
            table_name = match_table_name.group(1)
            
            # Store the current block of SQL for that table
            if table_name not in data_sqls:
                data_sqls[table_name] = []
            data_sqls[table_name].append(sql_block)
            
    # Write the data out to individual files
    for table_name, sql_blocks in data_sqls.items():
        data_filename = os.path.join(output_dir, f"{table_name}_insert_data.sql")
        
        # Join all blocks for the table with a newline/separator
        with open(data_filename, 'w') as f:
            f.write("\n\n".join(sql_blocks) + "\n")
            
        print(f"✅ Created Data file: {data_filename}")
        
    print(f"--- ✅ Processing Complete. Found {len(data_sqls)} tables. ---")

# --- Configuration ---
# 1. Updated path to your raw SQL data file
RAW_FILEPATH = r"C:\Repos\ks-bi-project\src\rd\rd_mavenfuzzyfactory.sql"
# 2. Directory to save the output files
OUTPUT_DIR_NAME = "output_insert_data_files" 

# --- Execution ---
if __name__ == "__main__":
    split_insert_data(RAW_FILEPATH, OUTPUT_DIR_NAME)