import os
import shutil

# 1️⃣ Set your base directory containing the folders
base_dir = "."  # replace with your path

# 2️⃣ Create the target folder if it doesn't exist
target_dir = os.path.join(base_dir, "systems_data")
os.makedirs(target_dir, exist_ok=True)

# 3️⃣ Loop through all items in the base directory
for folder_name in os.listdir(base_dir):
    folder_path = os.path.join(base_dir, folder_name)
    
    # Only process directories with UUID-like names
    if os.path.isdir(folder_path) and len(folder_name) == 36 and folder_name.count('-') == 4:
        # Look for CSV files inside
        for file_name in os.listdir(folder_path):
            if file_name.lower().endswith(".csv"):
                source_file = os.path.join(folder_path, file_name)
                dest_file = os.path.join(target_dir, file_name)
                
                # Move the CSV to the target folder
                shutil.move(source_file, dest_file)
        
        # Delete the original folder after moving CSVs
        shutil.rmtree(folder_path)

print("All CSVs moved and original folders deleted!")