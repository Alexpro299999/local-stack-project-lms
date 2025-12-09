import pandas as pd
import os
import glob

def split_data():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_data_dir = os.path.join(base_dir, 'data', 'raw')
    processed_data_dir = os.path.join(base_dir, 'data', 'processed')

    os.makedirs(processed_data_dir, exist_ok=True)

    print(f"Looking for CSV files in: {raw_data_dir}")

    all_files = glob.glob(os.path.join(raw_data_dir, "*.csv"))

    if not all_files:
        print("Error: No CSV files found in data/raw")
        return

    df_list = []
    for filename in all_files:
        print(f"Reading file: {filename}")
        try:
            df = pd.read_csv(filename, low_memory=False)
            df_list.append(df)
        except Exception as e:
            print(f"Could not read {filename}: {e}")

    if not df_list:
        return

    full_df = pd.concat(df_list, ignore_index=True)
    
    full_df.columns = full_df.columns.str.lower()
    
    print(f"Total rows loaded: {len(full_df)}")
    print(f"Columns found: {list(full_df.columns)}")

    target_col = 'departure'
    
    if target_col not in full_df.columns:
        print(f"Error: Column '{target_col}' not found.")
        return

    print("Converting dates...")
    full_df[target_col] = pd.to_datetime(full_df[target_col], errors='coerce')
    
    full_df = full_df.dropna(subset=[target_col])

    full_df['month_year'] = full_df[target_col].dt.to_period('M')

    unique_months = full_df['month_year'].unique()
    print(f"Splitting data into {len(unique_months)} months...")

    for period in unique_months:
        month_data = full_df[full_df['month_year'] == period]
        
        output_filename = f"{period}.csv"
        output_path = os.path.join(processed_data_dir, output_filename)
        
        month_data_to_save = month_data.drop(columns=['month_year'])
        
        month_data_to_save.to_csv(output_path, index=False)
        print(f"Saved {output_filename}")

    print("Done")

if __name__ == "__main__":
    split_data()