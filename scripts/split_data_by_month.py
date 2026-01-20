import pandas as pd
import os
import glob
import logging
import sys
from typing import List

current_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(current_dir, '../airflow/plugins')
sys.path.append(config_path)

try:
    from script_config import RAW_DATA_DIR, PROCESSED_DATA_DIR
except ImportError:
    print("error: could not import script_config")
    sys.exit(1)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def split_data():
    """
    consolidates raw csv files, filters by month, and saves processed monthly files.
    forces utf-8 encoding to fix weird characters.
    """
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

    logger.info(f"looking for csv files in: {RAW_DATA_DIR}")

    all_files = glob.glob(os.path.join(RAW_DATA_DIR, "*.csv"))

    if not all_files:
        logger.error(f"no csv files found in {RAW_DATA_DIR}")
        return

    df_list: List[pd.DataFrame] = []
    for filename in all_files:
        logger.info(f"reading file: {filename}")
        try:
            df = pd.read_csv(filename, low_memory=False, encoding='utf-8')
            df_list.append(df)
        except UnicodeDecodeError:
            logger.warning(f"utf-8 failed for {filename}, trying latin1...")
            df = pd.read_csv(filename, low_memory=False, encoding='latin1')
            df_list.append(df)
        except Exception as e:
            logger.error(f"could not read {filename}: {e}")

    if not df_list:
        return

    full_df = pd.concat(df_list, ignore_index=True)
    full_df.columns = full_df.columns.str.lower()

    logger.info(f"total rows loaded: {len(full_df)}")

    target_col = 'departure'
    if target_col not in full_df.columns:
        logger.error(f"column '{target_col}' not found.")
        return

    logger.info("converting dates...")
    full_df[target_col] = pd.to_datetime(full_df[target_col], errors='coerce')
    full_df = full_df.dropna(subset=[target_col])

    full_df['month_year'] = full_df[target_col].dt.to_period('M')

    unique_months = full_df['month_year'].unique()
    logger.info(f"splitting data into {len(unique_months)} months...")

    for period in unique_months:
        month_data = full_df[full_df['month_year'] == period]

        output_filename = f"{period}.csv"
        output_path = os.path.join(PROCESSED_DATA_DIR, output_filename)

        month_data_to_save = month_data.drop(columns=['month_year'])

        month_data_to_save.to_csv(output_path, index=False, encoding='utf-8-sig')
        logger.info(f"saved {output_filename}")

    logger.info("done")


if __name__ == "__main__":
    split_data()