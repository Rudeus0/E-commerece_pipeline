import pandas as pd
from src.utils import get_logger
from src.config import RAW_DATA


logger = get_logger("data_scanner")

# CONSTANTS (Global Configuration)
EXPECTED_SCHEMA = {
    "olist_orders_dataset.csv": ["order_id", "customer_id", "order_status"],
    "olist_customers_dataset.csv": ["customer_id", "customer_zip_code_prefix"],
}


def scan_raw_data():


    if not RAW_DATA.exists():
        logger.error(f"Path not found {RAW_DATA}")
        return

    csv_files = list(RAW_DATA.glob("*.csv"))
    logger.info(f"Found {len(csv_files)} datasets")

    for file in csv_files:
        try:
            ecom = pd.read_csv(file, nrows=5)

            if file.name in EXPECTED_SCHEMA:
                missing = [
                    cols
                    for cols in EXPECTED_SCHEMA[file.name]
                    if cols not in ecom.columns
                ]
                if missing:
                    logger.warning(f"Missing Columns in {file.name}: {missing}")
                else:
                    logger.info(f"schema for {file.name} valid")

            print(f"\n\nColumns: {list(ecom.columns)}")
            print(f"\nColumn count: {ecom.shape[1]}")
            print(f"Row count: {ecom.shape[0]}")

        except Exception as e:
            logger.error(f" Could not read {file.name}: {e}")


if __name__ == "__main__":
    scan_raw_data()
