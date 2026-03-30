import kagglehub
import shutil
from pathlib import Path
from src.utils import get_logger
from src.config import RAW_DATA

logger = get_logger("setup_data")


def fetch_olist_data():

    RAW_DATA.mkdir(parents=True, exist_ok=True)

    try:
        logger.info("Connecting to Kaggle...")
        download_path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")

        logger.info(f"Moving files to {RAW_DATA}...")
        for file in Path(download_path).glob("*.csv"):
            shutil.copy(file, RAW_DATA / file.name)
            logger.info(f"Copied: {file.name}")

        logger.info("Data setup complete!")
    except Exception as e:
        logger.error(f"Error: {e}")


if __name__ == "__main__":
    fetch_olist_data()
