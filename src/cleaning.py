import pandas as pd
from src.config import RAW_DATA, PROCESSED_DATA
from src.utils import get_logger

logger = get_logger("cleaning")


def load_datasets() -> dict:

    csv_file = list(RAW_DATA.glob("*.csv"))
    logger.info(f"loading: {len(csv_file)} dataset")

    ecom = {}
    for file in csv_file:
        ecom[file.stem] = pd.read_csv(file)
        logger.info(f"Loaded: {file.name}")
    return ecom


def merge_datasets(ecom: dict) -> pd.DataFrame:

    logger.info("merging datasets")

    ecom_mer = ecom["olist_orders_dataset"]

    ecom_mer = ecom_mer.merge(
        ecom["olist_customers_dataset"], on="customer_id", how="left"
    )
    logger.info("merged customer ID")

    ecom_mer = ecom_mer.merge(
        ecom["olist_order_items_dataset"], on="order_id", how="left"
    )
    logger.info("merged order items")

    ecom_mer = ecom_mer.merge(
        ecom["olist_order_payments_dataset"], on="order_id", how="left"
    )
    logger.info("merged order payments")

    ecom_mer = ecom_mer.merge(
        ecom["olist_order_reviews_dataset"], on="order_id", how="left"
    )
    logger.info("merged order review")

    ecom_mer = ecom_mer.merge(ecom["olist_sellers_dataset"], on="seller_id", how="left")
    logger.info("merged sellers")

    ecom_mer = ecom_mer.merge(
        ecom["olist_products_dataset"], on="product_id", how="left"
    )
    logger.info("merged products")

    ecom_mer = ecom_mer.merge(
        ecom["product_category_name_translation"],
        on="product_category_name",
        how="left",
    )
    logger.info("merged category translation")

    logger.info(f"Final shape:{ecom_mer.shape}")
    return ecom_mer


def clean_data(ecom: pd.DataFrame) -> pd.DataFrame:

    logger.info(f"Shape before cleaning: {ecom.shape}")

    ecom_cl = ecom.copy()

    ecom_cl = ecom_cl.drop_duplicates()  # drop duplicates

    ecom_cl = ecom_cl.dropna(subset=["order_id"])  # drop rows where order_id is null

    date_cols = [
        "order_purchase_timestamp",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]

    for col in date_cols:
        ecom_cl[col] = pd.to_datetime(ecom_cl[col])

    logger.info(f"Shape after cleaning: {ecom.shape}")

    return ecom_cl


def run_pipeline() -> pd.DataFrame:
    ecom = load_datasets()
    ecom_mer = merge_datasets(ecom)
    ecom_r = clean_data(ecom_mer)

    # export to processed folder
    out_path = PROCESSED_DATA / "olist_master.csv"
    ecom_r.to_csv(out_path, index=False)

    logger.info(f"Pipeline complete. saved to {out_path}")

    return ecom_r
