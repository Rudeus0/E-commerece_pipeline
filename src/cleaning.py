import pandas as pd
from src.config import RAW_DATA,PROCESSED_DATA
from src.utils import get_logger

logger = get_logger("cleaning")

def load_datasets() -> dict:
    
    csv_file =list(RAW_DATA.glob("*.csv"))
    logger.info(f"loading: {len(csv_file)} dataset")
    
    ecom={}
    for file in csv_file:

        ecom[file.stem]= pd.read_csv(file)
        logger.info(f"Loaded: {file.name}")
    return ecom
    

def merge_datasets(ecom: dict) -> pd.DataFrame:
    
    logger.info("merging datasets")

    ecom_merge = ecom["olist_orders_dataset"]
    
    ecom_merge = ecom_merge.merge(ecom["olist_customers_dataset"], on="customer_id", how="left")
    logger.info("merged customer ID")
    
    ecom_merge = ecom_merge.merge(ecom["olist_order_items_dataset"], on="order_id", how="left")
    logger.info("merged order items")    
    
    ecom_merge = ecom_merge.merge(ecom["olist_order_payments_dataset"], on="order_id", how="left")
    logger.info("merged order payments")    
    
    
    ecom_merge = ecom_merge.merge(ecom["olist_order_reviews_dataset"], on="order_id", how="left")
    logger.info("merged order review")
    
    ecom_merge = ecom_merge.merge(ecom["olist_sellers_dataset"], on="seller_id", how="left")
    logger.info("merged sellers")

    ecom_merge = ecom_merge.merge(ecom["olist_products_dataset"], on="product_id", how="left")
    logger.info("merged products")

    ecom_merge = ecom_merge.merge(ecom["product_category_name_translation"], on="product_category_name", how="left")
    logger.info("merged category translation")
    
    logger.info(f"Final shape:{ecom_merge.shape}")
    return ecom_merge


def clean_data(ecom: pd.DataFrame) -> pd.DataFrame:
    
    logger.info(f"Shape before cleaning: {ecom.shape}")
    
    ecom = ecom.copy()
    
    ecom = ecom.drop_duplicates() # drop duplicates
    
    ecom = ecom.dropna(subset=["order_id"]) # drop rows where order_id is null
    
    date_cols = [
        "order_purchase_timestamp",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]
    
    for col in date_cols:
        ecom[col] = pd.to_datetime(ecom[col])
        
    
    logger.info(f"Shape after cleaning: {ecom.shape}")
    
    return ecom

def run_pipeline() -> pd.DataFrame:
    ecom = load_datasets()
    ecom_merge = merge_datasets(ecom)
    ecom = clean_data(ecom_merge)
    
        # export to processed folder
    out_path = PROCESSED_DATA /"olist_master.csv"
    ecom.to_csv(out_path, index=False)
    
    logger.info(f"Pipeline complete. saved to {out_path}")
    
    return ecom