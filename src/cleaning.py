import pandas as pd
from src.config import RAW_DATA
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