# Brazilian E-Commerce Data Pipeline

An end-to-end data pipeline that downloads, merges, cleans, and exports the Olist Brazilian E-Commerce dataset. Built with professional engineering practices — modular design, centralised config, structured logging, and a full pytest test suite.

---

## Problem

The Olist dataset ships as 9 separate CSV files. Analysing it requires merging all tables into a single master dataframe with clean data types and no duplicates. This pipeline automates that process in one command.

---

## Project Structure

```
E-commerce_pipeline/
├── data/
│   ├── raw/                        # Downloaded Kaggle CSVs (gitignored)
│   └── processed/
│       └── olist_master.csv        # Final merged output (gitignored)
├── logs/
│   └── pipeline.log                # Auto-generated run logs (gitignored)
├── src/
│   ├── __init__.py
│   ├── config.py                   # Central path constants (ROOT, RAW_DATA, PROCESSED_DATA)
│   ├── utils.py                    # Logger with file + console handlers
│   ├── cleaning.py                 # Core pipeline: load → merge → clean → export
│   └── data_scanner.py             # Schema validation + CSV inspection tool
├── tests/
│   └── test_cleaning.py            # 4 pytest tests
├── setup_data.py                   # One-time Kaggle data download script
├── main.py                         # Pipeline entry point
├── requirements.txt
└── README.md
```

---

## Pipeline

```
setup_data.py          → downloads 9 CSVs from Kaggle to data/raw/
main.py                → runs full pipeline
  └── load_datasets()  → loads all 9 CSVs into a dict of dataframes
  └── merge_datasets() → joins all tables into one master dataframe
  └── clean_data()     → removes duplicates, drops null order_ids, converts dates
  └── exports          → saves olist_master.csv to data/processed/
```

---

## Dataset

- **Source:** [Olist Brazilian E-Commerce — Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- **Tables:** 9 CSV files — orders, customers, order items, payments, reviews, products, sellers, geolocation, category translation
- **Final output:** 119,143 rows × 40 columns

---

## Tables Merged

| Table | Join Key |
|-------|----------|
| olist_orders_dataset | base table |
| olist_customers_dataset | customer_id |
| olist_order_items_dataset | order_id |
| olist_order_payments_dataset | order_id |
| olist_order_reviews_dataset | order_id |
| olist_sellers_dataset | seller_id |
| olist_products_dataset | product_id |
| product_category_name_translation | product_category_name |

---

## How to Run

```bash
# Clone the repo
git clone https://github.com/Rudeus0/E-commerece_pipeline.git
cd E-commerce_pipeline

# Create virtual environment
python -m venv .venv
.venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Download the data (one time only — requires Kaggle API key)
python setup_data.py

# Run the full pipeline
python main.py
```

Output: `data/processed/olist_master.csv`

---

## Run Tests

```bash
pytest tests/ -v
```

Expected output:
```
tests/test_cleaning.py::test_load_datasets    PASSED
tests/test_cleaning.py::test_merge_datasets   PASSED
tests/test_cleaning.py::test_row_count        PASSED
tests/test_cleaning.py::test_clean_data       PASSED
4 passed
```

---

## Kaggle API Setup

1. Go to [kaggle.com](https://www.kaggle.com) → Account → API → Create New Token
2. Download `kaggle.json`
3. Place it at `C:\Users\YourName\.kaggle\kaggle.json`
4. Run `python setup_data.py`

---

## Error Analysis

**Known limitations:**

- `order_delivered_customer_date` has ~2,965 null values — orders not yet delivered at time of data export. Nulls preserved intentionally
- One order can have multiple items — merge on `order_id` produces duplicate order rows, one per item. This is expected behavior
- `olist_geolocation_dataset` excluded from merge — zip code mapping table is too large and not needed for order-level analysis
- Shape unchanged after cleaning (119,143 rows) — raw data was already high quality with no duplicate order IDs

---

## Tech Stack

- Python 3.12
- pandas
- kagglehub
- pathlib
- logging
- pytest

---

## Key Learnings

- Centralised `config.py` means paths are defined once and imported everywhere — no hardcoded strings across files
- `df.copy()` before mutations prevents `SettingWithCopyWarning` and protects the original dataframe
- Left merge preserves all orders even when related data is missing — safer than inner join for real-world data
- Logger with two handlers (file + console) gives both runtime visibility and permanent audit trail
- Schema validation in `data_scanner.py` catches column name changes before the pipeline runs