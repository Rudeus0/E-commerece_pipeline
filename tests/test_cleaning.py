from src.cleaning import load_datasets, merge_datasets, clean_data


def test_load_datasets():
    ecom = load_datasets()
    assert len(ecom) == 9


def test_merge_datasets():
    ecom = load_datasets()
    ecom_mer = merge_datasets(ecom)

    assert ecom_mer.shape[1] == 40


def test_row_count():
    ecom = load_datasets()
    ecom_mer = merge_datasets(ecom)

    assert ecom_mer.shape[0] == 119143


def test_clean_data():
    ecom = load_datasets()
    ecom_mer = merge_datasets(ecom)
    ecom_cl = clean_data(ecom_mer)
    assert str(ecom_cl["order_purchase_timestamp"].dtype) == "datetime64[us]"
