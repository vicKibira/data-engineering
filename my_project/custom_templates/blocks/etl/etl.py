import pandas as pd
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    return pd.read_csv('/home/src/pizza_sales.csv')


@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'