from src.dim_currency import create_dim_currency
import pandas as pd
import pytest

@pytest.mark.skip
def test_function_returns_df():
    currency_data = [{"currency_id": 1, "currency_code": "GBP", "created_at": "2022-11-03T14:20:49.962000", "last_updated": "2022-11-03T14:20:49.962000"}, {"currency_id": 2, "currency_code": "USD", "created_at": "2022-11-03T14:20:49.962000", "last_updated": "2022-11-03T14:20:49.962000"}, {"currency_id": 3, "currency_code": "EUR", "created_at": "2022-11-03T14:20:49.962000", "last_updated": "2022-11-03T14:20:49.962000"}]

    result = create_dim_currency(currency_data)
    assert type(result) == pd.DataFrame

@pytest.mark.skip
def test_correct_output_from_func():
      currency_data = [{"currency_id": 1, "currency_code": "GBP", "created_at": "2022-11-03T14:20:49.962000", "last_updated": "2022-11-03T14:20:49.962000"}, {"currency_id": 2, "currency_code": "USD", "created_at": "2022-11-03T14:20:49.962000", "last_updated": "2022-11-03T14:20:49.962000"}, {"currency_id": 3, "currency_code": "EUR", "created_at": "2022-11-03T14:20:49.962000", "last_updated": "2022-11-03T14:20:49.962000"}]

      result = create_dim_currency(currency_data)
      first_line = result.loc[0]
      assert first_line.to_dict() == {"currency_id": 1, "currency_code": "GBP", "currency_name": "Great British Pounds"}

      first_line = result.loc[1]
      assert first_line.to_dict() == {"currency_id": 2, "currency_code": "USD", "currency_name": "US Dollars"}

      first_line = result.loc[2]
      assert first_line.to_dict() == {"currency_id": 3, "currency_code": "EUR", "currency_name": "Euros"}


        



