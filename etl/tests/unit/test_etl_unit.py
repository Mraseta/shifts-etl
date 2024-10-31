import json
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from src.etl import ETL


class TestETL(unittest.TestCase):
    @patch("src.etl.fetch_shifts")
    @patch("src.etl.pd.DataFrame.to_sql")
    def test_process_and_save_raw_data(self, mock_to_sql, mock_fetch_shifts):
        with open("tests/data/mock_shifts_data.json") as f:
            mock_response_data = json.load(f)

        mock_fetch_shifts.return_value = mock_response_data["results"]

        etl_instance = ETL(MagicMock(), truncate_tables=False)
        shifts_df, allowances_df, breaks_df = etl_instance.process_and_save_raw_data()

        self.assertEqual(len(shifts_df), 2)
        self.assertEqual(len(allowances_df), 3)
        self.assertEqual(len(breaks_df), 2)
        self.assertEqual(
            shifts_df.iloc[0]["shift_id"], "7e6efbe3-0ff1-4b67-8c3f-d38095e3bc5b"
        )
        self.assertEqual(
            allowances_df.iloc[0]["allowance_id"],
            "27325af4-34bb-4afe-9784-19a6554bb364",
        )
        self.assertEqual(
            breaks_df.iloc[0]["break_id"], "948cc21b-703a-4375-8e2b-9b32990e0f8f"
        )
        mock_to_sql.assert_called()

    @patch("src.etl.pd.DataFrame.to_sql")
    def test_calculate_and_insert_kpis(self, mock_to_sql):
        shifts_df = pd.read_csv("tests/data/mock_shifts.csv")
        shifts_df["shift_date"] = pd.to_datetime(shifts_df["shift_date"])
        shifts_df["shift_start"] = pd.to_datetime(shifts_df["shift_start"], unit="ms")
        shifts_df["shift_finish"] = pd.to_datetime(shifts_df["shift_finish"], unit="ms")

        allowances_df = pd.read_csv("tests/data/mock_allowances.csv")

        breaks_df = pd.read_csv("tests/data/mock_breaks.csv")
        breaks_df["break_start"] = pd.to_datetime(breaks_df["break_start"], unit="ms")
        breaks_df["break_finish"] = pd.to_datetime(breaks_df["break_finish"], unit="ms")

        etl_instance = ETL(MagicMock(), truncate_tables=False)
        etl_instance.calculate_and_insert_kpis(shifts_df, allowances_df, breaks_df)

        mock_to_sql.assert_called()

    @patch("src.etl.fetch_shifts")
    @patch("src.etl.pd.DataFrame.to_sql")
    def test_etl_call(self, mock_to_sql, mock_fetch_shifts):
        with open("tests/data/mock_shifts_data.json") as f:
            mock_response_data = json.load(f)

        mock_fetch_shifts.return_value = mock_response_data["results"]
        mock_engine = MagicMock()

        etl_instance = ETL(mock_engine)
        etl_instance()

        self.assertTrue(mock_fetch_shifts.called)
        self.assertEqual(mock_to_sql.call_count, 5)


if __name__ == "__main__":
    unittest.main()
