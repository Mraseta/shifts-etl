import json
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import testing.postgresql
from sqlalchemy import create_engine, text

from src.etl import ETL


class TestETL(unittest.TestCase):
    @patch("src.etl.fetch_shifts")
    def test_etl_call(self, mock_fetch_shifts):
        with open("tests/data/mock_shifts_data.json") as f:
            mock_response_data = json.load(f)

        today = datetime.now().date()
        for i, record in enumerate(mock_response_data["results"]):
            # Move mock data so that we have data in last 14 days
            new_date = today - timedelta(days=i + 1)
            record["date"] = new_date.strftime("%Y-%m-%d")

        mock_fetch_shifts.return_value = mock_response_data["results"]

        sql_file_path = "../initdb.sql"

        with open(sql_file_path, "r") as file:
            sql_commands = file.read()

        commands = sql_commands.split(";")

        with testing.postgresql.Postgresql() as postgresql:
            engine = create_engine(postgresql.url())

            # Create tables in testing Postgres DB
            with engine.connect() as connection:
                with connection.begin():
                    for command in commands:
                        command = command.strip()
                        if command:
                            connection.execute(text(command))

            etl_instance = ETL(engine)
            etl_instance()

            # Fetch data from testing Postgres DB
            with engine.connect() as connection:
                result_shifts = connection.execute(text("SELECT * FROM shifts;"))
                shifts_data = result_shifts.fetchall()

                result_breaks = connection.execute(text("SELECT * FROM breaks;"))
                breaks_data = result_breaks.fetchall()

                result_allowances = connection.execute(
                    text("SELECT * FROM allowances;")
                )
                allowances_data = result_allowances.fetchall()

                result_award_interpretations = connection.execute(
                    text("SELECT * FROM award_interpretations;")
                )
                award_interpretations_data = result_award_interpretations.fetchall()

                result_kpis = connection.execute(text("SELECT * FROM kpis;"))
                kpis_data = result_kpis.fetchall()
                print(kpis_data)

        self.assertEqual(str(shifts_data[0][0]), "7e6efbe3-0ff1-4b67-8c3f-d38095e3bc5b")
        self.assertEqual(len(shifts_data), 2)

        self.assertEqual(str(breaks_data[0][0]), "948cc21b-703a-4375-8e2b-9b32990e0f8f")
        self.assertEqual(len(breaks_data), 2)

        self.assertEqual(
            str(allowances_data[0][0]), "27325af4-34bb-4afe-9784-19a6554bb364"
        )
        self.assertEqual(len(allowances_data), 3)

        self.assertEqual(
            str(award_interpretations_data[0][0]),
            "778244cd-0803-46f1-bef2-73f829414a94",
        )
        self.assertEqual(len(award_interpretations_data), 2)

        self.assertEqual(kpis_data[0][1], "mean_break_length_in_minutes")
        self.assertEqual(float(kpis_data[0][3]), 24.41)
        self.assertEqual(kpis_data[1][1], "mean_shift_cost")
        self.assertEqual(float(kpis_data[1][3]), 81.55)
        self.assertEqual(kpis_data[2][1], "max_allowance_cost_14d")
        self.assertEqual(float(kpis_data[2][3]), 29.70)
        self.assertEqual(kpis_data[3][1], "max_break_free_shift_period_in_days")
        self.assertEqual(float(kpis_data[3][3]), 0.00)
        self.assertEqual(kpis_data[4][1], "min_shift_length_in_hours")
        self.assertEqual(float(kpis_data[4][3]), 8.75)
        self.assertEqual(kpis_data[5][1], "total_number_of_paid_breaks")
        self.assertEqual(float(kpis_data[5][3]), 1.00)


if __name__ == "__main__":
    unittest.main()
