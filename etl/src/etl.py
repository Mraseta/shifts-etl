import os
from datetime import datetime, timedelta

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import Engine, create_engine, text

from utils import fetch_shifts, setup_logger


class ETL:
    def __init__(self, engine: Engine, truncate_tables: bool = True):
        """
        Initializes the ETL process.

        Args:
            engine (Engine): SQLAlchemy engine for database connection.
            truncate_tables (bool): Flag to indicate whether to truncate tables before loading data. Default is True.
        """
        self.logger = setup_logger("ETL")
        self.engine = engine
        self.truncate_tables = truncate_tables

    def __call__(self):
        """
        Executes the ETL process by calling the data processing and KPI calculation functions.
        """
        shifts_df, allowances_df, breaks_df = self.process_and_save_raw_data()
        self.calculate_and_insert_kpis(shifts_df, allowances_df, breaks_df)

    def process_and_save_raw_data(self):
        """
        Processes raw data fetched from the API and saves it to the database.

        If the truncate_tables flag is set to True, it truncates the relevant tables before inserting new data.

        Returns:
            tuple: A tuple containing the DataFrames for shifts, allowances, and breaks.
        """
        # For repeated runs of ETL with a fixed seed, truncate in order not to get IntegrityError
        if self.truncate_tables:
            with self.engine.connect() as connection:
                connection.execute(text("TRUNCATE TABLE shifts CASCADE;"))
                connection.execute(text("TRUNCATE TABLE kpis CASCADE;"))

        response_data = fetch_shifts()

        shifts_data = []
        allowances_data = []
        award_interpretations_data = []
        breaks_data = []

        # Iterate through response data and add objects to data arrays
        for record in response_data:
            shift_cost = 0

            for allowance in record["allowances"]:
                allowances_data.append(
                    {
                        "allowance_id": allowance["id"],
                        "shift_id": record["id"],
                        "allowance_value": allowance["value"],
                        "allowance_cost": allowance["cost"],
                    }
                )
                shift_cost += allowance["cost"]

            for award_interpretation in record["award_interpretations"]:
                award_interpretations_data.append(
                    {
                        "award_id": award_interpretation["id"],
                        "shift_id": record["id"],
                        "award_date": award_interpretation["date"],
                        "award_units": award_interpretation["units"],
                        "award_cost": award_interpretation["cost"],
                    }
                )
                shift_cost += award_interpretation["cost"]

            for break_data in record["breaks"]:
                breaks_data.append(
                    {
                        "break_id": break_data["id"],
                        "shift_id": record["id"],
                        "break_start": break_data["start"],
                        "break_finish": break_data["finish"],
                        "is_paid": break_data["paid"],
                    }
                )

            shifts_data.append(
                {
                    "shift_id": record["id"],
                    "shift_date": record["date"],
                    "shift_start": record["start"],
                    "shift_finish": record["finish"],
                    "shift_cost": shift_cost,
                }
            )

        # Create dataframes from data arrays
        shifts_df = pd.DataFrame(shifts_data)
        shifts_df["shift_date"] = pd.to_datetime(shifts_df["shift_date"])
        shifts_df["shift_start"] = pd.to_datetime(shifts_df["shift_start"], unit="ms")
        shifts_df["shift_finish"] = pd.to_datetime(shifts_df["shift_finish"], unit="ms")

        allowances_df = pd.DataFrame(allowances_data)
        award_interpretations_df = pd.DataFrame(award_interpretations_data)

        breaks_df = pd.DataFrame(breaks_data)
        breaks_df["break_start"] = pd.to_datetime(breaks_df["break_start"], unit="ms")
        breaks_df["break_finish"] = pd.to_datetime(breaks_df["break_finish"], unit="ms")

        # Insert data to database
        try:
            shifts_df.to_sql("shifts", self.engine, if_exists="append", index=False)
            self.logger.info("Successfully inserted shifts data")
            allowances_df.to_sql(
                "allowances", self.engine, if_exists="append", index=False
            )
            self.logger.info("Successfully inserted allowances data")
            award_interpretations_df.to_sql(
                "award_interpretations", self.engine, if_exists="append", index=False
            )
            self.logger.info("Successfully inserted award interpretations data")
            breaks_df.to_sql("breaks", self.engine, if_exists="append", index=False)
            self.logger.info("Successfully inserted breaks data")
        except Exception as e:
            self.logger.error("Error inserting data to database.")
            raise Exception(repr(e))

        # Return dataframes so that we don't have to read from database to calculate KPIs
        return shifts_df, allowances_df, breaks_df

    def calculate_and_insert_kpis(self, shifts_df, allowances_df, breaks_df):
        """
        Calculates key performance indicators (KPIs) based on the provided DataFrames
        and inserts them into the KPIs table in the database.

        Parameters:
            shifts_df (DataFrame): DataFrame containing shift data.
            allowances_df (DataFrame): DataFrame containing allowances data.
            breaks_df (DataFrame): DataFrame containing breaks data.
        """
        breaks_df["break_duration"] = (
            breaks_df["break_finish"] - breaks_df["break_start"]
        )
        shifts_df["shift_duration"] = (
            shifts_df["shift_finish"] - shifts_df["shift_start"]
        )
        shifts_df["shift_duration_hours"] = (
            shifts_df["shift_duration"].dt.total_seconds() / 3600
        )

        mean_break_time = breaks_df["break_duration"].mean()
        mean_break_time_seconds = mean_break_time.total_seconds()
        mean_break_time_minutes = mean_break_time_seconds / 60

        mean_shift_cost = shifts_df["shift_cost"].mean()
        min_shift_duration = shifts_df["shift_duration_hours"].min()
        paid_breaks_num = len(breaks_df[breaks_df["is_paid"]])

        today = pd.to_datetime(datetime.now().date())
        last_14_days = today - timedelta(days=14)
        recent_shifts = shifts_df[
            (shifts_df["shift_date"] >= last_14_days)
            & (shifts_df["shift_date"] <= today)
        ]
        shifts_allowances_df = pd.merge(recent_shifts, allowances_df, on="shift_id")
        max_allowance_cost = shifts_allowances_df["allowance_cost"].max()

        shifts_breaks_df = pd.merge(
            shifts_df, breaks_df, on="shift_id", how="left", indicator=True
        )
        shifts_breaks_df = shifts_breaks_df.sort_values(by="shift_date")
        longest_period = 0
        current_streak = 0

        for _, row in shifts_breaks_df.iterrows():
            if row["_merge"] == "left_only":
                current_streak += 1
            else:
                current_streak = 0

            longest_period = max(longest_period, current_streak)

        kpi_data = [
            {
                "kpi_name": "mean_break_length_in_minutes",
                "kpi_value": mean_break_time_minutes,
            },
            {"kpi_name": "mean_shift_cost", "kpi_value": mean_shift_cost},
            {"kpi_name": "max_allowance_cost_14d", "kpi_value": max_allowance_cost},
            {
                "kpi_name": "max_break_free_shift_period_in_days",
                "kpi_value": longest_period,
            },
            {"kpi_name": "min_shift_length_in_hours", "kpi_value": min_shift_duration},
            {"kpi_name": "total_number_of_paid_breaks", "kpi_value": paid_breaks_num},
        ]

        kpi_df = pd.DataFrame(kpi_data)
        kpi_df["kpi_date"] = datetime.now().date()
        kpi_df["kpi_value"] = kpi_df["kpi_value"].astype(float)
        kpi_df.to_sql("kpis", self.engine, if_exists="append", index=False)
        self.logger.info("Successfully inserted KPIs data")


if __name__ == "__main__":
    load_dotenv()

    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    engine = create_engine(
        f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )

    etl = ETL(engine)
    etl()
