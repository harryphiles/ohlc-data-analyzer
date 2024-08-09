import json
from datetime import datetime
from typing import Iterable, Any
import requests
import pandas as pd
from db_handler import DatabaseHandler
from openai_api import OpenAiApiHandler
from utils import process_data
from config import OPENAI_API_KEY


def get_binance_data(
    symbol: str, interval: str, startTime: int, endTime: int, limit: int = 10
) -> Iterable[Iterable[Any]]:
    url: str = "https://api.binance.com/api/v3/klines"
    params: dict[str, Any] = {
        "symbol": symbol,
        "interval": interval,
        "startTime": startTime,
        "endTime": endTime,
        "limit": limit,
    }
    response: requests.Response = requests.get(url, params=params)
    return response.json()


def test_open_api():
    symbol: str = "BTCUSDT"
    interval: str = "1d"
    db = DatabaseHandler("binance_data.db")
    table_name: str = f"{symbol.lower()}_{interval}"
    csv = db.query_to_csv(table_name)
    print(csv)
    oai = OpenAiApiHandler(OPENAI_API_KEY)
    response = oai.get_average(
        # response = oai.get_average_from_func(
        csv=csv,
        column="open",
        days=2,
    )
    print(f"{response = }")


def process_data_in_db(db: DatabaseHandler, table_name: str, data: pd.DataFrame) -> str:
    if db.table_exists(table_name):
        db.add_data(data, table_name)
        query_result = db.read_data(table_name)
        print("First 5 rows from SQLite database:")
        print(query_result)
    else:
        db.store_data(data, table_name)
        print(f"Table {table_name} does not exist in the database.")
    return db.query_to_csv(table_name)


def main() -> None:
    target = {
        "symbol": "BTCUSDT",
        "interval": "1d",
        "start_time": int(datetime(2024, 1, 1).timestamp() * 1000),
        "end_time": int(datetime(2024, 1, 10).timestamp() * 1000),
    }

    # Get data from Binance
    raw_data: Iterable[Iterable[Any]] = get_binance_data(*target.values())
    print("Sample of raw data:")
    print(json.dumps(list(raw_data)[:2], indent=2))

    # Make tables
    selected_columns = ["timestamp", "open", "high", "low", "close", "volume"]
    df: pd.DataFrame = process_data(raw_data, selected_columns)

    # Store it to DB and format it as csv
    db = DatabaseHandler("binance_data.db")
    table_name: str = f"{target["symbol"].lower()}_{target["interval"]}"
    csv_data = process_data_in_db(db, table_name, df)

    # Pass data to OpenAI API to get calculated data
    oai = OpenAiApiHandler(OPENAI_API_KEY)
    response = oai.get_average(
        csv=csv_data,
        column="open",
        days=2,
    )
    print(f"{response = }")


if __name__ == "__main__":
    main()
    # test_open_api()
