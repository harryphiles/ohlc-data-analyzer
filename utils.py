from typing import Iterable, Any
import pandas as pd


def process_data(
    data: Iterable[Iterable[Any]], selected_columns: list[str]
) -> pd.DataFrame:
    column_names = [
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume",
        "ignore",
    ]
    df: pd.DataFrame = pd.DataFrame(data, columns=column_names)
    df = df[selected_columns]

    if "timestamp" in selected_columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    if "close_time" in selected_columns:
        df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")

    numeric_columns = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_asset_volume",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume",
    ]
    numeric_columns = [col for col in numeric_columns if col in selected_columns]
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, axis=1)

    return df
