import sqlite3
import io
import pandas as pd


class DatabaseHandler:
    def __init__(self, db_name: str):
        self.db_name = db_name
        self.conn = sqlite3.connect(db_name)
        self.curr = self.conn.cursor()
        print(f"Connected to database: {db_name}")

    def __del__(self):
        if hasattr(self, "conn") and self.conn:
            self.conn.close()
            print(f"Connection to database {self.db_name} closed")

    def store_data(self, df: pd.DataFrame, table_name: str) -> None:
        df.to_sql(table_name, self.conn, if_exists="replace", index=False)
        self.conn.commit()
        print(
            f"Data stored in SQLite database '{self.db_name}' in table '{table_name}'"
        )

    def read_data(self, table_name: str, limit: int = 5) -> pd.DataFrame:
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        return pd.read_sql_query(query, self.conn)

    def table_exists(self, table_name: str) -> bool:
        self.curr.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
        )
        return self.curr.fetchone() is not None

    def execute_query(self, query: str) -> None:
        self.curr.execute(query)
        self.conn.commit()

    def fetch_all(self, query: str) -> list:
        self.curr.execute(query)
        return self.curr.fetchall()

    def close_connection(self) -> None:
        if self.conn:
            self.conn.close()
            print(f"Connection to database {self.db_name} closed")

    def query_to_csv(self, table_name: str) -> str:
        query = f"SELECT * FROM {table_name} ORDER BY timestamp ASC"
        df = pd.read_sql_query(query, self.conn)

        # Use StringIO to create a string buffer
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        # Get the CSV string and reset the buffer
        csv_string = csv_buffer.getvalue()
        csv_buffer.close()

        return csv_string

    def add_data(self, df: pd.DataFrame, table_name: str) -> None:
        expected_columns = ["timestamp", "open", "high", "low", "close", "volume"]
        if not all(col in df.columns for col in expected_columns):
            raise ValueError(f"DataFrame must contain columns: {expected_columns}")

        df["timestamp"] = pd.to_datetime(df["timestamp"])

        existing_data = pd.read_sql_query(f"SELECT * FROM {table_name}", self.conn)
        existing_data["timestamp"] = pd.to_datetime(existing_data["timestamp"])

        new_rows = df[~df["timestamp"].isin(existing_data["timestamp"])]

        if not new_rows.empty:
            # Append new rows to the existing table
            new_rows.to_sql(table_name, self.conn, if_exists="append", index=False)
            self.conn.commit()
            print(f"{len(new_rows)} new rows added to table '{table_name}'")
        else:
            print("No new data to add. All timestamps already exist in the table.")


if __name__ == "__main__":
    symbol: str = "BTCUSDT"
    interval: str = "1d"
    db = DatabaseHandler("binance_data.db")
    table_name: str = f"{symbol.lower()}_{interval}"
    csv = db.query_to_csv(table_name)
    print(csv)
