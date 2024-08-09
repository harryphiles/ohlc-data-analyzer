from openai import OpenAI
import requests
from config import OPENAI_API_KEY


class OpenAiApiHandler:
    def __init__(self, api_key: str) -> None:
        self.client = OpenAI(api_key=api_key)
        pass

    def get_average(self, csv, column, days):
        prompt = f"Get the last {days} values from {column} columns from {csv}. Get average of those values."
        system_content = "You are a calculator. Only return the resulting value."
        completion = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_content},
                {"role": "user", "content": prompt},
            ],
        )

        print(f"{completion.choices[0].message = }")
        return completion

    def get_average_from_func(self, csv, column, days):
        function_description = [
            {
                "name": "calculate_csv_average",
                "description": "Calculate the average of a specified column for the latest n days from CSV data",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "csv": {
                            "type": "string",
                            "description": "The CSV data as a string",
                        },
                        "column": {
                            "type": "string",
                            "description": "The name of the column to average",
                        },
                        "days": {
                            "type": "integer",
                            "description": "The number of latest days to consider",
                        },
                    },
                    "required": ["csv", "column", "days"],
                },
            }
        ]
        prompt = f"Calculate the average of the {column} column for the latest {days} days from the given CSV data. {csv}"
        completion = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            functions=function_description,
            function_call={"name": "calculate_csv_average"},
        )

        print(f"{completion.choices[0].message = }")
        return completion

    def run_chat_completions(self):
        completion = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                # {"role": "system", "content": "You are a data calculation expert."},
                {"role": "user", "content": "Hello!"}
            ],
        )

        print(f"{completion.choices[0].message = }")
        return completion


if __name__ == "__main__":
    oai = OpenAiApiHandler(OPENAI_API_KEY)
    response = oai.run_chat_completions()
    print(f"{response = }")
