import requests
import json
import pandas as pd

class XApiHandler:
    def __init__(self, bearer_token: str) -> None:
        self.bearer_token = bearer_token

    def bearer_oauth(self, r):
        """
        Method required by bearer token authentication.
        """
        r.headers["Authorization"] = f"Bearer {self.bearer_token}"
        r.headers["User-Agent"] = "YourAppName"
        return r

    def get_rules(self) -> dict:
        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream/rules", auth=self.bearer_oauth
        )
        if response.status_code != 200:
            raise Exception(
                f"Cannot get rules (HTTP {response.status_code}): {response.text}"
            )
        print(json.dumps(response.json(), indent=4))
        return response.json()

    def delete_all_rules(self, rules: dict) -> None:
        if rules is None or "data" not in rules:
            return None

        ids = [rule["id"] for rule in rules["data"]]
        payload = {"delete": {"ids": ids}}
        response = requests.post(
            "https://api.twitter.com/2/tweets/search/stream/rules",
            auth=self.bearer_oauth,
            json=payload
        )
        if response.status_code != 200:
            raise Exception(
                f"Cannot delete rules (HTTP {response.status_code}): {response.text}"
            )
        print(json.dumps(response.json(), indent=4))

    def set_rules(self, delete: None) -> None:
        sample_rules = [
            {"value": "NSE OR BSE OR Sensex OR Nifty"},
            {"value": "RELIANCE OR TCS OR INFY OR HDFC OR ICICIBANK"},
            {"value": "#StockMarketIndia OR #NSE OR #BSE OR #Sensex OR #Nifty"},
            {"value": "Indian stock market OR Sensex update OR Nifty news"}
        ]
        payload = {"add": sample_rules}
        response = requests.post(
            "https://api.twitter.com/2/tweets/search/stream/rules",
            auth=self.bearer_oauth,
            json=payload,
        )
        if response.status_code != 201:
            raise Exception(
                f"Cannot add rules (HTTP {response.status_code}): {response.text}"
            )
        print(json.dumps(response.json(), indent=4))

    def get_stream(self) -> pd.DataFrame:
        max_tweets = 100
        output_file = 'tweets.json'
        tweets = {}
        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream", auth=self.bearer_oauth, stream=True,
        )
        if response.status_code != 200:
            raise Exception(
                f"Cannot get stream (HTTP {response.status_code}): {response.text}"
            )

        tweet_count = 0
        for response_line in response.iter_lines():
            if response_line:
                try:
                    json_response = json.loads(response_line)
                    print(json.dumps(json_response, indent=4, sort_keys=True))
                    tweets[tweet_count] = json_response
                    tweet_count += 1
                    if tweet_count >= max_tweets:
                        with open(output_file, 'w', encoding='utf-8') as f:
                            json.dump(tweets, f, ensure_ascii=False, indent=4)
                        return pd.DataFrame.from_dict(tweets, orient='index')
                except json.JSONDecodeError as e:
                    print(f"JSON decode error: {e}")

    def get_X_data_as_df(self) -> pd.DataFrame:
        rules = self.get_rules()
        self.delete_all_rules(rules)
        self.set_rules(None)
        df = self.get_stream()
        return df
