import requests
import json
import pandas as pd

  # Configuration
  #use documentatio available on https://ai.jll.com/developer-sandbox to get URL, TOKEN, Subs_KEY
BASE_URL = ""
OKTA_ACCESS_TOKEN = ""
SUBSCRIPTION_KEY = ""

def generate_sql_query(df, table_name):
    column_definitions = ", ".join([f"{row['column_name']} {row['data_type']}" for _, row in df.iterrows()])
    sql_query = f"CREATE TABLE {table_name} ({column_definitions});"
    return sql_query
    
def call_jll_gpt_api(sql_query):
    url = f"{BASE_URL}/chat/completions"
    headers = {
        "Authorization": f"Bearer {OKTA_ACCESS_TOKEN}",
        "Subscription-Key": SUBSCRIPTION_KEY,
        "Content-Type": "application/json"
    }
    data = {
        "model": "GPT_35_TURBO",
        "messages": [
            {"role": "user", "content": f"Adjust following to valid SQL script I can use to create a table. if column data type is empty, use VARCHAR \n\n{sql_query}"}
        ],
        "max_tokens": 2000
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        result = response.json()
        return result['choices'][0]['message']['content']
    except requests.exceptions.RequestException as e:
        return f"Error occurred: {str(e)}"


def process_dataframe_and_generate_query(df, table_name):
    sql_query = generate_sql_query(df, table_name)
    optimized_query = call_jll_gpt_api(sql_query)
    return optimized_query
