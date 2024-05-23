import pandas as pd
import io
import requests
from sqlalchemy import create_engine
import os

def load_data(url):
    try:
        dataset = requests.get(url).content
        df = pd.read_csv(io.StringIO(dataset.decode('utf-8')))
        return df
    except Exception as e:
        print(f"Error: {e}")
        return None
    
def clean_data(df):
    try:
        df = df.dropna()
        return df
    except Exception as e:
        print(f"Error: {e}")
        return None

def save_to_sql(df, table_name, output_path):

    os.makedirs(output_path, exist_ok=True)
    engine = create_engine(f'sqlite:///{output_path}/{table_name}.db')
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

def main():
    urls = ["https://data.london.gov.uk/download/leggi/2d6ee3f1-e928-48a9-8eab-01748c65ac6f/energy-consumption-borough-leggi.csv", 
            "https://data.london.gov.uk/download/leggi/fcf8a0a3-2051-484a-ba9a-5c8bc2268a3e/co2-emissions-borough-leggi.csv"]
    names = ["energy_consumption", "co2_emissions"]
    output_path = "./data"

    for i, path in enumerate(urls):
        df = load_data(path)
        df = clean_data(df)
        save_to_sql(df, names[i], output_path)

if __name__ == "__main__":
    main()