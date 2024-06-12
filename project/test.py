import os
from pipeline import load_data, clean_data, save_to_sql

def main():
    urls = [
        "https://data.london.gov.uk/download/leggi/2d6ee3f1-e928-48a9-8eab-01748c65ac6f/energy-consumption-borough-leggi.csv", 
        "https://data.london.gov.uk/download/leggi/fcf8a0a3-2051-484a-ba9a-5c8bc2268a3e/co2-emissions-borough-leggi.csv"
    ]
    names = ["energy_consumption", "co2_emissions"]
    output_path = "./data"

    for i, url in enumerate(urls):
        df = load_data(url)
        if df is not None:
            df = clean_data(df)
            if df is not None:
                save_to_sql(df, names[i], output_path)

if __name__ == "__main__":
    main()

    # Check if output files exist
    expected_files = ["data/energy_consumption.db", "data/co2_emissions.db"]
    missing_files = [f for f in expected_files if not os.path.isfile(f)]

    if not missing_files:
        print("Test Passed: All expected files exist.")
    else:
        print(f"Test Failed: Missing files - {', '.join(missing_files)}")
        exit(1)
