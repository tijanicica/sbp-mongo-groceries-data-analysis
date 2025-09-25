import pandas as pd
import os

DATA_PATH = "."
SALES_FILE = "sales.csv"
PRODUCTS_FILE = "products.csv"
OUTPUT_FILE = "sales_cleaned.csv"


def clean_and_calculate_sales_data():

    sales_path = os.path.join(DATA_PATH, SALES_FILE)
    products_path = os.path.join(DATA_PATH, PRODUCTS_FILE)
    output_path = os.path.join(DATA_PATH, OUTPUT_FILE)

    print("--- Učitavanje podataka ---")
    try:
        print(f"Učitavanje fajla: {sales_path}...")
        df_sales = pd.read_csv(sales_path)
        print(f"Učitavanje fajla: {products_path}...")
        df_products = pd.read_csv(products_path)
        print("Fajlovi uspešno učitani.")
    except FileNotFoundError as e:
        print(f"Greška: Fajl nije pronađen. {e}")
        return

    print("\n--- Čišćenje i priprema podataka ---")

    print("Čišćenje 'SalesDate' kolone...")
    df_sales['SalesDate'] = pd.to_datetime(df_sales['SalesDate'], errors='coerce')

    initial_rows = len(df_sales)
    df_sales.dropna(subset=['SalesDate'], inplace=True)
    rows_dropped = initial_rows - len(df_sales)
    if rows_dropped > 0:
        print(f"Izbačeno {rows_dropped} redova sa nevalidnim ili praznim datumom.")

    df_prices = df_products[['ProductID', 'Price']].copy()

    print("\n--- Spajanje podataka o prodaji i cena proizvoda ---")

    df_merged = pd.merge(df_sales, df_prices, on='ProductID', how='left')

    missing_prices = df_merged['Price'].isnull().sum()
    if missing_prices > 0:
        print(
            f"UPOZORENJE: Pronađeno {missing_prices} prodaja za koje ne postoji cena proizvoda. TotalPrice će biti 0 za njih.")
        df_merged['Price'].fillna(0, inplace=True)

    print("\n--- Izračunavanje nove 'TotalPrice' kolone ---")

    df_merged['TotalPrice'] = (df_merged['Price'] - (df_merged['Price'] * df_merged['Discount'])) * df_merged[
        'Quantity']

    df_merged['TotalPrice'] = df_merged['TotalPrice'].round(2)

    print("Nova 'TotalPrice' kolona je uspešno izračunata.")
    print("Primer prvih 5 izračunatih vrednosti:")
    print(df_merged[['ProductID', 'Price', 'Discount', 'Quantity', 'TotalPrice']].head())


    final_df = df_merged.drop(columns=['Price'])

    print(f"\n--- Čuvanje očišćenog fajla na lokaciji: {output_path} ---")

    final_df.to_csv(output_path, index=False)

    print("\nČišćenje i izračunavanje podataka je završeno!")


if __name__ == "__main__":
    clean_and_calculate_sales_data()