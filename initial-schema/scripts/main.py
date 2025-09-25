import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import os

MONGO_URI =  "mongodb://student:ftn@localhost:27017/?authSource=admin"
DB_NAME = "grocery_sales_db"
DATA_PATH = "."

PRODUCTS_COLLECTION = "products"
CUSTOMERS_COLLECTION = "customers"
EMPLOYEES_COLLECTION = "employees"
SALES_COLLECTION = "sales"


def connect_to_mongo(uri):
    print("Povezivanje na MongoDB...")
    try:
        client = MongoClient(uri)
        # Provera konekcije
        client.admin.command('ping')
        print("MongoDB konekcija uspešna.")
        return client
    except ConnectionFailure as e:
        print(f"Greška pri povezivanju na MongoDB: {e}")
        return None


def preprocess_lookup_data(path):
    print("Priprema podataka za ugnježđivanje (lookup)...")

    try:
        categories_df = pd.read_csv(os.path.join(path, 'categories.csv'))
        cities_df = pd.read_csv(os.path.join(path, 'cities.csv'))
        countries_df = pd.read_csv(os.path.join(path, 'countries.csv'))
    except FileNotFoundError as e:
        print(f"Greška: CSV fajl nije pronađen. Proverite putanju. {e}")
        return None, None

    categories_map = {
        row['CategoryID']: {
            'CategoryID': row['CategoryID'],
            'CategoryName': row['CategoryName']
        }
        for index, row in categories_df.iterrows()
    }

    locations_df = pd.merge(cities_df, countries_df, on='CountryID', how='left')
    locations_map = {
        row['CityID']: {
            'CityID': row['CityID'],
            'CityName': row['CityName'],
            'Zipcode': row['Zipcode'],
            'Country': {
                'CountryID': row['CountryID'],
                'CountryName': row['CountryName'],
                'CountryCode': row['CountryCode']
            }
        }
        for index, row in locations_df.iterrows()
    }

    print("Lookup podaci uspešno pripremljeni.")
    return categories_map, locations_map


def import_collection(db, collection_name, csv_file, lookup_maps=None):
    print(f"\n--- Obrada kolekcije: {collection_name} ---")

    try:
        df = pd.read_csv(csv_file)
    except FileNotFoundError:
        print(f"Fajl {csv_file} nije pronađen. Preskačem.")
        return

    print(f"Brisanje postojeće kolekcije '{collection_name}'...")
    db[collection_name].delete_many({})

    documents_to_insert = []

    if collection_name == PRODUCTS_COLLECTION:
        categories_map = lookup_maps.get('categories', {})
        for index, row in df.iterrows():
            doc = {
                '_id': row['ProductID'],
                'ProductName': row['ProductName'],
                'Price': row['Price'],
                'Class': row['Class'],
                'ModifyDate': pd.to_datetime(row['ModifyDate']),
                'Resistant': row['Resistant'],
                'IsAllergic': row['IsAllergic'],
                'VitalityDays': row['VitalityDays'],
                'Category': categories_map.get(row['CategoryID'], {})
            }
            documents_to_insert.append(doc)

    elif collection_name == CUSTOMERS_COLLECTION:
        locations_map = lookup_maps.get('locations', {})
        for index, row in df.iterrows():
            doc = {
                '_id': row['CustomerID'],
                'FirstName': row['FirstName'],
                'MiddleInitial': row.get('MiddleInitial'),
                'LastName': row['LastName'],
                'Address': row['Address'],
                'Location': locations_map.get(row['CityID'], {})
            }
            documents_to_insert.append(doc)

    elif collection_name == EMPLOYEES_COLLECTION:
        locations_map = lookup_maps.get('locations', {})
        for index, row in df.iterrows():
            full_location = locations_map.get(row['CityID'], {})
            employee_location = {
                'CityID': full_location.get('CityID'),
                'CityName': full_location.get('CityName')
            }
            doc = {
                '_id': row['EmployeeID'],
                'FirstName': row['FirstName'],
                'MiddleInitial': row.get('MiddleInitial'),
                'LastName': row['LastName'],
                'BirthDate': pd.to_datetime(row['BirthDate']),
                'Gender': row['Gender'],
                'HireDate': pd.to_datetime(row['HireDate']),
                'Location': employee_location
            }
            documents_to_insert.append(doc)

    elif collection_name == SALES_COLLECTION:

        print("Učitavanje očišćenog sales fajla...")

        df = pd.read_csv(csv_file)

        df['SalesDate'] = pd.to_datetime(df['SalesDate'])

        documents_to_insert = df.to_dict('records')

    if documents_to_insert:
        print(f"Ubacivanje {len(documents_to_insert)} dokumenata u '{collection_name}'...")
        db[collection_name].insert_many(documents_to_insert)
        print("Uspešno završeno.")
    else:
        print(f"Nema dokumenata za ubacivanje u '{collection_name}'.")


def main():
    client = connect_to_mongo(MONGO_URI)
    if not client:
        return

    db = client[DB_NAME]

    categories_map, locations_map = preprocess_lookup_data(DATA_PATH)
    if categories_map is None:
        return

    lookup_maps = {
        'categories': categories_map,
        'locations': locations_map
    }

    import_collection(db, PRODUCTS_COLLECTION, os.path.join(DATA_PATH, 'products.csv'), lookup_maps)
    import_collection(db, CUSTOMERS_COLLECTION, os.path.join(DATA_PATH, 'customers.csv'), lookup_maps)
    import_collection(db, EMPLOYEES_COLLECTION, os.path.join(DATA_PATH, 'employees.csv'), lookup_maps)
    import_collection(db, SALES_COLLECTION, os.path.join(DATA_PATH, 'sales_cleaned.csv'))

    print("\nSvi podaci su uspešno importovani u MongoDB bazu!")
    client.close()


if __name__ == "__main__":
    main()