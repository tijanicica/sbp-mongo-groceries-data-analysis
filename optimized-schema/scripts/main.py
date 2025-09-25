import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import os

MONGO_URI = "mongodb://student:ftn@localhost:27017/?authSource=admin"
DB_NAME = "grocery_sales_db"
DATA_PATH = "."

PRODUCTS_COLLECTION = "products"
CUSTOMERS_COLLECTION = "customers"
EMPLOYEES_COLLECTION = "employees"
SALES_COLLECTION = "sales" 
SALES_DENORMALIZED_COLLECTION = "sales_denormalized"


def connect_to_mongo(uri):
    """Povezuje se na MongoDB i proverava konekciju."""
    print("Povezivanje na MongoDB...")
    try:
        client = MongoClient(uri)
        client.admin.command('ping')
        print("MongoDB konekcija uspešna.")
        return client
    except ConnectionFailure as e:
        print(f"Greška pri povezivanju na MongoDB: {e}")
        return None


def prepare_data_maps(path):
    """
    Priprema sve potrebne podatke iz CSV fajlova i pretvara ih u rečnike (mape)
    radi brzog pristupa tokom denormalizacije.
    """
    print("\n--- Priprema lookup mapa za denormalizaciju ---")
    try:
        categories_df = pd.read_csv(os.path.join(path, 'categories.csv'))
        cities_df = pd.read_csv(os.path.join(path, 'cities.csv'))
        countries_df = pd.read_csv(os.path.join(path, 'countries.csv'))
        products_df = pd.read_csv(os.path.join(path, 'products.csv'))
        customers_df = pd.read_csv(os.path.join(path, 'customers.csv'))

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
        print("Mapa lokacija kreirana.")

        categories_map = {
            row['CategoryID']: {'CategoryName': row['CategoryName']}
            for index, row in categories_df.iterrows()
        }
        print("Mapa kategorija kreirana.")
        products_map = {
            row['ProductID']: {
                'ProductID': row['ProductID'],
                'ProductName': row['ProductName'],
                'CategoryName': categories_map.get(row['CategoryID'], {}).get('CategoryName'),
                'IsAllergic': row['IsAllergic'],
                'Class': row['Class'],
                'Resistant': row['Resistant']
            }
            for index, row in products_df.iterrows()
        }
        print("Mapa proizvoda kreirana.")
        
        customers_map = {
            row['CustomerID']: {
                'CustomerID': row['CustomerID'],
                'CityName': locations_map.get(row['CityID'], {}).get('CityName')
            }
            for index, row in customers_df.iterrows()
        }
        print("Mapa kupaca kreirana.")

        print("Sve lookup mape su uspešno pripremljene.")
        
        return {
            'categories': categories_map,
            'locations': locations_map,
            'products': products_map,
            'customers': customers_map
        }

    except FileNotFoundError as e:
        print(f"Greška: CSV fajl nije pronađen. Proverite putanju. {e}")
        return None


def import_collection(db, collection_name, csv_file, lookup_maps={}):
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
            documents_to_insert.append({
                '_id': row['ProductID'], 'ProductName': row['ProductName'], 'Price': row['Price'],
                'Class': row['Class'], 'ModifyDate': pd.to_datetime(row['ModifyDate']),
                'Resistant': row['Resistant'], 'IsAllergic': row['IsAllergic'],
                'VitalityDays': row['VitalityDays'],
                'Category': categories_map.get(row['CategoryID'], {})
            })

    elif collection_name == CUSTOMERS_COLLECTION:
        locations_map = lookup_maps.get('locations', {})
        for index, row in df.iterrows():
            documents_to_insert.append({
                '_id': row['CustomerID'], 'FirstName': row['FirstName'],
                'MiddleInitial': row.get('MiddleInitial'), 'LastName': row['LastName'],
                'Address': row['Address'], 'Location': locations_map.get(row['CityID'], {})
            })

    elif collection_name == EMPLOYEES_COLLECTION:
        locations_map = lookup_maps.get('locations', {})
        for index, row in df.iterrows():
            full_location = locations_map.get(row['CityID'], {})
            documents_to_insert.append({
                '_id': row['EmployeeID'], 'FirstName': row['FirstName'],
                'MiddleInitial': row.get('MiddleInitial'), 'LastName': row['LastName'],
                'BirthDate': pd.to_datetime(row['BirthDate']), 'Gender': row['Gender'],
                'HireDate': pd.to_datetime(row['HireDate']),
                'Location': {'CityID': full_location.get('CityID'), 'CityName': full_location.get('CityName')}
            })

    elif collection_name == SALES_COLLECTION:
        df['SalesDate'] = pd.to_datetime(df['SalesDate'])
        documents_to_insert = df.to_dict('records')

    elif collection_name == SALES_DENORMALIZED_COLLECTION:
        print("Priprema obogaćenih (denormalizovanih) sales dokumenata...")
        products_map = lookup_maps.get('products', {})
        customers_map = lookup_maps.get('customers', {})

        df['SalesDate'] = pd.to_datetime(df['SalesDate'])

        for index, row in df.iterrows():
            product_info = products_map.get(row['ProductID'], {})
            customer_info = customers_map.get(row['CustomerID'], {})

            doc = {
                'SalesID': row['SalesID'],
                'SalesDate': row['SalesDate'],
                'Quantity': row['Quantity'],
                'Discount': row['Discount'],
                'TotalPrice': row['TotalPrice'],
                'TransactionNumber': row['TransactionNumber'],
                'Product': {
                    'ProductID': row['ProductID'],
                    'ProductName': product_info.get('ProductName'),
                    'CategoryName': product_info.get('CategoryName'),
                    'IsAllergic': product_info.get('IsAllergic'),
                    'Class': product_info.get('Class'),
                    'Resistant': product_info.get('Resistant')
                },
                'Customer': {
                    'CustomerID': row['CustomerID'],
                    'CityName': customer_info.get('CityName')
                },
                'SalesPerson': {
                    'SalesPersonID': row['SalesPersonID']
                }
            }
            documents_to_insert.append(doc)

    if documents_to_insert:
        print(f"Ubacivanje {len(documents_to_insert)} dokumenata u '{collection_name}'...")
        db[collection_name].insert_many(documents_to_insert, ordered=False) 
        print("Uspešno završeno.")
    else:
        print(f"Nema dokumenata za ubacivanje u '{collection_name}'.")


def main():
    client = connect_to_mongo(MONGO_URI)
    if not client:
        return

    db = client[DB_NAME]

    lookup_maps = prepare_data_maps(DATA_PATH)
    if not lookup_maps:
        print("Greška pri pripremi mapa. Prekidam izvršavanje.")
        return

    import_collection(db, PRODUCTS_COLLECTION, os.path.join(DATA_PATH, 'products.csv'), lookup_maps)
    import_collection(db, CUSTOMERS_COLLECTION, os.path.join(DATA_PATH, 'customers.csv'), lookup_maps)
    import_collection(db, EMPLOYEES_COLLECTION, os.path.join(DATA_PATH, 'employees.csv'), lookup_maps)
    import_collection(db, SALES_COLLECTION, os.path.join(DATA_PATH, 'sales_cleaned.csv'))
    
    import_collection(db, SALES_DENORMALIZED_COLLECTION, os.path.join(DATA_PATH, 'sales_cleaned.csv'), lookup_maps)

    print("\nSvi podaci su uspešno importovani u MongoDB bazu!")
    print(f"Kreirana je optimizovana kolekcija: '{SALES_DENORMALIZED_COLLECTION}'")
    client.close()


if __name__ == "__main__":
    main()