import pandas as pd
from datetime import timezone
from pymongo import MongoClient
from config import MONGODB_URI, MONGODB_DATABASE, MYSQL_CONFIG
from modules.flattener import flatten_dataframe
from modules.mysql_loader import create_database, load_dataframe, mark_deleted_documents, get_max_updated_at


def main():
    client = MongoClient(MONGODB_URI)
    db = client[MONGODB_DATABASE]
    collections = db.list_collection_names()

    create_database(MYSQL_CONFIG)

    for collection_name in collections:
        print(f"\n[{collection_name}]")
        collection = db[collection_name]
        last_sync_time = get_max_updated_at(collection_name, MYSQL_CONFIG)
        if last_sync_time is not None:
            mongo_query = {'updated_at': {'$gt': last_sync_time}}
            documents = list(collection.find(mongo_query))
            print(f"  Querying MongoDB with updated_at > {last_sync_time} → {len(documents)} doc(s)")
        else:
            documents = list(collection.find({}))
            print(f"  First run — fetching all {len(documents)} doc(s)")

        if not documents:
            print(f"  No new/updated documents — skipping.")
            continue

        df = pd.json_normalize(documents)

        if df.empty:
            continue

        df_flattened = flatten_dataframe(df)
        load_dataframe(collection_name, df_flattened, MYSQL_CONFIG)

    print("\n--- Deletion Check ---")

    for collection_name in collections:
        collection = db[collection_name]
        all_mongo_ids = [doc['_id'] for doc in collection.find({}, {'_id': 1})]

        if not all_mongo_ids:
            print(f"  [{collection_name}] Empty in MongoDB — skipping.")
            continue

        mark_deleted_documents(collection_name, all_mongo_ids, MYSQL_CONFIG)

    client.close()
    print(f"\nDone.")


if __name__ == "__main__":
    main()
