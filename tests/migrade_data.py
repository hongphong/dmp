import pymongo

# scp -i ~/.ssh/id_rsa tests/test_performance.py solscan@51.158.154.149:/tmp
target_client = pymongo.MongoClient("mongodb://admin:123456qwer@solscan-v2-db2:27021,solscan-v2-db3:27021/admin")
db_target = 'solscan'
from_client = pymongo.MongoClient("mongodb://admin:123456qwer@solscan-v2-db2:27021,solscan-v2-db3:27021/admin")
db_sync_from = 'solscan'
import logging
import copy
import time


def sync_data(collection_key):
    batch = int(sys.argv[2])
    logging.info("Start sync data from collection")
    last_checkpoint = get_checkpoint(collection_key)
    logging.info("Last checkpoint for collection: %s - %s" % (collection_key, last_checkpoint))
    sync_from_col = from_client[db_sync_from][collection_key]
    target_col = target_client[db_target][collection_key]
    if last_checkpoint:
        rs = sync_from_col.find({"_id": {"$gt": last_checkpoint}})
    else:
        rs = sync_from_col.find()

    last_id = None
    count = 0
    total = 0
    for item in rs:
        count += 1
        total += 1
        list_data.append(item)
        last_id = item['last_id']
        if count >= batch:
            st = time.time()
            target_col.insert_many(list_data, ordered=False, bypass_document_validation=True)
            print("done after: %s. Number insert: %s. Total: %s" % ((time.time() - st), count, total))
            save_checkpoint(collection_key, last_id)
            list_data = []
            count = 0
    if count > 0:
        st = time.time()
        target_col.insert_many(list_data, ordered=False, bypass_document_validation=True)
        print("done after: %s. Number insert: %s. Total: %s" % ((time.time() - st), count, total))
        save_checkpoint(collection_key, last_id)


def get_checkpoint(collection_key):
    config_table = target_client['config_job']['sync_checkpoint']
    rs = config_table.find({'key': collection_key}).sort('update_time', -1).limit(1)
    rs = list(rs)
    if (rs):
        return rs[0]['last_id']
    return None


def save_checkpoint(collection_key, _id):
    config_table = target_client['config_job']['sync_checkpoint']
    logging.info("Save checkpoint: %s - %s" % (collection_key, _id))
    config_table.update_one({
        "key": collection_key
    }, {
        "$set": {"key": collection_key,
                 "last_id": _id,
                 "update_time": time.time()
                 }
    }, upsert=True)


if __name__ == '__main__':
    import sys

    collection = sys.argv[1]
    max_data = int(sys.argv[2])
    batch = int(sys.argv[3])

    sync_data(collection)
