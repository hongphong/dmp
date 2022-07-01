import pymongo

data_temp = {
    "blockTime": 1643112447,
    "meta": {
        "fee": 5000,
        "innerInstructions": [
            {
                "index": 0,
                "instructions": [
                    {
                        "parsed": {
                            "info": {
                                "amount": "3498189823",
                                "authority": "Cv6FMXrcznHY7yhgyhM5FyP5ZRKRX6jBKWAZEa5vD3WY",
                                "destination": "CVc7W9xU9egGcs3koo5kPTHbqcAWMvGi4JCTE3UUMp3n",
                                "source": "46zQSbBRePTzouqFPFHcG7mkP9ysWaZmpMEBq7FTKxcs"
                            },
                            "type": "transfer"
                        },
                        "program": "spl-token",
                        "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
                    },
                    {
                        "parsed": {
                            "info": {
                                "amount": "3498189823",
                                "authority": "Cv6FMXrcznHY7yhgyhM5FyP5ZRKRX6jBKWAZEa5vD3WY",
                                "destination": "CVc7W9xU9egGcs3koo5kPTHbqcAWMvGi4JCTE3UUMp3n",
                                "source": "46zQSbBRePTzouqFPFHcG7mkP9ysWaZmpMEBq7FTKxcs"
                            },
                            "type": "transfer"
                        },
                        "program": "spl-token",
                        "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
                    },
                    {
                        "parsed": {
                            "info": {
                                "amount": "3498189823",
                                "authority": "Cv6FMXrcznHY7yhgyhM5FyP5ZRKRX6jBKWAZEa5vD3WY",
                                "destination": "CVc7W9xU9egGcs3koo5kPTHbqcAWMvGi4JCTE3UUMp3n",
                                "source": "46zQSbBRePTzouqFPFHcG7mkP9ysWaZmpMEBq7FTKxcs"
                            },
                            "type": "transfer"
                        },
                        "program": "spl-token",
                        "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
                    },
                    {
                        "parsed": {
                            "info": {
                                "amount": "3498189823",
                                "authority": "Cv6FMXrcznHY7yhgyhM5FyP5ZRKRX6jBKWAZEa5vD3WY",
                                "destination": "CVc7W9xU9egGcs3koo5kPTHbqcAWMvGi4JCTE3UUMp3n",
                                "source": "46zQSbBRePTzouqFPFHcG7mkP9ysWaZmpMEBq7FTKxcs"
                            },
                            "type": "transfer"
                        },
                        "program": "spl-token",
                        "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
                    },
                    {
                        "parsed": {
                            "info": {
                                "amount": "3498189823",
                                "authority": "Cv6FMXrcznHY7yhgyhM5FyP5ZRKRX6jBKWAZEa5vD3WY",
                                "destination": "CVc7W9xU9egGcs3koo5kPTHbqcAWMvGi4JCTE3UUMp3n",
                                "source": "46zQSbBRePTzouqFPFHcG7mkP9ysWaZmpMEBq7FTKxcs"
                            },
                            "type": "transfer"
                        },
                        "program": "spl-token",
                        "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
                    },

                ]
            }]
    }}
# scp -i ~/.ssh/id_rsa tests/test_performance.py solscan@51.158.154.149:/tmp
myclient = pymongo.MongoClient("mongodb://root:123456qwer@solscan-v2-db1:27021,solscan-v2-db2:27021,solscan-v2-db3:27021/admin")
mydb = myclient["test_shard"]
mycol = mydb["shard_zone"]
import copy
import time


def test_insert():
    import sys
    import json
    max_data = int(sys.argv[1])
    batch = int(sys.argv[2])
    print("Start test with number insert: %s and batch %s" % (max_data, batch))

    count = 0
    list_data = []
    total = 0
    first_time = time.time()
    last_index = 0

    # data_temp['meta'] = json.dumps(data_temp['meta'],separators=(',', ':'))
    import random
    for k in range(0, max_data):
        d = copy.copy(data_temp)
        last_index = k
        d['index'] = k
        d['shard_zone'] = random.randint(0, 9)
        list_data.append(d)
        count += 1
        total += 1
        if count >= batch:
            st = time.time()
            mycol.insert_many(list_data, ordered=False, bypass_document_validation=True)
            print("done after: %s. Number insert: %s. Total: %s" % ((time.time() - st), count, total))
            list_data = []
            count = 0

    if count > 0:
        st = time.time()
        mycol.insert_many(list_data, ordered=False, bypass_document_validation=True)
        print("done after: %s. Number insert: %s. Total: %s" % ((time.time() - st), count, total))
        list_data = []
        count = 0
    rs = mycol.find().sort("_id", -1).limit(1)
    rs = list(rs)[0]
    print(rs['index'], last_index)
    print('done after: %s for total %s' % (time.time() - first_time, total))


def test_read():
    rs = mycol.find()
    count = 0
    st = time.time()
    first = time.time()
    total = 0
    for k in rs:
        count += 1
        total += 1
        if count >= 10000:
            print('process 10000 after: %s' % (time.time() - st))
            count = 0
            st = time.time()
    print('process %s after %s' % (total, (time.time() - first)))

if __name__ == '__main__':
    test_insert()
