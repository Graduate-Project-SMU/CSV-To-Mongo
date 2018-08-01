import sys
import csv
from pymongo import MongoClient

MONGO_HOST = '*****'
MONGO_PORT = *****

DOCUMENT_NAME = '******'
COLLECTION_NAME = '*****'

FILENAME = '*****'



def connect_mongo():
    try:
        client = MongoClient(MONGO_HOST,MONGO_PORT)
        db = client[DOCUMENT_NAME]
        return db[COLLECTION_NAME]
    except Exception as e:
        print('error!')
        print(e)
        sys.exit(1)

def read_csv():
    f = open(FILENAME, 'r', encoding="euc-kr")
    return csv.reader(f)

def save_to_mongo():
    collection = connect_mongo()
    data = read_csv()
    json_datas = []
    for d in data:
        if (d[4] == "") or (d[4] is None) or (d[5] == "") or (d[5] is None) : continue
        j = {
            'company' : d[0],
            'branch' : d[1],
            'address' : d[2],
            'telephone' : d[3],
            'location' : {
                'type' : 'Point',
                'coordinates': [float(d[5]), float(d[4])]
            }
        }
        json_datas.append(j)

    try:
        result = collection.insert_many(json_datas)
        print('%d rows are saved to "%s" collection in "%s" document successfully!' % (len(result.inserted_ids), COLLECTION_NAME, DOCUMENT_NAME))
        sys.exit(1)
    except Exception as e:
         print('error')
         print(e)
         sys.exit(1)

if __name__ == '__main__':
     save_to_mongo()
