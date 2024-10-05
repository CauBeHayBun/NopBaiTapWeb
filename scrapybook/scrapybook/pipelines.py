import scrapy
import pymongo
import json
import csv
import os
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

class MongoDBmyscrapybookPipeline:
    def __init__(self):
        # Connection String
        econnect = str(os.environ.get('Mongo_HOST', 'localhost'))
        self.client = pymongo.MongoClient(f'mongodb://{econnect}:27017')
        self.db = self.client['myscrapybook_db']  # Create Database

    def process_item(self, item, spider):
        collection = self.db['books']  # Create Collection or Table
        try:
            collection.insert_one(dict(item))
            return item
        except Exception as e:
            raise DropItem(f"Error inserting item: {e}")

class JsonDBmyscrapybookPipeline:
    def process_item(self, item, spider):
        with open('jsondata_myscrapybook.json', 'a', encoding='utf-8') as file:
            line = json.dumps(dict(item), ensure_ascii=False) + '\n'
            file.write(line)
        return item

class CSVDBmyscrapybookPipeline:
    '''
    Mỗi thông tin cách nhau với dấu $.
    Ví dụ: coursename$tacgia$tietkiem$giahientai$co$tinhtrang$dichgia$nhaxuatban$sotrang$trongluong$noidung_brand
    '''
    def process_item(self, item, spider):
        with open('csvdata_myscrapybook.csv', 'a', encoding='utf-8', newline='') as file:
            writer = csv.writer(file, delimiter='$')
            writer.writerow([
                item.get('coursename', ''),
                item.get('tacgia', ''),
                item.get('tietkiem', ''),
                item.get('giahientai', ''),
                item.get('giathitruong', ''),
                item.get('tinhtrang', ''),
                item.get('dichgia', ''),
                item.get('nhaxuatban', ''),
                item.get('sotrang', ''),
                item.get('trongluong', ''),
                item.get('noidung_brand', ''),
            ])
        return item
