import csv
import pymongo

'''Verbinden van pymongo en psycopg2 met python'''
myclient =  pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["huwebshop"]
print(myclient.list_database_names())

def csv_file_write():
    print('generating the contents for the db')

    def inhoud_producten():
        '''De inhoud van de producten opvragen'''
        with open (products.csv, 'w',)

