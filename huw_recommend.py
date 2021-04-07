from flask import Flask, request, session, render_template, redirect, url_for, g
from flask_restful import Api, Resource, reqparse
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import psycopg2
import random

app = Flask(__name__)
api = Api(app)

postgresConnection = psycopg2.connect(user="postgres",
                                      password="root",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="huwebshop")
c = postgresConnection.cursor()

# We define these variables to (optionally) connect to an external MongoDB
# instance.
envvals = ["MONGODBUSER", "MONGODBPASSWORD", "MONGODBSERVER"]
dbstring = 'mongodb+srv://{0}:{1}@{2}/test?retryWrites=true&w=majority'

# Since we are asked to pass a class rather than an instance of the class to the
# add_resource method, we open the connection to the database outside of the
# Recom class.
load_dotenv()
if os.getenv(envvals[0]) is not None:
    envvals = list(map(lambda x: str(os.getenv(x)), envvals))
    client = MongoClient(dbstring.format(*envvals))
else:
    client = MongoClient()
database = client.huwebshop


class Recom(Resource):
    """ This class represents the REST API that provides the recommendations for
    the webshop. At the moment, the API simply returns a random set of products
    to recommend."""

    def get(self, profileid, count, recom_type, productid):
        """ This function represents the handler for GET requests coming in
        through the API. It currently returns a random sample of products. """
        # randcursor = database.products.aggregate([{ '$sample': { 'size': count } }])
        # prodids = list(map(lambda x: x['_id'], list(randcursor)))
        # return prodids, 200
        tel = 0
        prodids = []
        while tel != count:
            if recom_type == 0:
                queryc = "SELECT category FROM products WHERE product_id LIKE CAST(" + productid + " AS varchar)"
                c.execute(queryc)
                category = c.fetchone()
                queryg = "SELECT gender FROM products WHERE product_id LIKE CAST(" + productid + " AS varchar)"
                c.execute(queryg)
                gender = c.fetchone()
                queryp = """SELECT product_id FROM products 
                                                WHERE category LIKE '""" + category[0] + """' 
                                                AND product_id NOT LIKE CAST(""" + productid + """ AS varchar)
                                                AND gender LIKE '""" + gender[0] + """'
                                                ORDER BY random() LIMIT 1"""
                c.execute(queryp)
                product = c.fetchone()
                tel += 1
                prodids += product

            if recom_type == 1:
                querypop = """SELECT (products.product_id) FROM products
                                                    INNER JOIN pop_products on products.product_id = pop_products.product_id
                                                    ORDER BY pop_products.freq DESC
                                                    LIMIT 10;"""

                queryprice = """SELECT product_id, discount FROM products
                                                     WHERE discount IS NOT NULL
                                                     ORDER BY discount DESC
                                                     limit 10;"""
                c.execute(querypop)
                products_best_seller_raw = c.fetchall()
                products_best_seller_id = [i[0] for i in products_best_seller_raw]
                products_best_seller_random = random.sample(products_best_seller_id, 5)

                c.execute(queryprice)
                products_best_price_raw = c.fetchall()
                products_best_price_id = [i[0] for i in products_best_price_raw]
                products_best_price_random = random.sample(products_best_price_id, 5)

                products_combined = random.sample(products_best_seller_random + products_best_price_random, 1)
                tel += 1
                prodids += products_combined

        return prodids


# This method binds the Recom class to the REST API, to parse specifically
# requests in the format described below.
api.add_resource(Recom, "/<string:profileid>/<int:count>/<string:productid>/<int:recom_type>")
