import configparser
from pymongo import MongoClient
from pymongo.collection import Collection
import pymysql
from pymysql.cursors import Cursor


def get_collection(config) -> Collection:
    client = MongoClient(config["MONGODB"]["connection"])
    db = client[config["MONGODB"]["database"]]
    collection = db[config["MONGODB"]["collection"]]

    return collection


def read_config() -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read("configuracion.ini")
    return config


def connect_to_sql() -> pymysql.Connection:
    config = read_config()
    connection = pymysql.connect(
        host=config["SQL"]["host"],
        user=config["SQL"]["user"],
        password=config["SQL"]["password"],
        database=config["SQL"]["database"],
    )
    return connection
