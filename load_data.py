import configparser
import pymysql
import json
import datetime
import os
from pymongo.collection import Collection
from neo4JProyecto import get_product_types
from utils import read_config, get_collection, connect_to_sql
from queries import obtener_tuplas_items, Reviewer_Diferentes


def create_sql_tables(connection) -> None:
    """
    Crea las tablas de mySQL

    Args:
        connection: Conexión a mySQL
    """

    reviewers_table = """CREATE TABLE IF NOT EXISTS reviewers (
                    reviewerID VARCHAR(50),
                    reviewerName VARCHAR(50) NULL,
                    PRIMARY KEY (reviewerID)
                )"""
    types_table = """CREATE TABLE IF NOT EXISTS types (
            id INT,
            type VARCHAR(50),
            PRIMARY KEY (id)
        )"""

    items_table = """CREATE TABLE IF NOT EXISTS items (
            asin CHAR(10),
            type_id INT,
            PRIMARY KEY (asin, type_id),
            FOREIGN KEY (type_id) REFERENCES types(id)
        )"""

    # Create the sql tables
    tables_sql = [reviewers_table, types_table, items_table]
    cursor = connection.cursor()
    for table in tables_sql:
        cursor.execute(table)

    cursor.close()


def parse_json(line: dict, type_id: int) -> tuple[dict, dict, dict[str, int]]:
    """Crea los diccionarios que serán subidos a las bases de datos

    Args:
        line (dict): el diccionario de la línea que se está leyendo
        type_id (int): el id del tipo de producto

    Returns:
        tuple: tupla con los diccionarios creados
    """

    # Atributos que van a cada tabla
    mongo_atts = {"reviewerID", "asin", "helpful", "summary", "reviewText", "overall"}
    reviewers_atts = {"reviewerID", "reviewerName"}
    items_atts = {"asin"}

    items_table = {"type_id": type_id}
    reviewers_table = {}
    mongo_document = {}

    # Se recorren los atributos de la línea
    for k, v in line.items():
        if k in items_atts:
            items_table[k] = v
        elif k in reviewers_atts:
            reviewers_table[k] = v
        if k in mongo_atts:
            mongo_document[k] = v
        elif k == "unixReviewTime":
            mongo_document["reviewTime"] = datetime.datetime.fromtimestamp(v)
    mongo_document["type_id"] = type_id
    return mongo_document, reviewers_table, items_table


def parse_document_name(doc: str) -> str:
    """Quita _5.json a los nombres de los documentos

    Args:
        doc (str): nombre del documento

    Returns:
        str: nombre limpiado
    """
    doc = doc.replace("_5.json", "")
    return doc


def upload_to_mongo(doc: dict, collection: Collection) -> None:
    """Sube a la colección de mongoDB los datos

    Args:
        doc (dict): documento que se quiere subir
        collection (Collection): Colección de mongodb
    """
    collection.insert_one(doc)


def upload_to_sql(doc: dict, table: str, cursor) -> None:
    """
    Sube los datos a mySQL

    Args:
        doc (dict): diccionario con los valores que se quieren subir
        table (str): nombre de la tabla a la que subir los valores
        cursor: cursor de la conexión a mySQL
    """

    sql, vals = dict(
        reviewers=(
            """
                    INSERT INTO reviewers (reviewerID, reviewerName) VALUES (%s, %s)
                """,
            ("reviewerID", "reviewerName"),
        ),
        types=(
            """
                    INSERT INTO types (id, type) VALUES (%s, %s)
                """,
            ("id", "type"),
        ),
        items=(
            """
                    INSERT INTO items (asin, type_id) VALUES (%s, %s)
                """,
            ("asin", "type_id"),
        ),
    )[table]

    values = [doc.get(v, None) for v in vals]
    cursor.execute(sql, values)


def Type_IDs_Diferentes() -> set:
    """
    Consigue los valores únicos de los tipos de productos

    Returns:
        (set): set de los valores únicos de los tipos de productos
    """

    table = """SELECT distinct(id)
                FROM types"""
    cursor = connect_to_sql().cursor()
    cursor.execute(table)
    return set(*zip(*cursor.fetchall()))


def Nombres_Documentos_Ids() -> tuple:
    table = """SELECT *
                FROM types"""
    cursor = connect_to_sql().cursor()
    cursor.execute(table)
    return cursor.fetchall()


def obtener_clave_por_valor(diccionario, valor_buscado):
    for clave, valor in diccionario.items():
        if valor == valor_buscado:
            return clave
    return None


def insert_to_database(
    connection, collection: Collection, config: configparser.ConfigParser
) -> None:
    """Inserta todos los datos a las bases de datos

    Args:
        connection (connection): conexión a mySQL
        collection (Collection): colección de mongoDB
        config (ConfigParser): Valores de configuracion.ini
    """
    cursor = connection.cursor()

    # Se consiguen todos los valores existentes en la base de datos
    # types_ids = dict(Nombres_Documentos_Ids())
    types_ids = dict(get_product_types())

    items_id = set(obtener_tuplas_items())

    reviewers_id = Reviewer_Diferentes()

    # Para guardar los que no se han metido porque les falta el nombre
    # pero que luego aparece su nombre
    reviewers_not_added = dict()

    # Se recorre el directorio guardado en configuracion.ini
    for document in os.listdir(config["DATA_UPLOAD"]["path"]):

        # Se consigue el nombre del tipo de productos contenidos en el documento
        type_name = parse_document_name(document)

        print("Loading", type_name)

        # Si el tipo de producto no existe en la base de datos, se añade
        if type_name not in types_ids.values():
            type_id = len(types_ids)
            types_ids[type_name] = type_id
            upload_to_sql({"id": type_id, "type": type_name}, "types", cursor)

        else:
            # Si existe, se obtiene el type_id
            type_id = obtener_clave_por_valor(types_ids, type_name)

        # Se recorre el documento
        document_path = os.path.join(config["DATA_UPLOAD"]["path"], document)
        with open(document_path, "r", encoding="utf-8") as fh:
            for line in fh:

                # se obtiene el diccionario de la línea
                line_json = json.loads(line)

                # Se obtienen los diccionarios necesarios para subirlos a las bases de datos
                mongo_document, reviewers_table, items_table = parse_json(
                    line_json, type_id
                )

                # Se sube a mongoDB
                upload_to_mongo(mongo_document, collection)

                # Si el reviewerName existe y no está ya en la base de datos, se sube a mySQL
                if (
                    reviewers_table.get("reviewerName", None) is not None
                    and reviewers_table["reviewerID"] not in reviewers_id
                ):
                    # Se sube el reviewerID y reviewerName
                    upload_to_sql(reviewers_table, "reviewers", cursor)
                    reviewer_id = reviewers_table["reviewerID"]
                    # Se indica que ya se ha añadido
                    if reviewer_id in reviewers_not_added:
                        reviewers_not_added.pop(reviewer_id)
                    reviewers_id.add(reviewer_id)

                # Si no se ha subido pero su nombre es None, se guarda para subirlo más adelante,
                # por si acaso aparece más adelante con el nombre puesto
                elif reviewers_table["reviewerID"] not in reviewers_id:
                    reviewers_not_added[reviewers_table["reviewerID"]] = reviewers_table

                # Si el producto es nuevo, se sube a la base de datos de mySQL
                if (items_table["asin"], type_id) not in items_id:
                    items_table["type_id"] = type_id

                    # Se sube a mySQL
                    upload_to_sql(items_table, "items", cursor)

                    # Se indica que ya ha sido añadido
                    items_id.add((items_table["asin"], type_id))

    # Se añaden todos los reviewers que no tienen nombre
    if len(reviewers_not_added) > 0:
        print(f"No se ha encontrado el nombre de {len(reviewers_not_added)} reviewers.")
        for id in reviewers_not_added:
            upload_to_sql(reviewers_not_added[id], "reviewers", cursor)

    cursor.close()
    connection.commit()


def drop_database_sql(config) -> None:
    """Elimina la base de datos de mySQL

    Args:
        config (ConfigParser): configparser de configuracion.ini
    """
    connection = pymysql.connect(
        host=config["SQL"]["host"],
        user=config["SQL"]["user"],
        password=config["SQL"]["password"],
    )
    with connection:
        cursor = connection.cursor()
        cursor.execute(
            "DROP DATABASE IF EXISTS %s;" % (config["SQL"]["database"],),
        )
        cursor.close()


def create_database_sql(config):
    """Crea la base de datos de mySQL

    Args:
        config (ConfigParser): configparser de configuracion.ini

    Returns:
        connection: conexión a la base de datos de mySQL
    """
    connection = pymysql.connect(
        host=config["SQL"]["host"],
        user=config["SQL"]["user"],
        password=config["SQL"]["password"],
    )
    with connection:
        cursor = connection.cursor()
        cursor.execute(
            "CREATE DATABASE IF NOT EXISTS %s;" % (config["SQL"]["database"],),
        )
        cursor.close()

    connection = pymysql.connect(
        host=config["SQL"]["host"],
        user=config["SQL"]["user"],
        password=config["SQL"]["password"],
        database=config["SQL"]["database"],
    )
    return connection


if __name__ == "__main__":
    config = read_config()

    collection = get_collection(config)

    if config["DATA_UPLOAD"].getboolean("create_new_db"):
        collection.drop()
        drop_database_sql(config)

    connection = create_database_sql(config)

    create_sql_tables(connection)

    insert_to_database(connection, collection, config)

    print("Done!")
