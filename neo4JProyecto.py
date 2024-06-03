import os
from typing import Any
from utils import get_collection, read_config, connect_to_sql
from queries import get_product_types
from pymongo.collection import Collection
import random
from neo4j import GraphDatabase
import random

# QUERY 4.1


def get_most_reviews(collection: Collection, limit: int) -> list[dict]:
    """Busca los n usuarios con más reviews

    Args:
        collection (Collection): collección de MongoDB
        limit (int): El límite de usuarios a devolver

    Returns:
        list: lista de diccionarios de los usuarios con más reviews junto con el número de reviews que tienen cada uno
    """
    # Query en mongosh
    # db.reviews.aggregate([{$group: {_id: "$reviewerID", num_reviews: {$sum: 1}}},{$sort: {"num_reviews": -1}}, {$limit: 30}])
    docs = collection.aggregate(
        [
            {"$group": {"_id": "$reviewerID", "num_reviews": {"$sum": 1}}},
            {"$sort": {"num_reviews": -1}},
            {"$limit": limit},
        ]
    )

    return list(docs)


def get_set_articles(articles: list) -> set[str]:
    """Devuelve un set de los articulos. Junta el asin con el type_id para evitar los duplicados de distintos tipos de objetos

    Args:
        articles (list): Lista de artículos

    Returns:
        set[str]: set de los identificadores de los artículos
    """
    return set(
        "-".join([article["asin"], str(article["type_id"])]) for article in articles
    )


def calculate_jaccard_similarity(
    set_articles_user1: set, set_articles_user2: set
) -> float:
    """Calcula la similitud de Jaccard

    Args:
        set_articles_user1 (set): Set de los identificadores de los artículos del primer usuario
        set_articles_user2 (set): Set de los identificadores de los artículos del segundo usuario

    Returns:
        float: La similitud de Jaccard
    """

    intersection = len(set_articles_user1 & set_articles_user2)
    union = len(set_articles_user1 | set_articles_user2)

    return intersection / union


def insert_to_cache(cache: dict, key: str, data: Any, max_cache_size: int) -> None:
    """Inserta en el caché la información requerida

    Args:
        cache (dict): caché
        key (str): la clave
        data (Any): los datos a guardar
        max_cache_size (int): el tamaño máximo del caché
    """
    if len(cache) < max_cache_size:
        cache[key] = data
    else:
        delete_key = random.choice(cache.keys())
        cache.pop(delete_key)
        cache[key] = data


def add_user_articles_to_cache(
    collection: Collection,
    user_id: str,
    cache: dict,
    max_cache_size: int,
) -> None:
    """Añade los artículos del usuario deseado al caché

    Args:
        collection (Collection): collección de MongoDB
        user_id (str): Id del usuario
        cache (dict): caché
        max_cache_size (int): Tamaño máximo del caché
    """
    # Se buscan los artículos en MongoDB
    user_docs = collection.find({"reviewerID": user_id})
    # Se consigue el set de los artículos
    set_articles_user = get_set_articles(user_docs)
    # Se añaden al caché
    insert_to_cache(cache, user_id, set_articles_user, max_cache_size)


def store_similarity(
    collection: Collection, users: list, similarity_file: str, max_cache_size: int = 30
) -> None:
    """Se guarda la similitud de los usuarios en un fichero

    Args:
        collection (Collection): colección de MongoDB
        users (list): Lista de los usuarios
        similarity_file (str): Fichero donde se quiere guardar las similitudes
        max_cache_size (int, optional): Tamaño máximo del caché. Defaults to 30.
    """

    # Se crea el ficheros en blanco
    if os.path.exists(similarity_file):
        os.remove(similarity_file)
    with open(similarity_file, "w"):
        pass

    # Se inicializa el caché
    cache = {}

    # Se recorren todos los pares de usuarios
    for i, user1 in enumerate(users):

        # Se guarda la información del primer usuario
        id1 = user1["_id"]
        if id1 not in cache:
            add_user_articles_to_cache(collection, id1, cache, max_cache_size)

        for user2 in users[i + 1 :]:

            # Se guarda la información del segundo usuario
            id2 = user2["_id"]
            if id2 not in cache:
                add_user_articles_to_cache(collection, id2, cache, max_cache_size)

            # Se calcula la similitud
            similarity = calculate_jaccard_similarity(cache[id1], cache[id2])

            # Se guarda la similitud en el fichero si es mayor de 0
            if similarity > 0:
                with open(similarity_file, "a", encoding="utf-8") as fh:
                    fh.write(f"{id1} {id2} {similarity}\n")


def upload_to_neo4j(similarity_file: str, driver) -> None:
    """Sube las similitudes a Neo4j

    Args:
        similarity_file (str): Fichero donde se quiere guardar las similitudes
        driver: El driver de la conexión a la Neo4J
    """

    query = """
                MERGE (user1: User {user_id: $user1})
                MERGE (user2: User {user_id: $user2})
                CREATE (user1) - [:SIMILAR_TO {similarity: $similarity}] -> (user2)
                CREATE (user1) <- [:SIMILAR_TO {similarity: $similarity}] - (user2)
            """

    with driver.session() as session:
        with open(similarity_file, "r") as fh:

            # Se recorre el fichero
            for line in fh:
                id1, id2, similarity = line.strip().split(" ")

                # Se ejecuta la query
                session.run(query, user1=id1, user2=id2, similarity=float(similarity))


# QUERY 4.2


def get_product_asins(type_id: int) -> tuple[tuple]:

    connection = connect_to_sql()
    """Devuelve los asins de los productos de un tipo concreto

    Args:
        type_id (int): El tipo de producto

    Returns:
        tuple[tuple]: Los asins de los productos de ese tipo
    """
    sql = """
        SELECT asin
        FROM items
        where type_id = %s
        """

    cursor = connection.cursor()
    cursor.execute(sql, type_id)
    vals = cursor.fetchall()
    cursor.close()
    connection.close()

    return [item[0] for item in vals]


def usuarios_articulos_pedir_datos() -> tuple[list, int]:
    """Obtiene la selección de números aleatorios y el type_id de los productos
    Args:
        connection: conexión a la base de datos
    Returns:
        tupla con la lista de productos seleccionados y el type_id de los productos
    """
    diccionario_categorias = dict(get_product_types())

    print("Elige una categoria: ")
    categoria = " "
    while categoria not in diccionario_categorias.values():
        for i in diccionario_categorias.values():
            print(f"{i}\n")
        categoria = input()

    type_id_variable = int(
        [key for key, value in diccionario_categorias.items() if value == categoria][0]
    )
    numero_de_datos = len(get_product_asins(type_id_variable))

    n = -1
    while not 0 <= n <= numero_de_datos:
        print(f"Numero de datos aleatorios, entre 0 y {numero_de_datos}")
        n = int(input())

    lista_productos = get_product_asins(type_id_variable)
    seleccion = random.sample(lista_productos, k=n)

    return seleccion, type_id_variable


def query_4_2() -> None:
    """Genera el gráfico en neo4j con los usuarios y los artículos que han comprado

    Args:
        connection: conexión a la base de datos

    Returns:
        None (aunque carga datos en la base de datos de Neo4j)
    """

    seleccion, type_id_variable = usuarios_articulos_pedir_datos()

    config = read_config()
    collection = get_collection(config)

    lista_general = []

    for asin in seleccion:

        result = collection.find(
            {"asin": asin, "type_id": type_id_variable},
            {"asin": 1, "reviewerID": 1, "overall": 1, "reviewTime": 1, "_id": 0},
        )
        for res in result:
            lista_general.append(res)
    with open("resultados_reviews.txt", "w") as file:
        # Escribir los encabezados si es necesario
        # Iterar a través de la lista de diccionarios y escribir cada uno en el archivo
        for document in lista_general:
            # Convertir los valores del diccionario en una cadena separada por tabulaciones
            line = f"{document['reviewerID']}/{document['asin']}/{document['overall']}/{document['reviewTime']}\n"
            # Escribir la línea en el archivo
            file.write(line)

    print("Los datos han sido guardados en resultados_reviews.txt")

    class ReviewGraph:
        def __init__(self, uri, user, password):
            self.driver = GraphDatabase.driver(uri, auth=(user, password))

        def close(self) -> None:
            self.driver.close()

        def create_review_relationship(self, reviewer_id, asin, overall, review_time):
            with self.driver.session() as session:
                session.execute_write(
                    self._create_and_link, reviewer_id, asin, overall, review_time
                )

        @staticmethod
        def _create_and_link(tx, reviewer_id, asin, overall, review_time):
            query = (
                "MERGE (reviewer:Reviewer {id: $reviewer_id}) "
                "MERGE (product:Product {id: $asin}) "
                "MERGE (reviewer)-[:REVIEWED {overall: $overall, reviewTime: $review_time}]->(product)"
            )
            tx.run(
                query,
                reviewer_id=reviewer_id,
                asin=asin,
                overall=overall,
                review_time=review_time,
            )

    config = read_config()
    user = config["NEO4J"]["usuario"]
    password = config["NEO4J"]["password"]
    connection = config["NEO4J"]["connection"]
    uri = connection

    driver = GraphDatabase.driver(uri, auth=(user, password))
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")

    graph = ReviewGraph(uri, user, password)

    with open("resultados_reviews.txt", "r") as file:
        for line in file:
            parts = line.strip().split("/")
            if len(parts) == 4:
                reviewer_id, asin, overall, review_time = parts
                graph.create_review_relationship(
                    reviewer_id, asin, overall, review_time
                )

    graph.close()
    print("Grafo creado con éxito")


# QUERY 4.3


def apartado_4_3(connection, collection: Collection, driver) -> None:
    """Carga en Neo4j los usuarios que han escrito a más de dos tipos de productos distintos

    Args:
        connection: conexión a la base de datos
        collection (Collection): colección de MongoDB
        driver: driver de la conexión a Neo4j

    Returns:
        None (aunque carga datos en la base de datos de Neo4j)
    """

    sql = """
            SELECT reviewerID, reviewerName
            FROM reviewers
            ORDER BY reviewerName
            LIMIT 400;
        """

    neo4j_query = """
                MERGE (reviewer: User {user_id: $reviewer_id, reviewer_name: $reviewer_name})
                MERGE (type: ProductType {product_type: $product_type})
                CREATE (reviewer) - [:WROTE {number_of_articles: $count}] -> (type)
                """

    cursor = connection.cursor()
    cursor.execute(sql)
    reviewer_names = cursor.fetchall()

    # Se consigue un diccionario de los ids y nombres de productos
    product_types = dict(get_product_types())

    with driver.session() as session:
        # Se recorren los reviewers
        for id, name in reviewer_names:
            docs = collection.aggregate(
                [
                    {"$match": {"reviewerID": id}},
                    {"$group": {"_id": "$type_id", "count": {"$sum": 1}}},
                ]
            )
            docs = list(docs)

            # Si han escrito a más de dos tipos de productos distintos
            # Se suben las relaciones a Neo4j
            if len(list(docs)) >= 2:

                # Se cambia el nombre si no existe a NameDoesNotExist
                if name is None:
                    name = "NameDoesNotExist"

                for doc in docs:
                    product_type = product_types[doc["_id"]]
                    count = doc["count"]
                    # Se ejecuta la query
                    session.run(
                        neo4j_query,
                        reviewer_id=id,
                        reviewer_name=name,
                        product_type=product_type,
                        count=count,
                    )
    print("Se ha finalizado la carga en Neo4j")


def apartado_4_4() -> None:
    """Obtiene los artículos más populares, bajo un límite de 39 reviews y los artículos en común entre usuarios

    Args:
        collection (Collection): colección de MongoDB

    Returns:
        None (aunque imprime los resultados)
    """
    user = config["NEO4J"]["usuario"]
    password = config["NEO4J"]["password"]
    connection = config["NEO4J"]["connection"]
    uri = connection
    driver = GraphDatabase.driver(uri, auth=(user, password))
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    driver.close()

    def get_top_asin_reviewers() -> None:
        collection = get_collection(config)

        # La pipeline de agregación para obtener los asin
        pipeline = [
            {"$group": {"_id": "$asin", "reviewCount": {"$sum": 1}}},
            {"$match": {"reviewCount": {"$lt": 40}}},
            {"$sort": {"reviewCount": -1}},
            {"$limit": 5},
        ]

        # Obtener los asin con la mayor cantidad de reviews, pero menos de 40
        top_asin_under_40_reviews = list(collection.aggregate(pipeline))
        asins = [asin["_id"] for asin in top_asin_under_40_reviews]

        # Escribir los resultados en un archivo de texto
        with open("resultados_reviews.txt", "w") as f:
            for asin in asins:
                # Obtener los reviewerID para este asin
                reviewers = collection.find({"asin": asin}, {"_id": 0, "reviewerID": 1})
                reviewer_ids = [reviewer["reviewerID"] for reviewer in reviewers]
                # Escribir el asin seguido de los reviewerID asociados en la misma línea
                f.write(f"{asin}/{'/'.join(reviewer_ids)}\n")

    def create_nodes_and_relationships(tx, asin, reviewer_ids):

        driver = GraphDatabase.driver(uri, auth=(user, password))

        # Crear el nodo del artículo si no existe
        tx.run("MERGE (a:Article {asin: $asin})", asin=asin)

        for reviewer_id in reviewer_ids:
            # Crear el nodo del usuario si no existe
            tx.run("MERGE (u:User {reviewerID: $reviewer_id})", reviewer_id=reviewer_id)
            # Crear la relación entre el artículo y el usuario si no existe
            tx.run(
                """
                MATCH (a:Article {asin: $asin}), (u:User {reviewerID: $reviewer_id})
                MERGE (u)-[:REVIEWED]->(a)
                """,
                asin=asin,
                reviewer_id=reviewer_id,
            )
        driver.close()

    # Función para calcular y retornar los enlaces entre los usuarios
    def find_shared_reviews(tx) -> list:
        return list(
            tx.run(
                """
                    MATCH (u1:User)-[:REVIEWED]->(a:Article)<-[:REVIEWED]-(u2:User)
                    WHERE id(u1) < id(u2)
                    WITH u1, u2, COUNT(a) AS sharedReviews
                    RETURN u1.reviewerID AS User1, u2.reviewerID AS User2, sharedReviews
                    ORDER BY sharedReviews DESC
                """
            )
        )

    # Llamar a la función y especificar el archivo de salida
    get_top_asin_reviewers()

    # Conectar con Neo4j
    driver = GraphDatabase.driver(uri, auth=(user, password))

    # Leer el archivo .txt y procesar cada línea
    with driver.session() as session:
        with open("resultados_reviews.txt", "r") as file:
            for line in file:
                parts = line.strip().split("/")
                asin = parts[0]
                reviewer_ids = parts[1:]
                session.execute_write(
                    create_nodes_and_relationships, asin, reviewer_ids
                )

    # Cerrar la conexión con Neo4j
    driver.close()

    # Conectar con Neo4j y ejecutar la consulta
    driver = GraphDatabase.driver(uri, auth=(user, password))
    with driver.session() as session:
        shared_review_data = session.execute_read(find_shared_reviews)
        for record in shared_review_data:
            print(
                f"User {record['User1']} and User {record['User2']} have {record['sharedReviews']} shared reviews."
            )
        if len(shared_review_data) == 0:
            print("No se han encontrado usuarios con reviews en común")
        else:
            print(
                f"Se han encontrado {len(shared_review_data)} parejas de reviewers que tienen reviews en común"
            )
    driver.close()


def borrar_neo4j(driver) -> None:
    """Borra los nodos de la base de datos de Neo4J

    Args:
        driver (Driver): driver de conexión a Neo4J
    """

    with driver.session() as session:
        session.run(
            """
                MATCH (n)
                DETACH DELETE n;
            """
        )


if __name__ == "__main__":
    config = read_config()
    collection = get_collection(config)
    sql_connection = connect_to_sql()

    LIMITE_REVIEWERS = int(config["NEO4J"]["limite_usuarios_reviews"])
    similarity_file = config["NEO4J"]["fichero_similitud"]
    max_cache_size = int(config["NEO4J"]["max_cache_size"])
    users = get_most_reviews(collection, LIMITE_REVIEWERS)
    USUARIO = config["NEO4J"]["usuario"]
    PASSWORD = config["NEO4J"]["password"]
    connection = config["NEO4J"]["connection"]

    opcion_menu = None
    while opcion_menu is None or 1 <= opcion_menu <= 5:
        print()
        print("1. Obtener similitudes entre usuarios y mostrar los enlaces en Neo4J")
        print("2. Obtener enlaces entre usuarios y artículos")
        print(
            "3. Obtener algunos usuarios que han visto más de un determinado tipo de artículo"
        )
        print("4. Artículos populares y artículos en común entre usuarios")
        print("5. Borrar datos de Neo4J")
        print("Introduzca otro numero para salir.")
        opcion_menu = int(input("Inserte el número de la opción deseada: "))

        if opcion_menu == 1:

            driver = GraphDatabase.driver(connection, auth=(USUARIO, PASSWORD))
            store_similarity(collection, users, similarity_file, max_cache_size)
            upload_to_neo4j(similarity_file, driver)

        if opcion_menu == 2:
            query_4_2()

        if opcion_menu == 3:
            USUARIO = config["NEO4J"]["usuario"]
            PASSWORD = config["NEO4J"]["password"]
            connection = config["NEO4J"]["connection"]

            driver = GraphDatabase.driver(connection, auth=(USUARIO, PASSWORD))
            apartado_4_3(sql_connection, collection, driver)

        if opcion_menu == 4:
            apartado_4_4()

        if opcion_menu == 5:
            driver = GraphDatabase.driver(connection, auth=(USUARIO, PASSWORD))
            borrar_neo4j(driver)
