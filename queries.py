from utils import connect_to_sql, read_config, get_collection
import string
from collections import Counter


def count_words(word_list: list) -> Counter:
    """Genera una cuenta de las palabras en una lista de palabras

    Args:
        lsita (list): lista de palabras
    Returns:
        list: lista de diccionarios de los usuarios con más reviews junto con el número de reviews que tienen cada uno
    """
    # Use Counter to count how many times each word appears in the list
    word_count = Counter(word_list)
    return word_count


def clean_word(word: str):
    """Limpia una palabra de signos de puntuación
    Args:
        word (str): palabra a limpiar
    Returns:
        str: palabra sin signos de puntuación"""
    return word.strip(string.punctuation)


config = read_config()
collection = get_collection(config)


"""tipo_review puede ser 0,1,2,etc... o Todo"""


def get_product_types() -> tuple[tuple]:
    """Limpia una palabra de signos de puntuación
    Args:
        word (str): palabra a limpiar
    Returns:
        str: palabra sin signos de puntuación"""
    connection = connect_to_sql()
    sql = """
            SELECT id, type
            FROM types;
        """

    cursor = connection.cursor()
    cursor.execute(sql)
    vals = cursor.fetchall()
    cursor.close()
    connection.close()
    return vals


def obtener_tuplas_items() -> tuple:
    table = """SELECT asin, type_id
                FROM items"""
    cursor = connect_to_sql().cursor()
    cursor.execute(table)
    return cursor.fetchall()


def Reviewer_Diferentes() -> set:
    """
    Consigue los valores únicos de reviewerID

    Returns:
        (set): set de los valores únicos de reviewerID
    """

    table = """SELECT distinct(reviewerID)
                FROM reviewers"""
    cursor = connect_to_sql().cursor()
    cursor.execute(table)
    return set(*zip(*cursor.fetchall()))


def get_product_asin_type() -> tuple[tuple]:
    """Limpia una palabra de signos de puntuación
    Args:
        None
    Returns:
        tuple[tuple]: lista de tuplas con el asin, tipo y tipo_id de cada producto"""
    connection = connect_to_sql()
    sql = """
            SELECT asin, type, type_id
            FROM types INNER JOIN items ON items.type_id = id;
        """

    cursor = connection.cursor()
    cursor.execute(sql)
    vals = cursor.fetchall()
    cursor.close()
    return vals


def Query_1_Evolucion_Reviews_Por_Año(tipo_review: str) -> list:
    """Devuelve el número de reviews por categoria y año
    Args:
        tipo_review (str): tipo de review a buscar
    Returns:
        list: lista de diccionarios con el año y el número de reviews de ese año"""
    if tipo_review != "Todo":
        pipeline = [
            {"$match": {"type_id": tipo_review}},
            {"$group": {"_id": {"$year": "$reviewTime"}, "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}},
        ]

    else:
        pipeline = [
            {"$group": {"_id": {"$year": "$reviewTime"}, "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}},
        ]

    result = collection.aggregate(pipeline)

    return list(result)


# Query_1_Evolucion_Reviews_Por_Año(0)


def Query_2_Evolucion_Popularidad_Articulos(tipo_review):
    """Devuelve el número de reviews por año
    Args:
        tipo_review (str): tipo de review a buscar
    Returns:
        list: lista de diccionarios con el año y el número de reviews de ese año"""

    if tipo_review != "Todo":
        pipeline = [
            {"$match": {"type_id": tipo_review}},
            {"$group": {"_id": "$asin", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ]
    else:
        pipeline = [
            {"$group": {"_id": "$asin", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ]
    result = collection.aggregate(pipeline)

    return list(result)


Query_2_Evolucion_Popularidad_Articulos(0)


def Query_3_Histograma_Por_Nota(asin=None, type_id=None):
    """Devuelve el número de reviews por nota
    Args:
        asin (str): asin del producto a buscar
        type_id (str): tipo de review a buscar
    Returns:
        list: lista de diccionarios con la nota y el número de reviews de esa nota"""
    if asin is None or asin == "Todo":
        pipeline = [
            {"$group": {"_id": "$overall", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}},
        ]

    else:
        pipeline = [
            {"$match": {"asin": asin, "type_id": type_id}},
            {"$group": {"_id": "$overall", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}},
        ]

    result = collection.aggregate(pipeline)

    if result == None:
        print("No se encontraron datos")
        return []

    return list(result)


# Query_3_Histograma_Por_Nota()
# Query_3_Histograma_Por_Nota("5555991584")


def Query_4_Evolucion_Reviews_Tiempo_Todas_Categorias() -> list:
    """Muestra la evolución de las reviews a lo largo del tiempo
    Args:
        None
    Returns:
        list: lista de diccionarios con la fecha y el número de reviews de ese día"""
    pipeline = [
        {
            "$group": {
                "_id": {
                    "year": {"$year": "$reviewTime"},
                    "month": {"$month": "$reviewTime"},
                    "day": {"$dayOfMonth": "$reviewTime"},
                },
                "count": {"$sum": 1},
            }
        },
        {"$sort": {"_id": 1}},
    ]

    # Ejecutar la operación de agregación
    result = list(collection.aggregate(pipeline))

    # Imprimir los resultados
    suma = 0
    for doc in result:
        count_dia = doc["count"]
        doc["count"] += suma
        suma += count_dia
        date = doc["_id"]
        doc["fecha"] = f"{date['year']}-{date['month']:02d}-{date['day']:02d}"
        # print(
        #     f"Fecha: {date['year']}-{date['month']:02d}-{date['day']:02d}, Número de reseñas: {doc['count']}"
        # )

    return result


# Query_4_Evolucion_Reviews_Tiempo_Todas_Categorias()


def Query_5_Reviews_Por_Usuario() -> list:
    """Devuelve el número de reviews por usuario
    Args:
        None
    Returns:
        list: lista de diccionarios de los usuarios con más reviews junto con el número de reviews que tienen cada uno
    """
    pipeline = [
        {"$group": {"_id": "$reviewerID", "count": {"$sum": 1}}},
        {"$group": {"_id": "$count", "number_of_users": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]

    result = collection.aggregate(pipeline)

    return list(result)


# Query_5_Reviews_Por_Usuario()


def Query_6_Nube_Palabras_Por_Categoria(tipo_review: str) -> Counter:
    """Devuelve el numero de palabras en los resúmenes de las reviews
    Args:
        tipo_review (str): tipo de review a buscar
    Returns:
        Counter: contador de palabras en los resúmenes de las reviews"""
    documentos = collection.find({"type_id": tipo_review})

    # Concatenar resúmenes
    resumenes = " ".join(doc["summary"] for doc in documentos)

    # Eliminar conectores y palabras cortas
    palabras = [
        clean_word(palabra.lower()) for palabra in resumenes.split() if len(palabra) > 3
    ]
    frequencia_palabras = count_words(palabras)
    # for palabra, frequencia in frequencia_palabras.items():
    #     print(f"{palabra}, {frequencia}")
    return frequencia_palabras


# Query_6_Nube_Palabras_Por_Categoria(0)


def Query_7_Libre_Reviewers_Generosos() -> list:
    """La nota media que un reviewer pone a las cosas que valora
    Args:
        None
    Returns:
        list: lista de diccionarios con el reviewer y la nota media que pone a las cosas que valora
    """

    pipeline = [
        {"$group": {"_id": "$reviewerID", "averageRating": {"$avg": "$overall"}}},
        {"$sort": {"averageRating": -1}},
    ]

    result = collection.aggregate(pipeline)

    return list(result)


# Query_7_Libre_Reviewers_Generosos()
