import math
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from utils import read_config, get_collection, connect_to_sql
from queries import obtener_tuplas_items
from neo4j import GraphDatabase
import random
from scipy.spatial.distance import cdist
import seaborn as sns

import warnings

warnings.filterwarnings("ignore")

n_neighbors = 20


class KNN:
    def __init__(self, k=10) -> None:
        self.k = k

    def fit(self, X, similarity_matrix, reviewers) -> None:
        self.similarity_matrix = similarity_matrix
        self.reviewers = reviewers
        self.idx_to_ids = {v: k for k, v in reviewers.items()}
        self.X = X

    def get_k_nearest_neighbors(self, idx) -> None:
        # Se cogen los indices de los que tienen similitud mayor que 0
        similarity_indexes = np.where(self.similarity_matrix[idx] > 0)[0]

        # Se miden la distancias a los puntos con similitud (si se da un rating similar, serán más cercanos)
        distances = cdist([self.X[idx]], self.X[similarity_indexes])

        # Se cogen los k vecinos más cercanos
        nearest_indexes = np.argsort(distances[0])[: self.k]

        # Se devuelve por orden los índices de los reviewers
        return similarity_indexes[nearest_indexes]

    def predict(self, reviewer_ratings_matrix):
        n = len(reviewer_ratings_matrix)
        Y = np.copy(reviewer_ratings_matrix)
        for reviewer_idx in range(0, n):

            k_nearest_neighbors_indexes = self.get_k_nearest_neighbors(reviewer_idx)
            # Se calcula la media de los ratings de los vecinos
            prediction = np.nanmean(self.X[k_nearest_neighbors_indexes], axis=0)
            Y[reviewer_idx, np.isnan(Y[reviewer_idx])] = prediction[
                np.isnan(Y[reviewer_idx])
            ]
        return np.array(Y)


if __name__ == "__main__":
    config = read_config()
    connection_sql = connect_to_sql()

    USUARIO = config["NEO4J"]["usuario"]
    PASSWORD = config["NEO4J"]["password"]
    connection = config["NEO4J"]["connection"]

    driver = GraphDatabase.driver(connection, auth=(USUARIO, PASSWORD))

    # Se obtienen todos los productos que existen de mySQL
    items = {item: i for i, item in enumerate(obtener_tuplas_items())}
    n_items = len(items)

    # Se obtienen todos los reviewers de Neo4J
    with driver.session() as session:
        reviewers = session.run("MATCH (n:User) RETURN n").data()
    # Se crea un diccionario de reviewer a un indice que servirá como entrada
    # en la matriz de similitudes
    reviewers = {n["n"]["user_id"]: i for i, n in enumerate(reviewers)}
    n_reviewers = len(reviewers)

    print(f"Número de reviewers obtenidos: {n_reviewers}")
    print(f"Número de productos: {n_items}")

    # Se obtienen las similitude de Neo4J (subidas en el apartado 4.1)
    get_similarities_query = """MATCH (u1:User) - [similarity:SIMILAR_TO] - (u2:User)
                            RETURN u1.user_id as user1, u2.user_id as user2, similarity.similarity as similarity"""

    with driver.session() as session:
        result = session.run(get_similarities_query)
        user_similarities = result.data()

    # Se crea la matriz de similitud enteramente de ceros
    similarity_matrix = np.zeros((n_reviewers, n_reviewers))
    for similarity_dict in user_similarities:
        # Se añade la similitud a la entrada de la matriz correspondiente
        u1 = similarity_dict["user1"]
        u2 = similarity_dict["user2"]
        s = similarity_dict["similarity"]
        i = reviewers[u1]
        j = reviewers[u2]
        similarity_matrix[i, j] = s
        similarity_matrix[j, i] = s

    # Se crea la matriz de los ratings llena de nans
    X = np.empty((n_reviewers, n_items))
    X[:] = np.nan

    # Se obtienen todas las reviews de mongoDB
    collection = get_collection(config)
    for doc in collection.find({}):
        reviewer = doc["reviewerID"]
        rating = doc["overall"]

        asin = doc["asin"]
        type = doc["type_id"]
        item = (asin, type)

        # Se añade el rating a la matriz
        i = reviewers.get(reviewer)
        j = items[item]
        if i is not None:
            X[i, j] = rating

    # Se cambian a None unos índices para tener un train y test set
    not_nan_idxs = np.where(~np.isnan(X))
    num_mask = int(0.1 * len(not_nan_idxs[0]))
    print(f"Se van a tapar {num_mask} ratings")

    # Para reproducibilidad
    random.seed(33)

    # Se escogen aleatoriamente valores que poner a nan
    choices = random.choices(list(zip(*not_nan_idxs)), k=num_mask)
    mask_ids = tuple(zip(*choices))

    # Se crea el set de train tapando ciertos ratings
    X_train = X.copy()
    X_train[mask_ids] = np.nan

    # Se inicializa y entrena el modelo
    knn = KNN(n_neighbors)
    knn.fit(X_train, similarity_matrix, reviewers)

    # Se completa la matriz
    X_new = knn.predict(X_train)

    # Se muestran las gráficas de las diferencias de las matrices
    plt.figure()
    sns.heatmap(X, cmap="coolwarm")
    plt.title("X")
    plt.figure()
    sns.heatmap(X_train, cmap="coolwarm")
    plt.title("X_train")

    plt.figure()
    sns.heatmap(X_new, cmap="coolwarm")
    plt.title("X_new")

    numero_de_productos_no_imputados = len(np.where(np.isnan(X_new[mask_ids]))[0])

    print(
        f"Porcentaje de no imputados: {numero_de_productos_no_imputados / len(mask_ids[0]):.02%}"
    )
    print(
        f"MAE: {(np.nansum(np.abs(X[mask_ids] - X_new[mask_ids])))/ numero_de_productos_no_imputados:.03f}"
    )
    plt.show()
