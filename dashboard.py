import os
import signal
from threading import Thread
import time
import wordcloud
from dash import Dash, html, dcc, Input, Output, State
import plotly.express as px
from queries import (
    Query_1_Evolucion_Reviews_Por_Año,
    Query_2_Evolucion_Popularidad_Articulos,
    Query_3_Histograma_Por_Nota,
    Query_4_Evolucion_Reviews_Tiempo_Todas_Categorias,
    Query_5_Reviews_Por_Usuario,
    Query_6_Nube_Palabras_Por_Categoria,
    Query_7_Libre_Reviewers_Generosos,
    get_product_asin_type,
    get_product_types,
)
import pandas as pd


tabs_styles = {"height": "44px"}
tab_style = {
    "borderBottom": "1px solid #d6d6d6",
    "padding": "6px",
    "fontWeight": "bold",
}
tab_selected_style = {
    "borderTop": "1px solid #d6d6d6",
    "borderBottom": "1px solid #d6d6d6",
    "backgroundColor": "#119DFF",
    "color": "white",
    "padding": "6px",
}
app = Dash(__name__)
app.title = "Dashboard BBDD"
TABS = [
    "Evolución de reviews por años",
    "Popularidad de los artículos",
    "Histograma por nota",
    "Evolución de las reviews",
    "Reviews por usuario",
    "Nube de palabras",
    "Nota media reviewers ",
]

max_slider = 40
step_slider = 1

categories = {"Todo": "Todo"}
categories_without_todo = {}
for id, name in get_product_types():
    name_spaced = name.replace("_", " ")
    categories[name_spaced] = id
    categories_without_todo[name_spaced] = id


tab_1_content = html.Div(
    [
        dcc.Dropdown(
            id="dropdown_reviews_por_year",
            options=[{"label": k, "value": v} for k, v in categories.items()],
            value="Todo",
        ),
        dcc.Graph(id="reviews_por_year"),
    ]
)


@app.callback(
    Output(component_id="reviews_por_year", component_property="figure"),
    [
        Input(component_id="dropdown_reviews_por_year", component_property="value"),
        State(component_id="dropdown_reviews_por_year", component_property="options"),
    ],
)
def update_reviews_por_year(tipo_review, labels):
    result = Query_1_Evolucion_Reviews_Por_Año(tipo_review)
    result_df = pd.DataFrame(result)
    label = "Todo"
    for label_dict in labels:
        if label_dict["value"] == tipo_review:
            label = label_dict["label"]

    graph = px.bar(
        x=result_df["_id"],
        y=result_df["count"],
        labels={"x": "Año", "y": "Número de reviews"},
        title=label,
    )
    return graph


tab_1 = dcc.Tab(
    tab_1_content,
    value="tab_1",
    label=TABS[0],
    style=tab_style,
    selected_style=tab_selected_style,
)

tab_2_content = html.Div(
    [
        dcc.Dropdown(
            id="dropdown_popularidad_por_year",
            options=[{"label": k, "value": v} for k, v in categories.items()],
            value="Todo",
        ),
        dcc.Graph(id="popularidad_por_year"),
    ]
)
tab_2 = dcc.Tab(
    tab_2_content,
    value="tab_2",
    label=TABS[1],
    style=tab_style,
    selected_style=tab_selected_style,
)


@app.callback(
    Output(component_id="popularidad_por_year", component_property="figure"),
    [
        Input(component_id="dropdown_popularidad_por_year", component_property="value"),
        State(
            component_id="dropdown_popularidad_por_year", component_property="options"
        ),
    ],
)
def update_popularidad_por_year(tipo_review, labels):
    result = Query_2_Evolucion_Popularidad_Articulos(tipo_review)
    result_df = pd.DataFrame(result)
    label = "Todo"
    for label_dict in labels:
        if label_dict["value"] == tipo_review:
            label = label_dict["label"]

    graph = px.line(
        x=[i for i in range(len(result_df["_id"]))],
        y=result_df["count"],
        labels={"x": "Artículos", "y": "Número de reviews"},
        title=label,
    )
    return graph


asin_types = [(" ".join([a[0], a[1]]), a[2]) for a in get_product_asin_type()]
dropdown3_options = [
    {"label": k, "value": " ".join((k, str(v)))} for k, v in asin_types
]
dropdown3_options.append({"label": "Todo", "value": "Todo"})
tab_3_content = html.Div(
    [
        dcc.Dropdown(
            id="dropdown_reviews_por_nota", options=dropdown3_options, value="Todo"
        ),
        dcc.Graph(id="reviews_por_nota"),
    ]
)
tab_3 = dcc.Tab(
    tab_3_content,
    value="tab_3",
    label=TABS[2],
    style=tab_style,
    selected_style=tab_selected_style,
)


@app.callback(
    Output(component_id="reviews_por_nota", component_property="figure"),
    [
        Input(component_id="dropdown_reviews_por_nota", component_property="value"),
        State(component_id="dropdown_reviews_por_nota", component_property="options"),
    ],
)
def update_reviews_por_nota(asin_type, labels):

    dict_notas = {str(i): 0 for i in range(1, 5)}

    if asin_type == "Todo":
        result = Query_3_Histograma_Por_Nota()
        label = "Todo"
    else:
        asin_split = asin_type.split(" ")
        if len(asin_split) != 3:
            return None
        asin, type_name, type_id = asin_split
        # for label_dict in labels:
        #     if label_dict["value"] == " ".join(asin_type[0], asin_type[1]):
        #         label = label_dict["label"]
        #         type_id =
        label = " ".join((asin, type_name))
        result = Query_3_Histograma_Por_Nota(asin, int(type_id))

    dict_final = []
    for doc in result:
        dict_notas[str(int(doc["_id"]))] = doc["count"]

    result_df = pd.DataFrame([{"_id": k, "count": v} for k, v in dict_notas.items()])

    graph = px.bar(
        x=result_df["_id"],
        y=result_df["count"],
        labels={"x": "Artículos", "y": "Número de reviews"},
        title=label,
    )
    return graph


def get_evolucion_reviews_tiempo():
    result = Query_4_Evolucion_Reviews_Tiempo_Todas_Categorias()
    result_df = pd.DataFrame(result)

    graph = px.line(
        x=result_df["fecha"],
        y=result_df["count"],
        labels={"x": "Tiempo", "y": "Número de reviews totales"},
        title="Evolución de las reviews",
    )
    return graph


tab_4_content = html.Div(
    [
        dcc.Graph(
            id="evolucion_reviews_totales", figure=get_evolucion_reviews_tiempo()
        ),
    ]
)
tab_4 = dcc.Tab(
    tab_4_content,
    value="tab_4",
    label=TABS[3],
    style=tab_style,
    selected_style=tab_selected_style,
)


def get_reviews_por_usuario():
    result = Query_5_Reviews_Por_Usuario()
    result_df = pd.DataFrame(result)

    graph = px.bar(
        x=result_df["_id"],
        y=result_df["number_of_users"],
        labels={"x": "Número de reviews", "y": "Número de reviewers"},
        title="Reviews por usuario",
    )
    return graph


tab_5_content = html.Div(
    [dcc.Graph(id="reviews_por_usuario", figure=get_reviews_por_usuario())]
)


tab_5 = dcc.Tab(
    tab_5_content,
    value="tab_5",
    label=TABS[4],
    style=tab_style,
    selected_style=tab_selected_style,
)


def get_words():
    Query_6_Nube_Palabras_Por_Categoria()


tab_6_content = html.Div(
    [
        dcc.Dropdown(
            id="dropdown_wordcloud",
            options=[
                {"label": k, "value": v} for k, v in categories_without_todo.items()
            ],
            value=list(categories_without_todo.values())[0],
        ),
        html.Div(
            id="wordcloud",
            style={
                "width": "100%",
                "height": "100%",
                "display": "flex",
                "justifyContent": "center",
                "marginTop": "100px",
            },
        ),
    ]
)


@app.callback(
    Output(component_id="wordcloud", component_property="children"),
    Input(component_id="dropdown_wordcloud", component_property="value"),
)
def update_nube(tipo_review):
    result = Query_6_Nube_Palabras_Por_Categoria(tipo_review)

    wc = wordcloud.WordCloud(background_color="white", width=800, height=400)
    wc.fit_words(result)

    return html.Img(src=wc.to_image())


tab_6 = dcc.Tab(
    tab_6_content,
    value="tab_6",
    label=TABS[5],
    style=tab_style,
    selected_style=tab_selected_style,
)


def get_notas_medias():
    result = Query_7_Libre_Reviewers_Generosos()
    result_df = pd.DataFrame(result)
    return px.line(
        x=range(len(result_df["_id"])),
        y=result_df["averageRating"],
        labels={"x": "Reviewers", "y": "Nota media"},
        title="Nota media de reviewers",
    )


tab_7_content = html.Div([dcc.Graph(id="nota_media", figure=get_notas_medias())])


tab_7 = dcc.Tab(
    tab_7_content,
    value="tab_7",
    label=TABS[6],
    style=tab_style,
    selected_style=tab_selected_style,
)

tab_8_content = html.Div(
    [html.Button("Exit", id="button", n_clicks=0), html.H1("", id="text")]
)


def apagar_servidor() -> None:
    time.sleep(1)
    os.kill(os.getpid(), signal.SIGTERM)


@app.callback(
    Output("text", "children"),
    Input("button", "n_clicks"),
    prevent_initial_call=True,
)
def exit_button(n_clicks) -> None:
    th = Thread(target=apagar_servidor, daemon=True)
    th.start()
    return "EL SERVIDOR SE HA APAGADO"


tab_8 = dcc.Tab(
    tab_8_content,
    value="tab_8",
    label="Exit",
    style=tab_style,
    selected_style=tab_selected_style,
)
app.layout = html.Div(
    [
        dcc.Tabs(
            children=[tab_1, tab_2, tab_3, tab_4, tab_5, tab_6, tab_7, tab_8],
            value="tab_1",
        )
    ],
    style={"fontFamily": "arial"},
)

if __name__ == "__main__":
    app.run_server()
