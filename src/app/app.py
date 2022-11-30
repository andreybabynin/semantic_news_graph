import uuid
import re
import os
from itertools import combinations
from flask import Flask, render_template, request, jsonify, session
import pandas as pd
import networkx as nx
import psycopg2
from psycopg2 import Error


# Hyperparameters
app = Flask(__name__)
app.config["TEMPLATES_AUTO_RELOAD"] = True
# app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY')
app.config["SECRET_KEY"] = uuid.uuid4().hex

PG_CONN_CFG = {
    "dbname": os.environ.get("POSTGRES_DB"),
    "user": os.environ.get("POSTGRES_USER"),
    "host": os.environ.get("POSTGRES_HOST"),
    "port": os.environ.get("POSTGRES_PORT"),
}
with open(os.environ.get("POSTGRES_PASSWORD_FILE"), "r") as f:
    PG_CONN_CFG["password"] = f.readlines()[0].rstrip("\n")


def get_db_data_for_triplets(pg_conn_cfg, date_min: str, date_max: str):

    # check query dates to sql injection
    date_pattern = r"\d\d\d\d-\d\d-\d\d"
    if re.fullmatch(date_pattern, date_min) and re.fullmatch(date_pattern, date_max):
        try:
            pg_con = psycopg2.connect(
                dbname=pg_conn_cfg["dbname"],
                user=pg_conn_cfg["user"],
                password=pg_conn_cfg["password"],
                host=pg_conn_cfg["host"],
                port=pg_conn_cfg["port"],
            )
            query = f"""
                SELECT id_news, summary_text, news_date
                FROM (SELECT * FROM news
                    WHERE news_date
                            BETWEEN '{date_min}' AND
                                    '{date_max}') news
                    INNER JOIN news_summary USING(id_news)
            """
            df_news = pd.read_sql(query, pg_con, index_col=["id_news"])

            query = f"""
                SELECT id_news, ner_name
                FROM (SELECT * FROM news_links
                    WHERE id_news IN (SELECT id_news FROM news
                                        WHERE news_date
                                        BETWEEN '{date_min}' AND
                                        '{date_max}')) news_links
                    INNER JOIN ner USING(id_ner)
            """
            df_nlinks = pd.read_sql(query, pg_con)

        except (Exception, Error) as error:
            print("Error connection to PostgreSQL:\n", error)
            df_news = None
            df_nlinks = None

        finally:
            if "pg_con" in locals() and pg_con:
                pg_con.close()
    else:
        df_news = None
        df_nlinks = None
        print(f"Dates is incorrect {date_min} - {date_max}")

    return df_news, df_nlinks


def compute_triplets(
    PG_CONN_CFG,
    input_ner,
    date_min: str,
    date_max: str,
    graph_depth=None,
    min_news_count=1,
):
    # query to db
    df_news, df_nlinks = get_db_data_for_triplets(PG_CONN_CFG, date_min, date_max)

    # task: change to fuzzy search by synonyms table
    founded_ner = input_ner

    if (
        df_news is None
        or df_nlinks is None
        or (input_ner != "" and founded_ner not in df_nlinks.ner_name.values)
    ):
        return pd.DataFrame(
            {
                "source": ["no_node1"],
                "target": ["no_node2"],
                "edge": ["0 news"],
                "amount": [0],
                "news": [["no news"]],
            }
        )

    df_nlinks_counts = df_nlinks.groupby(by="id_news").ner_name.count()

    # удаляем новости с >= 10 нер, пока не решится вопрос с комплексными
    # сводками новостей (часто с ключевым словом "главное:"), где могут
    # в виде списка приводится не связанные между собой новости,
    # в настоящий момент все ner, упоминаемые в любой части тако сводки,
    # окажутся связанными между собой, что не является верным; имеет смысл
    # дробить такие новости на несколько, либо использовать схожий подход,
    # когда ner будут связаны между обой только в пределах перечислений
    id_news_to_drop = df_nlinks_counts[
        ((df_nlinks_counts < 2) | (df_nlinks_counts > 10))
    ].index
    df_nlinks = df_nlinks[~df_nlinks.id_news.isin(id_news_to_drop)]

    if input_ner != "":

        lvl_ners = [
            [founded_ner],
        ]  # functionality for future
        lvl_idx = []  # functionality for future
        prev_lvls_idx = []

        for _ in range(graph_depth):

            lvl_idx.append(
                list(
                    set(
                        df_nlinks[
                            (
                                df_nlinks.ner_name.isin(lvl_ners[-1])
                                & (~df_nlinks.id_news.isin(prev_lvls_idx))
                            )
                        ].id_news
                    )
                )
            )

            lvl_ners.append(
                list(
                    set(df_nlinks[df_nlinks.id_news.isin(lvl_idx[-1])].ner_name)
                    - set(lvl_ners[-1])
                )
            )

            prev_lvls_idx.extend(lvl_idx[-1])

        # print(list(map(len, lvl_idx)))
        # print(list(map(len, lvl_ners)))

        df_nlinks = df_nlinks[df_nlinks.id_news.isin(prev_lvls_idx)]

    df_nlinks = df_nlinks.groupby("id_news").agg({"ner_name": sorted})

    df_nlinks = df_nlinks.merge(df_news, how="left", left_index=True, right_index=True)
    del df_news

    df_nlinks["ner_name"] = df_nlinks.ner_name.map(lambda x: list(combinations(x, 2)))

    df_nlinks["news"] = df_nlinks.news_date.astype(str) + ": " + df_nlinks.summary_text
    df_nlinks = df_nlinks[["ner_name", "news"]]

    df_triples = df_nlinks.explode("ner_name")
    del df_nlinks

    df_triples = df_triples.groupby("ner_name", as_index=False).agg(
        news=("news", sorted), amount=("news", len)
    )

    # drop edges with news amount < min_news_count
    if min_news_count > 1:
        df_triples = df_triples[df_triples.amount >= min_news_count]

    df_triples["edge"] = df_triples.amount.map(lambda x: f"{x} news")

    df_triples[["source", "target"]] = pd.DataFrame(
        df_triples["ner_name"].tolist(), index=df_triples.index
    )

    df_triples = df_triples[["source", "target", "edge", "amount", "news"]]

    return df_triples


def build_network(graph_query):

    df_triples = compute_triplets(PG_CONN_CFG, **graph_query)
    df_triples = df_triples.rename(columns={"edge": "name"})[
        ["source", "target", "name"]
    ]

    G = nx.from_pandas_edgelist(
        df_triples,
        source="source",
        target="target",
        edge_attr="name",
        create_using=nx.Graph(),
    )
    data = nx.node_link_data(G)

    return data


# DELETE BEFORE PROD!!!
@app.route("/temp", methods=["GET", "POST"])
def chrt():
    return render_template("temp2.html")


@app.route("/", methods=["GET", "POST"])
def index_func():
    if request.method == "POST":
        graph_query = {
            "input_ner": request.form.get("input_ner"),
            "date_min": request.form.get("date_min"),
            "date_max": request.form.get("date_max"),
            "graph_depth": int(request.form.get("graph_depth")),
            "min_news_count": int(request.form.get("min_news_count")),
        }
    else:
        graph_query = {
            "input_ner": "",
            "date_min": "2022-08-01",
            "date_max": "2022-08-31",
            "min_news_count": 10,
        }

    session["graph_query"] = graph_query

    print(f"Info: Graph query: {graph_query}")

    return render_template("index.html")


@app.route("/data")
def static_proxy():
    network = build_network(session.get("graph_query"))

    return jsonify(network)


def main():
    app.run(host="0.0.0.0", debug=True)


if __name__ == "__main__":
    main()
