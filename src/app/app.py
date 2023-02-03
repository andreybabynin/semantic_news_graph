import uuid
import re
import os
from itertools import combinations
from flask import Flask, render_template, request, jsonify, session
from werkzeug.middleware.proxy_fix import ProxyFix
from waitress import serve
from datetime import datetime, timedelta
import pandas as pd
import networkx as nx
import psycopg2
from psycopg2 import Error

app = Flask(__name__)
app.config["TEMPLATES_AUTO_RELOAD"] = True
app.config["SECRET_KEY"] = uuid.uuid4().hex
app.config['JSON_AS_ASCII'] = False

PG_CONN_CFG = {
    "dbname": os.environ.get("POSTGRES_DB"),
    "user": os.environ.get("POSTGRES_USER"),
    "host": os.environ.get("POSTGRES_HOST"),
    "port": os.environ.get("POSTGRES_PORT"),
}
with open(os.environ.get("POSTGRES_PASSWORD_FILE"), "r") as f:
    PG_CONN_CFG["password"] = f.readlines()[0].rstrip("\n")

sample_df = pd.DataFrame({"source": ["bad-PER", "bad-LOC", "bad-ORG"],
                        "target": ["bad-ORG", "bad-MISC", "bad-LOC"],
                        "amount": [0, 0, 0],
                        "news": [["nonews00", "nonews01", "nonews02"],
                                ["nonews2"], ["nonews3"]]})

sample_node_type_dic = {'bad-PER': {'ner_type': 'PER'},
                        'bad-LOC': {'ner_type': 'LOC'},
                        'bad-MISC': {'ner_type': 'MISC'},
                        'bad-ORG': {'ner_type': 'ORG'}
                        }

def get_db_data_for_triplets(pg_conn_cfg, input_ner: str, date_min: str, date_max: str, depth: int):

    # clean input ner
    input_ner = re.sub(r"[^a-zA-Zа-яА-ЯёЁ№0-9 ]+", "", input_ner)
    founded_ner = None
    df_query = []

    try:
        pg_con = psycopg2.connect(**pg_conn_cfg)

        # synonyms search for input ner
        if len(input_ner) > 0:
            query = """
                with t as (
                    select id_ner
                    from ner_synonyms
                            ORDER BY SIMILARITY(ner_synonym, %(input_ner)s) desc 
                            limit 1)
                select ner_name
                    from ner join t on t.id_ner = ner.id_ner;
                    """
            pg_cur = pg_con.cursor()
            pg_cur.execute(query, {"input_ner": input_ner})
            res = pg_cur.fetchall()
            founded_ner = res[0][0]
            pg_cur.close()           

        if founded_ner is not None:
            query = f"""
                    with recursive t1 as (select id_ner, ner_name, id_news, ner_type, news_date
                        from ner join news_links nl using(id_ner)
                                    join news using(id_news)
                                    join ner_types using(id_ner_type)
                        where news_date BETWEEN '{date_min}' and '{date_max}' 
                        ),
                    t3 as 
                        (select t1.id_news, t1.ner_name as source, t1.id_ner as source_id, 
                                        t1.ner_type as source_type, 
                                        t2.ner_name as target, t2.id_ner as target_id,
                                        t2.ner_type as target_type,
                                        t1.news_date
                            from t1, t1 as t2
                            where t1.ner_name <> t2.ner_name
                                and t1.id_news = t2.id_news
                        ),
                    tr as (select id_news, news_date, source, source_id, source_type, 
                                target, target_id, target_type, 1 as depth
                            from t3
                                where t3.source='{founded_ner}'
                        union
                            select t3.id_news, t3.news_date, tr.target, tr.target_id, tr.target_type,
                                    t3.target, t3.target_id, t3.target_type, depth+1
                            from tr, t3
                            where tr.target = t3.source
                                and tr.id_news <> t3.id_news ---для того чтобы избежать полного графа A -> B -> C -> A
                                and depth<{depth}
                        )
                    select id_news, news_date, source, source_id, source_type, 
                                target, target_id, target_type, summary_text, depth
                    from tr join news_summary using (id_news)              
                    """
            df_query = pd.read_sql(query, pg_con)

    except (Exception, Error) as error:
            print("Error connection to PostgreSQL:\n", error)

    finally:
            if "pg_con" in locals() and pg_con:
                pg_con.close()

    return founded_ner, df_query

def compute_triplets(
                        PG_CONN_CFG,
                        input_ner,
                        date_min: str,
                        date_max: str,
                        graph_depth=1,
                        min_news_count=1,
                    ):
    # query to db and fuzzy search by synonyms table
    founded_ner, df_query = get_db_data_for_triplets(
                            PG_CONN_CFG, input_ner, date_min, date_max, graph_depth
                            )

    if (founded_ner is None) or (len(df_query)==0):
        return sample_df, sample_node_type_dic

    # remove news with number of NEs >5
    #TODO: calibrate threshold
    df_temp = df_query.groupby(['id_news', 'source'])['news_date'].count().sort_values(ascending=False).reset_index()
    id_news_to_drop = df_temp[df_temp['news_date']>=5].id_news.values
    df_query = df_query[~df_query.id_news.isin(id_news_to_drop)]
    del df_temp

    # df_query['source_name'] = df_query.apply(lambda x: x['source']+ '#SELF' if x['depth']==1 else 
    #                                             x['source']+'#' + x['source_type'], axis =1)
    # df_query['target_name'] = df_query.apply(lambda x: x['target']+ '#'+ x['target_type'], axis=1)

    df_triplets = df_query.groupby(['source', 'target']).agg({'id_news': ['count']})
    df_triplets = df_triplets.reset_index()
    df_triplets.columns = ['source', 'target', 'count']
    df_triplets = df_triplets[(df_triplets['count']>=min_news_count) | (df_triplets['source']==founded_ner)]
    
    df_triplets = pd.merge(df_triplets, df_query.drop(columns=['depth']), 
            how='inner', left_on=['source', 'target'],
            right_on=['source', 'target']).drop(columns=['id_news', 'source_id', 'target_id'])

    # convert data
    df_triplets['news_date'] = pd.to_datetime(df_triplets['news_date'])
    df_triplets['news_date'] = df_triplets['news_date'].apply(lambda x: datetime.strftime(x.date(), '%Y-%m-%d'))

    #add data to news
    df_triplets['summary_text'] = df_triplets.apply(lambda x: x['news_date'] + ' - ' + x['summary_text'], axis=1)

    # extract NEs types
    common_list = df_triplets[['source', 'source_type']].drop_duplicates().values.tolist() + \
                    df_triplets[['target', 'target_type']].drop_duplicates().values.tolist()

    nodes_type_dic = {i[0]: {'ner_type': i[1]} for i in common_list}
    nodes_type_dic[founded_ner]['ner_type'] = 'SELF'

    #sort news by date from earliest to latest
    df_triplets = df_triplets.sort_values(by='news_date')

    df_triplets = df_triplets.groupby(['source', 'target', 'count'])['summary_text'].apply(list).reset_index()
    df_triplets.rename(columns={'count': 'amount', 'summary_text': 'news'}, inplace=True)
    
    return df_triplets, nodes_type_dic


def build_network(graph_query):

    df_triplets, node_type_dic = compute_triplets(PG_CONN_CFG, **graph_query)

    G = nx.from_pandas_edgelist(
        df_triplets,
        source="source",
        target="target",
        edge_attr=["amount", "news"],
        create_using=nx.Graph(),
    )
    #add nes attribut to nodes
    nx.set_node_attributes(G, node_type_dic)
    data = nx.node_link_data(G)

    return data


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
            "input_ner": "Москва",
            "date_min": (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d"),
            "date_max": datetime.now().strftime("%Y-%m-%d"),
            "graph_depth": 1,
            "min_news_count": 3,
        }

    session["graph_query"] = graph_query

    return render_template("index.html")


@app.route("/data")
def static_proxy():
    network = build_network(session.get("graph_query"))
    return jsonify(network)


@app.route("/FAQ", methods=["GET", "POST"])
def faq_func():
    return render_template("FAQ.html")


def main(production=True):
    if production:
        # for production
        app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)
        serve(app, host="0.0.0.0", port=5000)
    else:
        # for debugging, not production
        app.run(host="0.0.0.0", port=5000, debug=True)


if __name__ == "__main__":
    main(production=True)
