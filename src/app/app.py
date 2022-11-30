from flask import Flask, render_template, request, jsonify, session
import pandas as pd
import networkx as nx
import uuid

app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True
# app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY')
# app.config['SECRET_KEY'] = uuid.uuid4().hex
app.config['SECRET_KEY'] = '3d6f45a5fc12445dbac2f59c3b6c7cb1'


def build_network(graph_query):
    print("BUILD_NETWORK_FUNK:", graph_query)
    df = pd.read_csv('example_graph.csv')
    G = nx.from_pandas_edgelist(df, source='source', target='target', edge_attr='name', create_using=nx.Graph())
    data = nx.node_link_data(G)

    return data


# DELETE BEFORE PROD!!!
@app.route("/temp", methods=['GET', 'POST'])
def chrt():
    return render_template('temp2.html')


@app.route("/", methods=['GET', 'POST'])
def index_func():
    if request.method == "POST":
        graph_query = {
            "input_ner": request.form.get('input_ner'),
            "date_min": request.form.get('date_min'),
            "date_max": request.form.get('date_max'),
            "graph_depth": request.form.get('graph_depth'),
            "min_news_count": request.form.get('min_news_count'),
        }
        session['graph_query'] = graph_query

    return render_template('index.html')


@app.route("/data")
def static_proxy():
    network = build_network(session.get('graph_query'))

    return jsonify(network)


def main():
    app.run(host="0.0.0.0", debug=True)


if __name__ == '__main__':
    main()
