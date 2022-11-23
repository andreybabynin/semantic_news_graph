from flask import Flask, render_template, url_for, jsonify
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import pandas as pd
import numpy as np

app = Flask(__name__)



@app.route('/')
@app.route('/home')
def index():
    return render_template("index.html")


@app.route('/about')
def about():
    return render_template("about.html")




if __name__ == "__main__":
    app.run(debug=True)
