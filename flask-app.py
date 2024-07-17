from flask import Flask,render_template,jsonify

import pandas as pd
import plotly.express as px

app  = Flask(__name__)

df = pd.read_csv("2024-07-14_00_00_06_186018_BTC_rawdata_1.csv")

@app.route('/')

def index():
    return render_template("index.html")
def data():
    fig = px.line(df,x = "date",y = "prc_vol1",title="persent of volume over time")
    graphJSON = fig.to_json()
    return  jsonify(graphJSON)
if __name__=='__main__':
    app.run(debug=True)
