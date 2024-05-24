from flask import Flask, request, redirect, url_for, flash, jsonify, make_response
import numpy as np 
import pandas 
import pickle
import json
import os, sys
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from helper import score_new_df

#Path for PySpark source folder
os.environ['SPARK_HOME'] = 'C:/Users/aditya/Downloads/spark-3.0.2-bin-hadoop2.7'

#Add pyspark to python path
sys.path.append('C:/Users/akhil/Downloads/spark-3.0.2-bin-hadoop2.7/python')

conf = SparkConf().setAppName('real_time_scoring_api')
conf.set('spark.sql.warehouse.dir', 'file://C:/Users/aditya/Downloads/spark-3.0.2-bin-hadoop2.7/spark-warehouse')
conf.set("spark.driver.allowMultipleContexts", "true")
spark = SparkSession.builder.master('akhil').config(conf=conf).getOrCreate()
sc = spark.sparkContext

app = Flask(__name__)
@app.route('/api/', methods=['POST'])

def makecalc():
    
    json_data = request.get_json()
    #Real the real time input to pyspark df
    score_data = spark.read.json(sc.parallelize(json_data))
    final_scores_df = score_new_df(score_data)
    pred = final_scores_df.toPandas()
    final_pred = pred.to_dict(orient='rows')[0]
    return jsonify(final_pred)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
