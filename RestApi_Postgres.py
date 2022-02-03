# -*- coding: utf-8 -*-
"""
Created on Fri Jan 21 20:23:27 2022

@author: Independence
"""

import pandas as pd

from flask import Flask, request , Response
from flask_restful import Resource, Api

from functions import *

import sys

app = Flask(__name__)
api = Api(app)


@app.route('/getPlanRecomendation', methods=['GET'])
def getRol(): 
    
    args = request.args
    id_user = args.get('id_user')
    try:
        rslt_df = logic_recomendation(id_user)    
        return Response(rslt_df.to_json(orient="records"), mimetype='application/json')
    except BaseException as ex:
        
        ex_type, ex_value, ex_traceback = sys.exc_info()
        message='Opps ha ocurrido un error: ' + str(ex_type)+str(ex_value) 
        return Response(message,status=500, mimetype='application/json')

if __name__ == '__main__':
    app.run(debug=True)


