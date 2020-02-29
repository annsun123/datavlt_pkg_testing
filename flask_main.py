# -*- coding: utf-8 -*-
"""
Created on Fri Feb  7 15:03:43 2020

@author: anyan
"""

from final_main_api import main 
from flask import Flask, request, jsonify
app = Flask(__name__)

#API 1 - starting the process
@app.route('/get_main', methods=['GET'])
def get_main():
    from final_main_api import main 

    init = main()

    return ('successful run')

#API 2 - check DS algorithm status
@app.route('/get_main_status', methods=['GET'])
def get_main_status():
    # from new_function import status_api
    return jsonify({"status": "not implemented", "time": "---"})

#API 3 - returning json
@app.route('/get_json_api', methods=['POST','GET'])
def get_json():
    from new_function import json_api
    data = request.get_json(force=True)
    to_date = data['to_date']
    from_date = data['from_date']
    category=data['category']
    date_range=data['date_range']
    return jsonify({"output": json_api(to_date,from_date,category,date_range)})
    

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5002)