from flask import Flask, request, jsonify, render_template
from Databases.mySql import mySql

app = Flask(__name__)

@app.route('/', methods=['POST','GET'])
def home_page():
  return render_template('home.html')

@app.route('/gotodatabase', methods=['POST'])
def goToDatabase():
  if(request.method=='POST'):
    database_name = request.form['select_db']
  return render_template('databases.html',db_name=database_name)

@app.route('/mysqlPostman', methods=['POST'])
def execute_query_mysql():
  if (request.method=='POST'):
    dbName = request.json['dbName']
    operation = request.json['operation']
    tableName = request.json['tableName']
    query = request.json['query']
    
    obj1 = mySql(dbName)
    if (operation=='create_table'):
      result = obj1.create_table(tableName,query)
    elif (operation=='insertSingleData'):
      result = obj1.insertSingleData(tableName, query)
    elif (operation=='updateData'):
      result = obj1.updateData(tableName, query)
    elif (operation=='bulkInsertion'):
      dataList = list(query)
      result = obj1.bulkInsertion(tableName, dataList)  
    elif (operation=='deletefromTable') :
      result = obj1.delete_from_table(tableName,query)    
    else:
      result = 'Incorrect Operation'
  
  return jsonify(result)

if __name__=='__main__':
  app.run()