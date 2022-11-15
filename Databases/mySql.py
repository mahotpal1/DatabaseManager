import mysql.connector as conn
import logging as lg

class mySql : 
  
  def __init__(self,db_name):
    self.db_name = db_name
    lg.basicConfig(filename='C:\\Users\\Vishal\\DatabaseManager\\Logs\\mysqlLogs.log',level=lg.NOTSET, filemode='a',format='%(process)s-%(levelname)s-%(asctime)s-%(message)s', datefmt='%d-%b-%y %H:%M:%S')
  
  def create_table(self,table_name,table_comps):
    lg.info('Executing Create Table!!!!')
    lg.debug('Trying to establish db connection...')
    try :
      mydb = conn.connect(host="localhost", database=str(self.db_name), user="mahotpal1", passwd="Abc@12345", use_pure=True)
      if(mydb.is_connected) :
        lg.info('Database connection established!')
      try :
        query = "create table "+self.db_name+"."+table_name+" "+table_comps
        lg.info("Query provided : ",query)
        cursor = mydb.cursor()
        cursor.execute(query)
      except Exception as E :
        lg.warning('Please check query ')
        lg.error(str(E))   
    except Exception as e :
      mydb.close()
      lg.error('Database Connection failed!')
      lg.error(str(e))
    else:
      mydb.close()
  
  def insertSingleData(self,table_name,data):
    lg.info('Executing insertSingleData!!!!')
    try :
      mydb = conn.Connect(host="localhost", database=self.db_name, user="mahotpal1", passwd="Abc@12345")
      if(mydb.is_connected) :
        lg.info('Database connection established!')
      try :
        query = "Insert into "+self.db_name+"."+table_name+" Values "+data
        cursor = mydb.cursor()
        cursor.execute(query)
        mydb.commit()
      except Exception as E :
        lg.error(str(E))
    except Exception as e :
      lg.error(str(e))
      mydb.close()
    
  def updateData(self, table_name, update_statement):
    lg.info('Executing update Table!')
    try :
      mydb = conn.Connect(host="localhost", database=self.db_name, user="mahotpal1", passwd="Abc@12345")
      if(mydb.is_connected):
        lg.info('Database Connection Established!!!')
      try :
        query = "Update " + self.db_name + "." + table_name + update_statement
        print(query)
        cursor = mydb.cursor()
        cursor.execute(query)
        mydb.commit()
      except Exception as E :
        lg.error(str(E))
    except Exception as e :
      lg.error(str(e))
      mydb.close()

  def bulkInsertion(self, table_name, dataList) :
    lg.info('Executing bulk Insertion!!!')
    try :
      mydb = conn.Connect(host="localhost", database=self.db_name, user="mahotpal1", passwd="Abc@12345")
      if(mydb.is_connected):
        lg.info('Database Connection Established!!!')
      try :
        for i in dataList :
          query = "Insert into "+self.db_name+"."+table_name+" Values "+str(i)
          cursor = mydb.cursor()
          cursor.execute(query)
          mydb.commit() 
      except Exception as E : 
        lg.error(str(E))
    except Exception as e :
      lg.error(str(e))
      mydb.close()

  def __str__(self):
    return "mySql operation completed. Check log files for details."
    lg.shutdown()

#if __name__ == '__main__' :
  #c = mySql("db1")
  #c.create_table("table_ac","(Studentid INT AUTO_INCREMENT PRIMARY KEY, FirstName VARCHAR(60), LastName VARCHAR(60));")
  #create table db1.table123 (Studentid INT AUTO_INCREMENT PRIMARY KEY, FirstName VARCHAR(60), LastName VARCHAR(60));
  #drop table db1.table123;
  #select * from db1.table123; 
  #c.updateData("table123"," set FirstName='Harsh', LastName='Mrigank' \n where Studentid=1;")
  #c.insertSingleData("table123","(3, 'Lakshya', 'Prabhav');")
  #c.bulkInsertion("table123", ["(4, 'Akash', 'Deep');", "(5, 'Kumar', 'Ashish');", "(6, 'Prabhat', 'Kumar');"])