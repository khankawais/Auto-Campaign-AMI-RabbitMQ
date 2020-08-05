import mysql.connector
import sys
import pika
from producer_logger import genlog
RMQcredentials = pika.PlainCredentials('admin', 'root')
try:
    RMQconnection = pika.BlockingConnection(pika.ConnectionParameters('10.0.0.121',5672,'/',RMQcredentials))
    RMQchannel = RMQconnection.channel()
    RMQchannel.queue_declare(queue='hello', durable=True)
    try:
        mysqlconnection = mysql.connector.connect(host='10.0.1.98',database='campaign',user='root',password='awais')
        if mysqlconnection.is_connected():
            genlog.info("Connection Established with MySQL Server")
            cursor = mysqlconnection.cursor()
            cursor.execute(sys.argv[1])
            Result=cursor.fetchall()
            if len(Result) == 0:
                genlog.info("Query Returned Nothing ! [ Nothing to Publish ]")
                
            else:
                for result in Result:
                    dictionary={"id":result[0],"name":result[1],"phone_number":result[2],"email":result[3],"process_id":result[4],"process_time":result[5]}                    
                    RMQchannel.basic_publish(exchange='', routing_key='hello', body=str(dictionary),properties=pika.BasicProperties(delivery_mode = 2,))
                    genlog.info("Publish Successfull")
    except mysql.connector.Error as e:
        genlog.error(f"Error while connecting to MySQL : {e}")
        
        
        
    finally:
        if (mysqlconnection.is_connected()):
            cursor.close()
            RMQconnection.close()
except:
    genlog.error("Error while connecting to RabbitMQ Server !!") 