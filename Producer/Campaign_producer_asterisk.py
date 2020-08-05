import mysql.connector
import sys
import pika
from producer_logger import genlog
RMQcredentials = pika.PlainCredentials('admin', 'root')
try:
    RMQconnection = pika.BlockingConnection(pika.ConnectionParameters('10.0.0.121',5672,'/',RMQcredentials)) # Connecting to RabbitMQ Server.
    RMQchannel = RMQconnection.channel()
    RMQchannel.queue_declare(queue='hello', durable=True)   # Creating a Duable Queue .. it will not be created if the queue already exists.
    try:
        mysqlconnection = mysql.connector.connect(host='10.0.1.98',database='campaign',user='root',password='awais')    # Connectin gto MySQL Server.
        if mysqlconnection.is_connected():
            genlog.info("Connection Established with MySQL Server")
            cursor = mysqlconnection.cursor()   # Cursor object creater to execute MySQL Queries.
            cursor.execute(sys.argv[1]) # Takes the query from the Arguement given when running the Python Script
            Result=cursor.fetchall()
            if len(Result) == 0:
                genlog.info("Query Returned Nothing ! [ Nothing to Publish ]")
                
            else:
                for result in Result:
                    dictionary={"id":result[0],"name":result[1],"phone_number":result[2],"email":result[3],"process_id":result[4],"process_time":result[5]} # Create a dictionary to send to the Queue.           
                    RMQchannel.basic_publish(exchange='', routing_key='hello', body=str(dictionary),properties=pika.BasicProperties(delivery_mode = 2,)) # Publish / Send the message to the Queue .. delivery mode =2 means the mesaage is persistant.. it will stay in the queue if the server is restarted.  
                    genlog.info("Publish Successfull")
    except mysql.connector.Error as e:
        genlog.error(f"Error while connecting to MySQL : {e}")
        
        
        
    finally:
        if (mysqlconnection.is_connected()):    # Closing the Connections when Done publishing.
            cursor.close()
            RMQconnection.close()
except:
    genlog.error("Error while connecting to RabbitMQ Server !!") 