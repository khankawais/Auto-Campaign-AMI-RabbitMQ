import pika
import mysql.connector
import datetime
from consumer_logger import genlog
import asterisk.manager

RMQcredentials = pika.PlainCredentials('admin', 'root')
RMQconnection = pika.BlockingConnection(pika.ConnectionParameters('10.0.0.121',5672,'/',RMQcredentials)) # Connecting to RabbitMQ Server
RMQchannel = RMQconnection.channel()
RMQchannel.queue_declare(queue='hello', durable=True) # Creating a Duable Queue .. it will not be created if the queue already exists.

AMImanager = asterisk.manager.Manager()
AMImanager.connect('10.0.1.155')
AMImanager.login('awais', 'awais') # Login to the Asterisk Manager Interface ( AMI ) 


def callback(ch, method, properties, body):
    global cursor # Mysql Object to Execute Queries
    global sqlconnection # Mysql Connection Object
    body=body.decode("utf-8") # Decoding bytes into String ( UTF-8 )
    data=eval(body)  # Evaluating string to be a Dictionary
    genlog.info(f" [x] Received {data}") # Writing into log file
    phone=data["phone_number"]
    if int(phone) != 0:

        response=AMImanager.originate(
        channel="SIP/auto_campaign",
        exten=103,
        context='phones', 
        priority='1', 
        timeout='8000', 
        application=None, 
        data=None, 
        caller_id="Automated Campaign", 
        run_async=False, 
        earlymedia='false', 
        account=None, 
        variables={'UEXT':phone,'NAME':data["name"]}
        )   # Calling Originate Action For AMI

        if str(response) == "Success":
            genlog.info("[+] Call was Picked Up by the Agent")
            process_id=data["process_id"]
            process_id=int(process_id)+1
            time_now=str(datetime.datetime.now())
            time_now=time_now[:19]
            query=f"UPDATE customers SET `process_id`='{process_id}' , `process_time`= '{time_now}' where id={data['id']}"
            cursor.execute(query)
            sqlconnection.commit()
            ch.basic_ack(delivery_tag = method.delivery_tag) # Sending Acknowledgement that the Message was Recieved and Processed.
        else:
           genlog.info(" [X] Call was not Picked Up by the Agent")
           ch.basic_ack(delivery_tag = method.delivery_tag)

    else:
        ch.basic_ack(delivery_tag = method.delivery_tag) 

try:
    sqlconnection = mysql.connector.connect(host='10.0.1.98',database='campaign',user='root',password='awais')  # Connecting to the MYSQL Database
    cursor = sqlconnection.cursor()
    RMQchannel.basic_consume(queue='hello', on_message_callback=callback)
    genlog.info(' [*] Waiting for messages')
    RMQchannel.start_consuming()
except mysql.connector.Error as error:
    genlog.error("Failed to update table record: {}".format(error))
finally:
    if (sqlconnection.is_connected()):
        RMQconnection.close()