import logging
LOG_FORMAT="%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(filename='/var/log/consumer.log',level=logging.INFO,format=LOG_FORMAT)

genlog=logging.getLogger()
