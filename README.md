# Auto Campaign through AMI and RabbitMQ

- The Producer code connects to the database and fetches data and then produce that data in the form of messages.  
- These messages are consumed by Consumer and the consumer places calls to the users in the database
