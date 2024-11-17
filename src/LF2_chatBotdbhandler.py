import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# SQS queue and email setup
sqs_queue_url = "https://sqs.us-east-1.amazonaws.com/183631322204/myAwsQueue"
dynamodbTable = 'yelp-restaurants'  # DynamoDB table name that containing restaurant data
email_id = "ayshag@umich.edu"  #SES verified email ID for sending reataurant suggestions

#Method to get slots of intent
def get_slots_intent(message):
    
    # Checking whether the MessageAttributes exists in the SQS message
    if 'MessageAttributes' not in message:
        raise KeyError("MessageAttributes not found in the SQS message.")
    
    # Getting attributes from SQS message
    attributes = message.get('MessageAttributes', {})
    
    # Retrieving values from attributes, handling missing cases  
    cuisine = attributes.get('Cuisine', {}).get('StringValue', 'Unknown')
    location = attributes.get('Location', {}).get('StringValue', 'Unknown')
    dining_date = attributes.get('DiningDate', {}).get('StringValue', 'Unknown')
    dining_time = attributes.get('DiningTime', {}).get('StringValue', 'Unknown')
    people = attributes.get('NumberOfPeople', {}).get('StringValue', '1')
    email = attributes.get('Email', {}).get('StringValue', '')

    if not email:
        raise ValueError("Email is missing in the message of SQS.")
    
    return cuisine, location, dining_date, dining_time, people, email
    
#Method to get restaurants from DB
def get_restaurant_from_db(table, restaurant_ids):
    restaurant_list = []

    for restaurant_id in restaurant_ids[:10]:
        response = table.get_item(
            Key={'RestaurantID': restaurant_id}
        )
        if 'Item' in response:
            restaurant_list.append(response['Item'])
    
    return restaurant_list

#Method for getting the email message for user
def get_email_message(restaurant_list, cuisine, location, date, dining_time, people, email):
    # Storing the email in its receiving format
    email_message = f"Hello! Thank you for using Dining concierge chatbot. Here are my {cuisine} restaurant suggestions in {location} for {people} people, for {date} at {dining_time}: <br><br>"
    
    for index, restaurant in enumerate(restaurant_list, 1):
        email_message += f"{index}. {restaurant['name']}, located at {restaurant['address']}.<br><br>"
    
    email_message += "Enjoy your meal!"
    return email_message

#Method to handle the send email 
def send_email(recipients, email_message):
    ses = boto3.client('ses')
    response = ses.send_email(
        Source=email_id, 
        Destination={
            'ToAddresses': [recipients] 
        },
        ReplyToAddresses=[email_id], 
        Message={
            'Subject': {'Data': 'Dining Concierge restaurant suggestions', 'Charset': 'utf-8'},
            'Body': {
                'Text': {'Data': email_message, 'Charset': 'utf-8'},
                'Html': {'Data': email_message, 'Charset': 'utf-8'}
            }
        }
    )
    return response

#Method for handling the deletion of SQS message
def delete_SQSEntry(sqs, sqs_queue_url, message):
    receipt_handle = message['ReceiptHandle']
    sqs.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=receipt_handle)

#Method to connect dynamoDb Table
def connect_restaurant_dynamoDB():
    db = boto3.resource('dynamodb')
    table = db.Table(dynamodbTable)
    return table
    
#Method to get restaurants from DB that matches cuisine
def get_restaurant_ids(cuisine):
    table = connect_restaurant_dynamoDB()
   
    response = table.scan(
        FilterExpression=boto3.dynamodb.conditions.Attr('cuisine_type').eq(cuisine.lower())
    )
    restaurant_ids = [item['id'] for item in response['Items']]
    return restaurant_ids
    
#Method to get SQS queue message 
def get_sqsQueueMessage():
    sqs = boto3.client('sqs')
    sqs_message = sqs.receive_message(
        QueueUrl=sqs_queue_url, 
        MaxNumberOfMessages=1, 
        MessageAttributeNames=['All'],
        VisibilityTimeout=0, 
        WaitTimeSeconds=0
    )
    message = sqs_message['Messages'][0]
    logger.debug('Message Received from SQS Queue Q1={}'.format(message))
    return sqs, message

#Method for handling the lambda
def lambda_handler(event, context):
    
    # Polling SQS for a new message
    sqs, message = get_sqsQueueMessage()
   
    # Retrieving the message attributes from the SQS message
    cuisine, location, dining_date, dining_time, people, email = get_slots_intent(message)
    
    #Querying dynamoDB for restaurant IDs based on cuisine type
    restaurant_ids = get_restaurant_ids(cuisine)
    
    #Fetching the restaurant details based on the IDs
    table = connect_restaurant_dynamoDB()
    restaurant_list = get_restaurant_from_db(table, restaurant_ids)
    
    #Setting up the email message
    email_message = get_email_message(restaurant_list, cuisine, location, dining_date, dining_time, people, email)
  
    # Sending the email to specified email
   
    send_email(email, email_message)
    
    # Deleting the processed SQS email message 
    delete_SQSEntry(sqs, sqs_queue_url, message)

    return {
        'statusCode': 200,
        'body': email_message
        
    }