import os
import time
import boto3
import datetime
import dateutil.parser

#Initialized dynamoDB resource  and specified the table
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('user_info')  

#Method to handle the slot values
def handle_slot_value(slots, slot_name):
    if slot_name in slots:
        slot = slots[slot_name]
        if slot and 'value' in slot and 'interpretedValue' in slot['value']:
            return slot['value']['interpretedValue']
    return None
    
# #Method to handle the recommendations
# def get_existing_recommendation(email):
#     response = table.get_item(
#         Key={
#             'email': email
#         }
#     )
#     return response.get('Item')
    
#Method to handle slots 
def pushEmailInfoToSqs(slot_values):
    sqs_client = boto3.client('sqs')
    queue_url = "https://sqs.us-east-1.amazonaws.com/183631322204/myAwsQueue"

    response = sqs_client.send_message(
        QueueUrl=queue_url,
        # DelaySeconds=10,
        MessageAttributes={
            'Cuisine': {
                'DataType': 'String',
                'StringValue': slot_values[0]
            },
            'Location': {
                'DataType': 'String',
                'StringValue': slot_values[1]
            },
            'DiningDate': {
                'DataType': 'String',
                'StringValue': slot_values[2]
            },
            'DiningTime': {
                'DataType': 'String',
                'StringValue': slot_values[3]
            },
            'NumberOfPeople': {
                'DataType': 'Number',
                'StringValue': "{}".format(slot_values[4])
            },
            'Email': {
                'DataType': 'String',
                'StringValue': slot_values[5]
            }
        },
        MessageBody=(
            'User Input'
        )
    )
    return response
    

#Handle the slot elicit
def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message_content):
    slots[slot_to_elicit] = None
    response = {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'intent': {
                'name': intent_name,
                'state': 'InProgress',
                'slots': slots
            },
            'dialogAction': {
                'type': 'ElicitSlot',
                'slotToElicit': slot_to_elicit
            }
        },
        'messages': [{
            'contentType': 'PlainText',
            'content': message_content
        }]
    }
    
    return response

#Handle the close function    
def close(session_attributes, intent_name, message_content):
    print('Inside close')
    print('session_attributes -> ', session_attributes, ' intent_name -> ', intent_name, 'message_content', message_content)
    response = {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': {
                'type': 'Close'
            },
            'intent': {
                'name': intent_name,
                'state': 'Fulfilled',
            }
        },
        'messages': [{
            'contentType': 'PlainText',
            'content': message_content
        }]
    }
    return response
    
#Handle the delegate    
def delegate(session_attributes, slots):
    print('Inside delegate')
    print('session_attributes -> ', session_attributes, ' slots -> ', slots)
    return {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'intent': {
                'name': 'DiningSuggestionsIntent',
                'state': 'InProgress',
                'slots': slots
            },
            'dialogAction': {
                'type': 'Delegate'
            }
        }
    }

#Method to validate the date       
def validate_date(date):
    try:
        input_date = dateutil.parser.parse(date).date()
        if input_date < datetime.date.today():
            return True
        return False
    except ValueError:
        return False

#Method to validate the cuisine       
def validate_cuisine(cuisine):
    cuisines = ['indian','italian','mexican','chinese','japanese','french','greek']
    return cuisine.lower() not in cuisines

#Method to validate the time      
def validate_time(time, date):
    try:
        input_time = dateutil.parser.parse(time).time()
        input_date = dateutil.parser.parse(date).date()
        current_date = datetime.date.today()
        current_time = datetime.datetime.now().time()
        if input_date == current_date and input_time < current_time:
            return True
        return False
    except ValueError:
        return False

#Method to validate the location
def validate_location(location):
    return location.lower() != 'manhattan'

#Method to validate the people number
def validate_peoplenumber(people):
    return people < 1 or people > 20
    
    
#Method to handle the dining suggestion intent
def handle_dining_suggestions_intent(intent_request):
    slots = intent_request['sessionState']['intent']['slots']

    cuisine = handle_slot_value(slots, 'Cuisine')
    location = handle_slot_value(slots, 'Location')
    dining_date = handle_slot_value(slots, 'date')
    dining_time = handle_slot_value(slots, 'Time')
    people = handle_slot_value(slots, 'people')
    email = handle_slot_value(slots, 'email')
    continue_with = handle_slot_value(slots, 'ContinueWithRecommendation')

    session_attributes = intent_request.get('sessionAttributes', {})
  
    if people:
        people = int(people)
    
    if intent_request['invocationSource'] == 'DialogCodeHook':
        invalid_slot = None
        invalid_message = None

        validations = [
            (cuisine, validate_cuisine, 'Cuisine', 'This cuisine is not supported. Please provide another cuisine like italian, chinese, indian etc.'),
            (location, validate_location, 'Location', 'This location is not supported. Please provide another city'),
            (dining_date, validate_date, 'date', 'The entered date is invalid. Please provide date later than today.'),
            (dining_time, lambda x: validate_time(x, dining_date), 'Time', 'The invalid time entered. Please enter time later than the current time.'),
            (people, validate_peoplenumber, 'people', 'People number provided is invalid, Please provide a valid number between 1-20.')
        ]

        for value, check_function, slot, message in validations:
            if value and check_function(value):
                invalid_slot = slot
                invalid_message = message
                break

        if invalid_slot:
            return elicit_slot(session_attributes, intent_request['sessionState']['intent']['name'], slots, invalid_slot, invalid_message)
        return delegate(session_attributes, slots)
        
    slot_values = [cuisine, location, dining_date, dining_time, people, email]
    sqs_response = pushEmailInfoToSqs(slot_values)
    return close(session_attributes, 'DiningSuggestionsIntent', 'You’re all set. Expect my suggestions shortly at {} ! Have a good day!'.format(email))

#Method to handle the Thank you intent
def handle_Thankyou_intent(intent_request):
    session_attributes = intent_request.get('sessionState', {}).get('sessionAttributes', {})
    return close(session_attributes, 'ThankYouIntent', 'Thank you for choosing Dining Conceirge Chatbot.')
    
#Method to handle the greeting intent
def handle_greeting_intent(intent_request):
    session_attributes = intent_request.get('sessionState', {}).get('sessionAttributes', {})
    current_time_hour = datetime.datetime.now().hour
    if 5 <= current_time_hour < 12:
        user_greeting = "Hello Good morning"
    elif 12 <= current_time_hour < 18:
        user_greeting = "Hello Good afternoon"
    else:
        user_greeting = "Hello Good evening"
    response_msg = f'{user_greeting}, How can I help you?'
    return close(session_attributes, 'GreetingIntent', response_msg)

#Method to handle the intents
def route_intent(intent_request):
  
    intent_name = intent_request['sessionState']['intent']['name']
    if intent_name == 'GreetingIntent':
        return handle_greeting_intent(intent_request)

    elif intent_name == 'DiningSuggestionsIntent':
        return handle_dining_suggestions_intent(intent_request)

    elif intent_name == 'ThankYouIntent':
        return handle_Thankyou_intent(intent_request)

        
#Method to handle the lambda
def lambda_handler(event, context):
    #setting the time zone for lambda
    os.environ["TZ"] = 'America/New_York'
    time.tzset()
    return route_intent(event)
    
