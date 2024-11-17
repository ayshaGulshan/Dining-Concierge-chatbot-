import json
import boto3
client = boto3.client('lexv2-runtime')
def lambda_handler(event, context):
  
    """
    Lambda Handler to handle the user input and interact with Lex bot.
    """
    print("Event received is ", event)
    
    #retrieved the user message
    user_input_message = event['messages'][0]['unstructured']['text']
    
    #Lex response from bot
    lex_response_to_user = client.recognize_text(
                botAliasId= "TSTALIASID",
                botId= "H7AZUYTS0Z",
                localeId= "en_US",
                sessionId= "183631322204720",
                text=user_input_message
    )
    #Retrieving the lex message from from the response
    lex_message_to_user = lex_response_to_user['messages'][0].get('content', 'Sorry, I didn’t get your message.')

    #Sending initial response to the 
    return {
      "messages": [
        {
          "type": "unstructured",
          "unstructured": {
            "text": lex_message_to_user
          }
       }
]
}
