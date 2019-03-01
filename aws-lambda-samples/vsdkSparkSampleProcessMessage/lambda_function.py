import os
import boto3
import json
import datetime
import vault_rest as vr

# Retrieve Environment variables
VaultRESTApiBaseUrl = os.environ['VAULT_REST_API_BASE_URL'] # i.e. 'https://<your-vault>.vaultdev.com/api/v18.3/'
VaultIntegrationUser = os.environ['VAULT_USER']
VaultIntegrationPassword = os.environ['VAULT_PASSWORD']
VaultClientId = os.environ['CLIENT_ID'] # See https://developer.veevavault.com/docs/#client-id
VaultAPIBurstLimitCutoff = os.environ['VAULT_API_BURST_LIMIT_CUTOFF'] # i.e. 200
VaultVQLPageLimit = os.environ['VAULT_VQL_PAGE_LIMIT'] # the maximum number of pages return by the VQL, e.g. 200. Maximum value of 1000

# Main method
def lambda_handler(event, context):

    # Variables
    batchIds = []
    vaultObject = 'vsdk_loan_approval__c'

    # Retrive record from SQS queue entry
    record = event['Records'][0]
    messageId = record.get('messageId')
    print("messageId: " + messageId)

    # Retreive details from the SQS record's message body
    bodyString = record.get('body')
    body = json.loads(bodyString)
    messagecontents = body.get('message')
    # Attributes
    attributes = messagecontents.get('attributes')
    vaultEvent = attributes.get('event')  # i.e. 'External update'
    vaultSessionId = attributes.get('sessionId')

    # Items
    messageItems = messagecontents.get('items')
    for messageItem in messageItems:
        
        vaultId = messageItem
        print("vaultId: " + vaultId)

        # Add the Vault Id currently being processed to the list of 
        # batch Ids
        batchIds.append(vaultId)
        
    # Update the specified object record via the Vault REST API, to set the 
    # AWS Finance Quote fields for all the items in the batch
    processedStatus =  vaultUpdateObjectAWSFinanceDetails(vaultObject, batchIds, vaultEvent, vaultSessionId)

    return {
        'statusCode': 200,
        'body': "Function processed"
    }
    
#
# Vault Business Logic
#

def vaultUpdateObjectAWSFinanceDetails(vaultObject, batchIds, vaultEvent, currVaultSessionId):
    """
    Update the AWS Finance Quote Detail fields, where they haven't already been 
    populated.

    :param vaultObject: the name of Vault Object being updated
    :param batchIds: array of Vault Objects to be processed
    :param vaultEvent: event in Vault the message was created from
    :param currVaultSessionId: the Vault sessionId provided in the Spark message

    :return: processedStatus: Whether the Vault call was successful
    """

    # Obtain session id from username/password if it isn't provided in the Spark message
    if not currVaultSessionId:
        sessionId = vr.vaultLogin(VaultRESTApiBaseUrl, VaultIntegrationUser, VaultIntegrationPassword, VaultClientId)
    else:
        sessionId = currVaultSessionId
    print("sessionId: " + str(sessionId))

    # Retreive the records for the specified batches
    vqlQuery = "SELECT id, name__v, surname__c, item__c, loan_amount__c, loan_period_months__c, number_of_quotes__c "
    vqlQuery = vqlQuery + " FROM " + vaultObject
    vqlQuery = vqlQuery + " WHERE id CONTAINS ("
    for batchId in batchIds:
        vqlQuery = vqlQuery + "'" + batchId + "',"
    vqlQuery = vqlQuery[:-1] + ")"
    vqlQuery = vqlQuery + " LIMIT " + VaultVQLPageLimit
    print("vqlQuery : " + vqlQuery)

    restAPIResponseStr = vr.vaultPost(VaultRESTApiBaseUrl + 'query', {'q': vqlQuery}, sessionId, VaultClientId)
    processedStatus = vr.apiProcessedStatus(restAPIResponseStr, VaultAPIBurstLimitCutoff)
    response = json.loads(restAPIResponseStr.content)
    queryData = response.get('data')
    
    ####################################################################
    #
    # Add 3rd Party Code here to process the message
    # i.e. create/update records, etc.
    #
    # For example...
    #
    # Generate an AWS finance quote for each row in the queryData record and 
    # update the corresponding records in Vault 
    
    if ((processedStatus != vr.VAULT_REST_API_BURST_BREACH) and (processedStatus != vr.VAULT_REST_API_FAILURE)):
        hirePurchaseQuotes = generateLoanApprovalQuotes(queryData, vaultEvent)
        processedStatus = vaultSetObjectAWSFinanceDetails(hirePurchaseQuotes, vaultObject, sessionId)
        
    ####################################################################
    
    
    # Process the subsequent pages if needed
    if ((processedStatus != vr.VAULT_REST_API_BURST_BREACH) and (processedStatus != vr.VAULT_REST_API_FAILURE)):
        while 'next_page' in response['responseDetails']:
            # Get the full next page URL
            nextPage = response['responseDetails']['next_page']
            fullNextPageUrl = VaultRESTApiBaseUrl + nextPage[nextPage.find('query'):]  
            print("fullNextPageUrl: " + fullNextPageUrl)
            
            # Update the aws_id__c field with the external AWS ID, for each row 
            # in the next queryData record
            restAPIResponseStr = vr.vaultGet(fullNextPageUrl, sessionId, VaultClientId)
            processedStatus = vr.apiProcessedStatus(restAPIResponseStr, VaultAPIBurstLimitCutoff)
            response = json.loads(restAPIResponseStr.content)
            queryData = response.get('data')
            print("queryData : " + queryData)
            
            ####################################################################
            #
            # Add 3rd Party Code here to process the message for subsequent pages
            # i.e. create record / update records, etc.
            #
            # For example...
            
            # Generate an AWS finance quote for each row in the queryData record and 
            # update the corresponding records in Vault 
            if (processedStatus):
                hirePurchaseQuotes = generateLoanApprovalQuotes(queryData, vaultEvent)
                processedStatus = vaultSetObjectAWSFinanceDetails(hirePurchaseQuotes, vaultObject, sessionId)
                
            ####################################################################
    
    return processedStatus


def generateLoanApprovalQuotes(queryData, vaultEvent):
    """
    Generate loan approval quotes for each of the quote requests from Vault

    :param queryData: the records from the Vault Object VQL to generate the quotes from
    :param vaultEvent: event in Vault the message was created from
    
    :return: loanApprovalQuotesJson: JSON data containing the loan approval quotes
    """
    
    # Create a JSON body for the bulk update of the object
    loanApprovalQuotesJson = []

    # For each row, generate a new quote and add it to a JSON object
    # for the subsequent bulk update in Vault
    for row in queryData:
        
        # Retreive record values
        objId = row['id']
        objForename = row['name__v']
        objSurname = row['surname__c']
        objItem = row['item__c'] 
        objAmount = row['loan_amount__c'] 
        objPeriod = row['loan_period_months__c'][0]
        objNumberOfQuotes = row['number_of_quotes__c'] 
        
        # Set Loan Approval Quote Details
        
        # Calculate the repayable amounts
        annualInterestPercentage = 15
        # Give discount for requotes
        if (vaultEvent == 'Loan re-quote'):
            annualInterestPercentage = 12

        interestRateAnnual = str(annualInterestPercentage)+'%'
        periodInYears = int(objPeriod[:2]) // 12
        currYear = 1
        totalRepayable = objAmount;
        while currYear <= periodInYears:
            totalRepayable = totalRepayable // 100 * (100+annualInterestPercentage)
            currYear += 1
        monthlyRepayable = round(totalRepayable // int(objPeriod[:2]))
        totalRepayable = monthlyRepayable * int(objPeriod[:2])  

        # Get the current timestamp when the batch is run
        currDate = datetime.datetime.now()
        quoteDatestamp = currDate.replace(microsecond=0).isoformat() + '.000Z'

        # Create Quote Ref Number using sample format <initial>-<surname><item>-<currDateTime>   
        quoteRefNo = (objForename[0] + '-' + objSurname + objItem[:3] + currDate.strftime('%Y%m%d%H%M%S')).upper().replace("'","")
        
        if not objNumberOfQuotes:
            numberOfQuotes = 0
        else:
            numberOfQuotes = objNumberOfQuotes + 1
        
        # Set the approval status
        approvalStatus = 'approved__c'
        if objAmount > 100000:
            approvalStatus = 'rejected__c'

        loanApprovalQuotesJson.append({
            'id': objId,
            'quote_reference_number__c': quoteRefNo,
            'quote_received_date__c': quoteDatestamp,
            'interest_rate_annually__c' : interestRateAnnual,
            'monthly_payment__c' : monthlyRepayable,
            'total_repayable__c' : totalRepayable,
            'approval_status__c' : approvalStatus,
            'number_of_quotes__c' : numberOfQuotes
            
        })
            
    print(loanApprovalQuotesJson)
    
    return loanApprovalQuotesJson

def vaultSetObjectAWSFinanceDetails(loanApprovalQuotes, vaultObject, sessionId):
    """
    Update the AWS Finance Quote fields, for each row in the quote data set.

    :param loanApprovalQuotes: JSON data containing the loan approval quotes
    :param vaultObject: the name of Vault Object being updated
    :param sessionId: Vault Session Id
    
    :return: processedStatus: Whether the Vault call was successful
    """

    # Run the bulk update inserting the AWS quote details
    url = VaultRESTApiBaseUrl + 'vobjects/' + vaultObject
    params = {
        'client_id': VaultClientId
    }

    restAPIResponseStr = vr.vaultPutBulk(url, params, loanApprovalQuotes, sessionId)
    processedStatus = vr.apiProcessedStatus(restAPIResponseStr, VaultAPIBurstLimitCutoff)
    return processedStatus
    