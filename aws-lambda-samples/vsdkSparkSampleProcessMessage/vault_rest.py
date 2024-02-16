import json
import requests

#
# Global constants
#
VAULT_REST_API_SUCCESS = 'SUCCESS'
VAULT_REST_API_FAILURE = 'FAILURE'
VAULT_REST_API_BURST_BREACH = 'BURST_BREACH'

# 
# Generic REST API specific Logic
#
def vaultRequestHeaderFromSessionId(sessionId, clientId):
    """
    Return the Vault Authorization Header from the Vault SessionID.

    :param sessionId: vault sessionId
    :param vaultClientId: client ID to track REST API Calls in logs

    :return: headers: Vault Authorization Header
    """
    headers = {'Authorization': sessionId, 'clientId': clientId}
    return headers
    
def vaultLogin(baseUrl, vaultUser, vaultPwd, vaultClientId):
    """
    Make the Vault login request, capture session id for the specified username
    and password

    :param baseUrl: the Vault Base URL
    :param vaultUser: Vault User
    :param vaultPwd: Vault Users password
    :param vaultClientId: client ID to track REST API Calls in logs

    :return: sessionId.
    """
    sessionId = ''
    authUrl = baseUrl + 'auth'
    params = {
        'username': vaultUser,
        'password': vaultPwd,
        'client_id': vaultClientId
    }
    responseStr = requests.post(url=authUrl, params=params)
    response = json.loads(responseStr.content)
    if response['responseStatus'] != 'SUCCESS':
        print('HTTP POST request to Vault\n' +
                    'ERROR while posting to URL "' + authUrl + '": ' +
                    response['errors'][0]['message'])
    else:
        sessionId = response['sessionId']
        print("Logged in using sessionId: " + sessionId)
    return sessionId

def vaultPut(url, params, sessionId, clientId):
    """
    Make an HTTP PUT request to Vault.

    :param url: the URL to PUT
    :param params: query parameters
    :param sessionId: vault sessionId
    :param clientId: client ID to track REST API Calls in logs

    :return: responseStr: JSON response string from Vault API call
    """
    authHeaders = vaultRequestHeaderFromSessionId(sessionId, clientId)
    responseStr = requests.put(url, params=params, headers=authHeaders)
    response = json.loads(responseStr.content)
    if response['responseStatus'] != 'SUCCESS':
        print('Update failed\nHTTP PUT request to Vault\n' +
                    'ERROR while putting to URL "' + url + '": ' +
                    response['errors'][0]['message'])
                    
    return responseStr

def vaultPutBulk(url, params, body, sessionId):
    """
    Make an HTTP PUT request to Vault.

    :param url: the URL to PUT
    :param params: query parameters
    :param body: update body
    :param sessionId: vault sessionId

    :return: responseStr: JSON response string from Vault API call
    """
    headers = {'Authorization': sessionId,'Content-Type': 'application/json','Accept': 'application/json'}
    responseStr = requests.put(url, params=params, json=body, headers=headers)
    response = json.loads(responseStr.content)
    if response['responseStatus'] != 'SUCCESS':
        print('Bulk update failed\nHTTP PUT request to Vault\n' +
                    'ERROR while putting to URL "' + url + '": ' +
                    response['errors'][0]['message'])
        
    return responseStr

def vaultPost(url, params, sessionId, clientId):
    """
    Make an HTTP POST request to Vault; die if there's an error.

    :param url: the URL to POST
    :param params: query parameters
    :param sessionId: vault sessionId
    :param clientId: client ID to track REST API Calls in logs

    :return: responseStr: JSON response string from Vault API call
    """

    authHeaders = vaultRequestHeaderFromSessionId(sessionId, clientId)
    responseStr = requests.post(url=url, params=params, headers=authHeaders)
    response = json.loads(responseStr.content)
    if response['responseStatus'] != 'SUCCESS':
        print('Update failed\nHTTP POST request to Vault\n' +
              'ERROR while putting to URL "' + url + '": ' +
              response['errors'][0]['message'])
    return responseStr

def vaultGet(url, sessionId, clientId):
    """
    Make an HTTP GET request to Vault; die if there's an error.

    :param url: the URL to GET
    :param sessionId: vault sessionId
    :param clientId: client ID to track REST API Calls in logs

    :return: responseStr: JSON response string from Vault API call
    """

    authHeaders = vaultRequestHeaderFromSessionId(sessionId, clientId)
    responseStr = requests.get(url=url, headers=authHeaders)
    response = json.loads(responseStr.content)
    if response['responseStatus'] != 'SUCCESS':
        print('Update failed\nHTTP GET request to Vault\n' +
              'ERROR while putting to URL "' + url + '": ' +
              response['errors'][0]['message'])
    return responseStr
    
def apiProcessedStatus(responseStr, burstLimit):
    """
    Determine whether the REST API call was processed successfully.

    :param responseStr: JSON response string from Vault API call
    :param burstLimit: Vault API burst limit cutoff

    :return: processedStatus: Whether the REST call was successful ('SUCCESS', 'FAILURE' or 'BURST_BREACH')
    """
    response = json.loads(responseStr.content)
    responseHeader = responseStr.headers
    processedStatus = ''
    if response['responseStatus'] != 'SUCCESS':
        processedStatus = VAULT_REST_API_FAILURE
    else:
        # If the burst limit remaining threshold is breached  
        # prevent further records being retreived from the queue
        apiBurstLimitRemaining = responseHeader['X-VaultAPI-BurstLimitRemaining']
        if (int(apiBurstLimitRemaining) <= int(burstLimit)):
            processedStatus = VAULT_REST_API_BURST_BREACH
        else:
            processedStatus = VAULT_REST_API_SUCCESS
    return processedStatus