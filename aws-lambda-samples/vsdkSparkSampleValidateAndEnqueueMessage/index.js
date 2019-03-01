'use strict'
const crypto = require('crypto');
var keymanager = require('./keymanager.js');

//
// Retrieve Environment variables
//
const queueURL = process.env.VAULT_SAMPLE_SQS_QUEUE_URL; // e.g. 'https://sqs.us-east-2.amazonaws.com/294734476466/vsdk-spark-sample-external-queue'


//
// Main Handler Method
//
exports.handler = async function(event, context, callback) {
  try {
    //  console.log('Received event:', JSON.stringify(event, null, 2));

    //
    // 1. Retrieve request headers and body from the Lambda function input.
    //
    //    Note: To pass the headers and body through the API Gateway's API in AWS  
    //          it's necessary to create a mapping template of type application/json
    //          for the integration request. The mapping template code will need
    //          to take the form...
    //          {
    //              "method": "$context.httpMethod",
    //              "body" : $input.json('$'),
    //              "headers": {
    //                  #foreach($param in $input.params().header.keySet())
    //                  "$param": "$util.escapeJavaScript($input.params().header.get($param))"
    //                  #if($foreach.hasNext),#end
    //                  #end
    //              }
    //          }
    //
    let headers = event.headers;
    let body = event.body;

    // 2. Check for existance of Public Key (PEM) to validate the request against
    //
    //    Make sure that the PEM file corresponding with the key in the header 
    //    exists locally in the '/PublicKeys' folder. If not details will be 
    //    written to the cloudwatch event logs, for it to be created manually.
    //    
    //    Until that time an error will be returned and the spark messages won't be
    //    added to the queue.
    let certificateId = headers["X-VaultAPISignature-CertificateId"];
    let pubKeyExists = await keymanager.pemFileExists(certificateId);
    if(pubKeyExists == false) {
      let response = {
      "statusCode": "403",
      "headers": {
          "Content-Type": "application/json"
      },
      "body": "Invalid Key"
    };
    callback(null,response);
    }


    //
    // 3. Validate the message was sent from Vault
    //
    let isValidMessageProm = validate(headers,body);

    //
    // 4. Send the record to the SQS Queue, if validated successfully
    //
    if (isValidMessageProm) {
      enqueueMessage(queueURL,body);
    }

    //
    // 5. Return a response message
    //
    let response = {
      "statusCode": isValidMessageProm ? "200" : "403",
      "headers": {
          "Content-Type": "application/json"
      },
      "body": isValidMessageProm ? "Success" : "Forbidden"
    };
    callback(null,response);

  } catch (error) {
      console.log(error.message);
      console.log(error.type);
      console.log(error.body);
      let response = {
        "statusCode": "500",
        "headers": {
            "Content-Type": "application/json"
        },
        "body": "Internal Service Error"
      };
      callback(null, response);
  }

};

//
// Helper Functions
//

function enqueueMessage(queueUrl,body)
/**
 * Send the record to the SQS Queue 
 * 
 * :param queueUrl: the URL for the SQS queue to place messages on
 * :param body: the message body to place on the queue
 * 
 * :return: none
 * 
 */
{
  let queueName = queueUrl.split("/")[4];
  console.log('Enqueuing Spark Message to queue "' + queueName + '"');

  let AWS = require('aws-sdk');
  let queueRegion = queueURL.split(".")[1];
  AWS.config.update({region: queueRegion});  // e.g. 'us-east-2'
  // Create an SQS service object
  let sqs = new AWS.SQS({apiVersion: '2012-11-05'});
  // Construct the message parameters
  let bodyString = JSON.stringify(body, null, 2);
  let params = {
    DelaySeconds: 10,
    MessageBody: bodyString,
    QueueUrl: queueURL
  };

  sqs.sendMessage(params, function(err, data) {
    if (err) {
      console.log("Error", err);
    } else {
      console.log("Success", data.MessageId);
    }
  });
}

 
function validate(headers,body)
/**
 * Validate the request header and body has come from Vault
 * 
 * :param headers: the request headers
 * :param body: the request body
 * 
 * :return: verified: true/false whether the request has come from Vault
 * 
 */
{

  // 1.  Prepare string-to-sign format to mimic the way it is
  //     done in Vault
  let stringToSign = prepareDataToSign(headers,body);
  
  // 2.  Get Public Certification key from file system, which in turn was retrieved from Vault.
  //     Note: The public key is available from Vault using the REST API
  //           https://{{server}}/api/v19.1/services/certificate/
  //             {{Value of headers['X-VaultAPI-CertificateID'], i.e. 00001}} 
  //
  let fs = require("fs");
  let publicKeyFilename = headers["X-VaultAPISignature-CertificateId"];
  let pubKeyPem = fs.readFileSync(__dirname + '/PublicKeys/' + publicKeyFilename + '.pem');

  // 2. Use Signature coming in from Headers to Verify
  let signature = headers['X-VaultAPI-Signature'];
  let verified = validateSignUsingCrypto(stringToSign,signature,pubKeyPem);
  return verified;
}


function prepareDataToSign(headers, body)
/**
 * Prepare a String-To-Sign in the same way it is done in Vault
 * 
 * :param headers: the request headers
 * :param body: the request body
 * 
 * :return: stringToSign: the string to be signed as part of the verification
 * 
 */
{
  // The string-to-sign shall contain the following data:
  // - All “X-VaultAPISignature-***” headers in the request in the following format:
  //   Lowercase(<HeaderName1>)+":"+Trim(<value>)+"\n"
  //   Lowercase(<HeaderName2>)+":"+Trim(<value>)+"\n"
  //   Where: 
  //    - Each header name is in lowercase
  //    - Each header name and value pairs are separated by the newline character ("\n")
  //    - Each value must not contain any spaces
  //    - All the header names are sorted alphabetically
  // - The raw text of the message json object in the HTTP body must be appended to the 
  //   header values
  
  let headersArray = [];
  Object.keys(headers).filter(propertyName => {
    return propertyName.indexOf("X-VaultAPISignature-") === 0;})
    .sort().forEach(key => {
    headersArray.push(key.toLowerCase()+":"+headers[key].trim()+"\n");
  });
  let stringToSign = headersArray.join("") + JSON.stringify(body);
  return stringToSign;
}

function validateSignUsingCrypto(stringToSign,signature,pubKeyPem)
/**
 * Validate the string-to-sign using the public key with Crypto
 * 
 * :param stringToSign: local string to sign
 * :param signature: the request signature from Vault
 * :param pubKeyPem: the Vault public PEM key required for verification
 * 
 * :return: verified: true/false whether the request has come from Vault
 * 
 */
{
  console.log('validateSignUsingCrypto starts');
  const hashAlgo = 'RSA-SHA256';

  // Verify the string-to-sign against the signature with the public key
  const verifier = crypto.createVerify(hashAlgo);
  verifier.update(stringToSign);
  const verified = verifier.verify(pubKeyPem, signature, 'base64');
  console.log('validateSignUsingCrypto=' + verified);
  return verified;
}