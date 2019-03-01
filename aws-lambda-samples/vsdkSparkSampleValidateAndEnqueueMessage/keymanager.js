'use strict';
const https = require('https');
const querystring = require('querystring');

//
// Retrieve Environment variables
//
const vaultHostname = process.env.VAULT_HOSTNAME; // e.g. 'spark-demo.vaultdev.com'
const vaultIntegrationUser = process.env.VAULT_USER // e.g. 'admin@spark-demo.com'
const vaultIntegrationPassword = process.env.VAULT_PASSWORD // e.g. 'your password'

//
// Private Methods
//
var getSessionId = async function(hostname, username, password) {
    
    // Build the post string from an object
    var postdata = querystring.stringify({
      'username': username,
      'password': password
    });

    var options = { method: 'POST',
      host: hostname,
      path: '/api/auth',
      headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(postdata)
      }
    };
    
    let response = await makePostRequest(options, postdata);
    var responseStatus = response["responseStatus"];
    if (responseStatus == 'SUCCESS') {
        return response["sessionId"];
    } else {
        // Enter error handling based on your method used in AWS
        console.log("Error obtaining Session Id for user '" + username + "' in oder to retrieve PEM file for configuring verification");
        return '';
    }

}

var getPublicKey = async function(sessionId,certId,hostname) {
    
    let certPath = '/api/v18.3/services/certificate/'+certId;
    console.log("certPath: " + certPath);

    var options = { method: 'GET',
      host: hostname,
      path: certPath,
      headers: {
          'authorization': sessionId
      }
    };
    
    let response = await makeGetRequest(options, '');
    return response;
}

var makeGetRequest = function(options, postdata) {
    return new Promise((resolve, reject) => {
        const req = https.request(options, (res) => {
            const chunks = [];
            var returnData = "";    
            res.on("data", function (d) {
                returnData += d;
            });
            res.on('end', () => {
                resolve(returnData);
            });
        });
    
        req.on('error', (e) => {
            reject(e.message);
        });

        // Send the request
        if(postdata) {
            req.write(postdata);
        } else {
            req.write('');
        }
        req.end();
    });
}

var makePostRequest = function(options, postdata) {
    return new Promise((resolve, reject) => {
        const req = https.request(options, (res) => {
            const chunks = [];
            var returnData = "";    
            res.on("data", function (d) {
                returnData += d;
            });
            res.on('end', () => {
                resolve(JSON.parse(returnData));
            });
        });
    
        req.on('error', (e) => {
            reject(e.message);
        });

        // Send the request
        if(postdata) {
            req.write(postdata);
        } else {
            req.write('');
        }
        req.end();
    });
}

//
// Public Methods
//
module.exports = {
    pemFileExists: async function(certificateId) {
        let fs = require("fs");
        if(!fs.existsSync(__dirname + '/PublicKeys/' + certificateId + '.pem')) {
            console.log("PEM file not found");
            
            let sessionId = await getSessionId(vaultHostname, vaultIntegrationUser, vaultIntegrationPassword);
            if(sessionId) {
                let publicKey = await getPublicKey(sessionId,certificateId,vaultHostname);
                
                // Write details of the PEM file to be created to the Cloudwatch logs
                console.log("Certificate to be added to '/PublicKeys/" + certificateId + ".pem':");
                console.log(publicKey);
            }
            return false;
        } else {
            return true; 
        }
    }
};
