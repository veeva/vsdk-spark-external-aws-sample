package com.veeva.vault;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import okhttp3.*;
import org.apache.log4j.Logger;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import java.io.*;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.*;

public class LambdaHandler implements RequestHandler<APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse> {
    private static OkHttpClient client = new OkHttpClient(); // Use OKHTTP to make call to Vault
    private static ObjectMapper objectMapper = new ObjectMapper(); // Jackson ObjectMapper; used to deserialize json
    private static Logger logger = Logger.getLogger(LambdaHandler.class);
    private static Regions clientRegion = Regions.US_EAST_2;
    private static AmazonS3 s3Client = AmazonS3ClientBuilder
            .standard()
            .withRegion(clientRegion)
            .build();

    private static final AmazonSQS SQS = AmazonSQSClientBuilder.defaultClient();
    private static final String BUCKET_NAME =  System.getenv("BUCKET_NAME"); // Lambda Environment variable
    private static final String SQS_URL = System.getenv("VAULT_SAMPLE_SQS_QUEUE_URL");
    private static final int SQS_DELAY = 10; // 10 Seconds
    private static final String API_AUTH_ENDPOINT = "/api/v21.1/auth";
    private static final String API_RETRIEVE_SIGNING_CERTIFICATE_ENDPOINT = "/api/v21.1/services/certificate/";

    @Override
    public APIGatewayV2HTTPResponse handleRequest(APIGatewayV2HTTPEvent event,  Context context) {

        // Extract request body and headers
        String body = event.getBody();
        logger.info("Body: " + body);
        Map<String, String> headers = event.getHeaders();
        logger.info("Headers: " + event.getHeaders());

        APIGatewayV2HTTPResponse response = new APIGatewayV2HTTPResponse();

        // Get the certificateId from the headers
        String certificateId = headers.get("X-VaultAPISignature-CertificateId");
        if(certificateId == null) certificateId =  headers.get("x-vaultapisignature-certificateid");
        logger.info("Certificate Id: " + certificateId);

        // Find certificate corresponding to certificateId
        // If not found, write certificate to cloudwatch logs and add to s3 bucket
        String certificatePath = new StringBuilder("PublicKeys/")
                .append(certificateId)
                .append(".pem")
                .toString();
        byte[] certificateFile = getCertificateFile(certificateId, certificatePath);
        logger.info(certificateFile);

        // If Certificate file is null(doesn't exist on s3 and not returned by vault)
        if(certificateFile == null) {
            response.setStatusCode(403);
            response.setBody("Invalid Key");
            return response;
        }

        String pemKey = new String(certificateFile)
                .replace("-----BEGIN CERTIFICATE-----", "")
                .replaceAll("\\R", "")
                .replace("-----END CERTIFICATE-----", "");

        // Decodes key from base 64
        byte[] decoded = Base64.getMimeDecoder().decode(pemKey.getBytes());
        InputStream certStream = new ByteArrayInputStream(decoded);

        // Get Public Key from certificate
        PublicKey publicKey = null;
        try {
            Certificate certificate = CertificateFactory.getInstance("X.509").generateCertificate(certStream);
            publicKey = certificate.getPublicKey();
        } catch (CertificateException e) {
            logger.info(e.getMessage());
        }

        logger.info(publicKey);

        // Valid Spark Message
        boolean isValidMessage = validate(headers, body, publicKey);

        // If spark message is valid, send to sqs queue for further processing
        // If not, throw error
        if(isValidMessage) {
            // Enqueue Message
            enqueueMessage(body);
            response.setStatusCode(200);
            response.setBody("SUCCESS");
        } else {
            response.setStatusCode(500);
            response.setBody("FAILURE");
        }
        return response;
    }

    /*
     * getCertificateFile returns a certificate(PEM) file if it exists on s3 or the returned PEM file from the Vault request
     * @param certificateId, the certificate id which matches the certificate file's name
     * @param certificatePath, a string value for the presumed path of the pem file on s3
     * @returns certificateFile, a byte array that returns the contents of the pem file(certificate) from s3 or vault
     */
    public byte[] getCertificateFile(String certificateId, String certificatePath) {
        byte[] certificateFile = null;

        if(s3Client.doesObjectExist(BUCKET_NAME, certificatePath)) {
            // Use BUCKET_NAME an certificatePath to get certificate(pem) file as string
            String fileData = s3Client.getObjectAsString(BUCKET_NAME, certificatePath);
            logger.info(BUCKET_NAME);
            logger.info(certificatePath);
            logger.info(fileData);
            certificateFile = fileData.getBytes();
            return certificateFile;
        } else { // Get from vault
            // Get vault user, password and hostname
            String username = System.getenv("VAULT_USER");
            String password = System.getenv("VAULT_PASSWORD");
            String hostname = System.getenv("VAULT_HOSTNAME");

            // Make auth call to vault and get session id
            // Makes GET /api/{version}/auth endpoint call
            String sessionId = getSessionId(username, password, hostname);
            if(sessionId != null) {
                // Make Retrieve Signing Certificate API call
                // GET /api/{version}/services/certificate/{cert_id}
                String publicKey = getPublicKey(sessionId, certificateId, hostname);
                logger.info("Certificate to be added to " + BUCKET_NAME);

                // Add certificate retrieved from vault to s3
                s3Client.putObject(BUCKET_NAME, certificatePath, publicKey);
                logger.info(publicKey);
                return publicKey.getBytes();
            }
            return certificateFile;
        }
    }

    /*
     * getSessionId makes a basic auth call to Vault HTTPS REST API and returns an active sessionId
     * @param username, the username for the vault user
     * @param password, the password for the vault user
     * @param hostname, the vault hostname
     * @returns sessionId, an active sessionId for the vault
     */
    public String getSessionId(String username, String password, String hostname) {
        String url = new StringBuilder(hostname).append(API_AUTH_ENDPOINT).toString();

        RequestBody requestBody = new MultipartBody.Builder()
                .setType(MultipartBody.FORM)
                .addFormDataPart("username", username)
                .addFormDataPart("password", password)
                .build();

        Request request = new Request.Builder()
                .url(url)
                .addHeader("Content-Type", "application/x-www-form-urlencoded")
                .post(requestBody)
                .build();

        try (Response response = client.newCall(request).execute()) {
            // If the API call is successful then get the session id from the json returned
            if(response.isSuccessful()) {
                ObjectNode responseJSON = objectMapper.readValue(response.body().string(), ObjectNode.class);
                return responseJSON.get("sessionId").toString();
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return null;
    }

    /*
     * getPublicKey makes a REST API call to the Vault HTTPS REST API to retrieve the certificate/pem file
     * @param sessionId, an active sessionId for the vault
     * @param certificateId, the id of the certificate/pem to be retrieved
     * @param hostname, the vault hostname
     * @returns certificate, the certificate retrieved from the vault
     */
    public String getPublicKey(String sessionId, String certificateId, String hostname) {
        String certPath = new StringBuilder(hostname).append(API_RETRIEVE_SIGNING_CERTIFICATE_ENDPOINT).append(certificateId).toString();

        Request request = new Request.Builder()
                .url(certPath)
                .addHeader("authorization", sessionId)
                .get()
                .build();

        try (Response response = client.newCall(request).execute()) {
            if(response.isSuccessful()) { // If successful, return the certificate
                return response.body().string();
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return null;
    }

    /*
     * validate validates the message sent to the lambda function using the public key
     * @param headers, the map containing the headers received
     * @param body, the body received as a json string
     * @param publicKey, the public key used to verify the message
     * @returns verified, an boolean representing whether the spark message was valid
     */
    public boolean validate(Map<String,String> headers, String body, PublicKey publicKey) {
        String stringToVerify = null;

        // Create the String-to-Verify
        stringToVerify = prepareDataToVerify(headers, body);

        // Get the X-Vault-API-SignatureV2
        String xVaultAPISignature = "";
        if(headers.get("X-VaultAPI-SignatureV2") != null) {
            xVaultAPISignature = headers.get("X-VaultAPI-SignatureV2");
        } else if (headers.get("x-vaultapi-signaturev2") != null) {
            xVaultAPISignature = headers.get("x-vaultapi-signaturev2");
        }
        logger.info("Signature: " + xVaultAPISignature);

        // Verify the spark message
        boolean verified = verifySignUsingCrypto(stringToVerify,xVaultAPISignature, publicKey);
        logger.info("Verified: " + verified);
        return verified;
    }

    /*
     * prepareDataToVerify prepares the string-to-verify/string-to-sign
     * @param headers, the map containing the headers received
     * @param body, the body received as a json string
     * @returns stringToVerify, the string used to verify the spark message
     */
    public String prepareDataToVerify(Map<String,String> headers, String body) {

        // Create set of X-VaultAPISignature-* headers
        Set<String> filteredHeaderSet = new HashSet<>();
        for(String key: headers.keySet()){
            if(key.indexOf("X-VaultAPISignature-") == 0 || key.indexOf("x-vaultapisignature-") == 0) {
                filteredHeaderSet.add(key);
            }
        }
        // Sort X-VaultAPISignature-* headers alphabetically
        String[] headerKeysArray = filteredHeaderSet.stream().toArray(String[]::new);
        Arrays.sort(headerKeysArray);

        // Convert X-VaultAPISignature-* headers to Lowercase(<HeaderName1>)+":"+Trim(<value>)+"\n" format
        int headerKeysArrayLength = headerKeysArray.length;
        for(int i = 0; i < headerKeysArrayLength; i++) {
            String key = headerKeysArray[i];
            headerKeysArray[i] = key.toLowerCase() + ":" + headers.get(key).trim() + "\n";
        }

        /*
        * The String-to-verify must be in the following format:
        * All X-VaultAPISignature-* headers in the request must be in the following format: Lowercase(<HeaderName1>)+":"+Trim(<value>)+"\n"
        * Each header name-value pair must be separated by the newline character (\n)
        * Header names must be in lower case
        * Header name-value pairs must not contain any spaces
        * Header names must be sorted alphabetically
        * The JSON object in the HTTP body of the request must be raw text
        * Add a newline character after the HTTP body, followed by the full HTTPS URL as received by your external service.
        * Make sure this also includes any query parameters.
         */
        StringBuilder stringToVerifySB = new StringBuilder(String.join("", headerKeysArray))
                .append(body)
                .append("\n");
        if (headers.get("X-VaultAPISignature-URL") != null) {
            stringToVerifySB.append(headers.get("X-VaultAPISignature-URL"));
        } else if(headers.get("x-vaultapisignature-url") != null) {
            stringToVerifySB.append(headers.get("x-vaultapisignature-url"));
        }

        logger.info(stringToVerifySB.toString());
        return stringToVerifySB.toString();
    }

    /*
     * verifySignUsingCrypto verifies the spark message
     * @param stringToVerify, the string that will be used to verify the spark message
     * @param xVaultAPISignature, the signature of the message
     * @param pubKey, the public key used to verify the message
     * @returns verified, a boolean value indicating whether the spark message was valid
     */
    public boolean verifySignUsingCrypto(String stringToVerify, String xVaultAPISignature, PublicKey pubKey) {
        boolean verified = false;
        try {
            // Create signature using the public key and the string to verify
            Signature signature = Signature.getInstance("SHA256withRSA");
            signature.initVerify(pubKey);
            signature.update(stringToVerify.getBytes());
            // Verify that the string to verify matches the signature received in the headers
            verified = signature.verify(Base64.getMimeDecoder().decode(xVaultAPISignature.getBytes()));
        } catch (NoSuchAlgorithmException | InvalidKeyException | SignatureException e) {
            logger.error(e.getMessage());
        }
        return verified;
    }

    /*
     * enqueueMessage, pushes the message to a SQS queue
     * @param body, a string SQS message
     */
    public void enqueueMessage(String body) {
        logger.info("Enqueuing Spark Message to queue: " + SQS_URL);
        SendMessageRequest sendMessageRequest = new SendMessageRequest()
                .withQueueUrl(SQS_URL)
                .withMessageBody(body)
                .withDelaySeconds(SQS_DELAY);
        SQS.sendMessage(sendMessageRequest);
    }

}