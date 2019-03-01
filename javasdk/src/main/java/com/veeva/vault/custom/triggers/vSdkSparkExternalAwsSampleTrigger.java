package com.veeva.vault.custom.triggers;

import com.veeva.vault.sdk.api.core.*;
import com.veeva.vault.sdk.api.data.*;
import com.veeva.vault.sdk.api.queue.Message;
import com.veeva.vault.sdk.api.queue.PutMessageResponse;
import com.veeva.vault.sdk.api.queue.QueueService;
import com.veeva.vault.sdk.api.core.VaultCollections;
import java.util.List;

/**
 * vSDK SPARK AWS Queue Sample Trigger
 *
 * This trigger is pushes new Loan Approval records created in Vault to an
 * external Sample AWS Finance system for processing via an external Vault Queue
 */
@RecordTriggerInfo(object = "vsdk_loan_approval__c", events = {RecordEvent.AFTER_INSERT})
public class vSdkSparkExternalAwsSampleTrigger implements RecordTrigger {

    public void execute(RecordTriggerContext recordTriggerContext) {

        LogService logService = ServiceLocator.locate(LogService.class);
        String event = "trigger '" + recordTriggerContext.getRecordEvent().toString() + "'";
        List vaultIds = VaultCollections.newList();
        for (RecordChange triggerRecord : recordTriggerContext.getRecordChanges()) {

            if (vaultIds.size() < 500) {
                vaultIds.add(triggerRecord.getNew().getValue("id", ValueType.STRING));
            } else {
                moveMessagesToQueue( "vsdk_aws_queue_sample__c",
                        "vsdk_aws_queue_sample_api_gateway",
                        "vsdk_loan_approval__c",
                        event,
                        vaultIds);
                vaultIds.clear();
            }
        }

        if (vaultIds.size() > 0){
            moveMessagesToQueue( "vsdk_aws_queue_sample__c",
                    "vsdk_aws_queue_sample_api_gateway",
                    "vsdk_loan_approval__c",
                    event,
                    vaultIds);
        }
    }

    // Move to the Spark Queue AFTER the record has successfully been inserted.
    public void moveMessagesToQueue(String queueName, String connectionName, String objectName, String recordEvent, List vaultIds) {

        QueueService queueService = ServiceLocator.locate(QueueService.class);
        LogService logService = ServiceLocator.locate(LogService.class);
        RecordService recordService = ServiceLocator.locate(RecordService.class);
        List<Record> recordList = VaultCollections.newList();
        Message message = queueService.newMessage(queueName)
                .setAttribute("object", objectName)
                .setAttribute("event", recordEvent)
                .setAttributeWithToken("sessionId", "${Session.SessionId}")
                .setMessageItems(vaultIds)
                .appendPath(connectionName, "/message");
        PutMessageResponse response = queueService.putMessage(message);

        //Check that the message queue successfully processed the message.
        //If it's successful, change the `approval_status__c` flag to 'pending_loan_approval__c'.
        //If there is an error, change the `approval_status__c` flag to 'send_for_approval_failed__c'.
        if (response.getError() != null) {
            logService.info("ERROR Queuing Failed: " + response.getError().getMessage());

            for (Object vaultId : vaultIds) {
                Record recordUpdate = recordService.newRecordWithId(objectName, vaultId.toString());
                recordUpdate.setValue("approval_status__c", VaultCollections.asList("send_for_approval_failed__c"));
                recordList.add(recordUpdate);
            }
        }
        else {
            for (Object vaultId : vaultIds) {
                Record recordUpdate = recordService.newRecordWithId(objectName, vaultId.toString());
                recordUpdate.setValue("approval_status__c", VaultCollections.asList("pending_loan_approval__c"));
                recordList.add(recordUpdate);
            }
        }

        //If a subsequent error occurs save the record change, raise an 'OPERATION_NOT_ALLOWED'
        //error through the Vault UI.
        if (recordList.size() > 0) {
            recordService.batchSaveRecords(recordList)
                .onErrors(batchOperationErrors -> {

                    //Iterate over the caught errors.
                    //The BatchOperation.onErrors() returns a list of BatchOperationErrors.
                    //The list can then be traversed to retrieve a single BatchOperationError and
                    //then extract an **ErrorResult** with BatchOperationError.getError().
                    batchOperationErrors.stream().findFirst().ifPresent(error -> {
                        String errMsg = error.getError().getMessage();
                        int errPosition = error.getInputPosition();
                        String name = recordList.get(errPosition).getValue("name__v", ValueType.STRING) + " " +
                                      recordList.get(errPosition).getValue("surname__c", ValueType.STRING);
                        throw new RollbackException("OPERATION_NOT_ALLOWED", "Unable to create '" +
                                recordList.get(errPosition).getObjectName() + "' record: '" +
                                name + "' because of '" + errMsg + "'.");
                    });
                })
                .execute();
        }
    }
}