# Vault Java SDK - vsdk-spark-external-aws-sample

**Please see the [project wiki](https://github.com/veeva/vsdk-spark-external-aws-sample/wiki) for a detailed walkthrough.**

The vsdk-spark-external-aws-sample project contains an example of how to send a spark messages from a vault to external Amazon Web Services (AWS) SQS queue for processing. The project will step through:

* Creation of an sample 3rd party AWS Application 
    * Amazon API Gateway - point of entry from Vault to AWS
    * Amazon Simple Queue Service - store Spark messages in AWS for processing
    * Amazon Lambda - verify, enqueue and process Spark messages
* Vault
    * External messaging - send a simple message from a source vault to AWS for processing

**An AWS account will be required to implement this example.**

## How to import

Import as a Maven project. This will automatically pull in the required Vault Java SDK dependencies. 

For Intellij this is done by:
- File -> Open -> Navigate to project folder -> Select the 'pom.xml' file -> Open as Project

For Eclipse this is done by:
- File -> Import -> Maven -> Existing Maven Projects -> Navigate to project folder -> Select the 'pom.xml' file
	    
## License

This code serves as an example and is not meant for production use.

Copyright 2019 Veeva Systems Inc.
 
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
 
    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.