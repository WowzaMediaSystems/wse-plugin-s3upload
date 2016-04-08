# ModuleS3Upload
The **ModuleS3Upload** module for [Wowza Streaming Engine™ media server software](https://www.wowza.com/products/streaming-engine) automatically uploads finished recordings to an Amazon S3 bucket. It uses the Amazon Web Services (AWS) Java SDK to upload the recorded files.

## Prerequisites
Wowza Streaming Engine 4.0.0 or later is required.

S3 Upload requires the AWS Java SDK to provide the upload functionality. Download the [AWS SDK for Java](http://aws.amazon.com/sdk-for-java/), unzip it, and then copy **lib/aws-java-sdk-xx.xx.xx.jar** to the **lib** folder in the Wowza Streaming Engine installation (**[install-dir]/lib/**).

## Installation
Copy the **wse-plugin-s3upload.jar** file to your Wowza Streaming Engine **[install-dir]/lib/** folder.

## Configuration
To enable this module, add the following module definition to your application configuration. See [Configure modules](https://www.wowza.com/forums/content.php?625-How-to-get-started-as-a-Wowza-Streaming-Engine-Manager-administrator#configModules) for details.

**Name** | **Description** | **Fully Qualified Class Name**
-----|-------------|---------------------------
ModuleS3Upload | Uploads recordings to Amazon S3. | com.wowza.wms.plugin.s3upload.ModuleS3Upload

## Properties
After enabling the module, you can adjust the default settings by adding the following properties to your application. See [Configure properties](https://www.wowza.com/forums/content.php?625-How-to-get-started-as-a-Wowza-Streaming-Engine-Manager-administrator#configProperties) for details.

**Path** | **Name** | **Type** | **Value** | **Notes**
-----|------|------|-------|------
Root/Application | s3UploadAccessKey | String | [your-s3-access-key] | The S3 access key for your AWS account. (default: not set)
Root/Application | s3UploadSecretKey | String | [your-s3-secret-key] | The S3 secret key for your AWS account. (default: not set)
Root/Application | s3UploadBucketName | String | [your-s3-bucket name] | The S3 bucket that you'll upload the files to. (default: not set)
Root/Application | s3UploadResumeUploads | Boolean | true | Specifies if interrupted file uploads should resume after a restart. (default: **true**)
Root/Application | s3UploadDeletOriginalFiles | Boolean | false | Specifies if the original files should be deleted after uploading. (default: **false**)

## Usage
When a recording is finished, a temporary file named **[recording-name].upload** is created to track the recording and sort any data that may be needed to resume the file upload later if it's interrupted. AWS TransferManager uploads the recorded file, splitting it into a multipart upload if required. After the recorded file is uploaded, the temporary **[recording-name].upload** file is deleted.

When the Wowza Streaming Engine application starts or restarts, the module checks to see if any interrupted uploads must be completed. Interrupted single part uploads are restarted from the beginning while interrupted multipart uploads are resumed from the last complete part. If the module is set to not resume uploads after interruptions (**s3UploadResumeUploads** = **false**), incomplete multipart uploads are deleted from the S3 bucket.

## API Reference
[Wowza Streaming Engine Server-Side API Reference](http://www.wowza.com/resources/WowzaStreamingEngine_ServerSideAPI.pdf)

## Contact
Wowza Media Systems, LLC

Wowza Media Systems provides developers with a platform to create streaming applications and solutions. See [Wowza Developer Tools](https://www.wowza.com/resources/developers) to learn more about our APIs and SDK.

## License
This code is distributed under the [Wowza Public License](https://github.com/WowzaMediaSystems/wse-plugin-s3upload/blob/master/LICENSE.txt).
