# wse-plugin-s3upload

S3 Upload is a Wowza Streaming Engine module will automatically upload completed recordings to an Amazon S3 bucket. It uses the AWS Java SDK to complete the uploads.

## Prerequisites

S3 Upload requires the AWS Java SDK to provide the upload functionality.  Download the [AWS SDK for Java](http://aws.amazon.com/sdk-for-java/), unzip it and copy `lib/aws-java-sdk-xx.xx.xx.jar` to your Wowza Streaming Engine `[install-dir]/lib/ folder`. 

##Installation

Copy `wse-plugin-s3upload.jar` to your Wowza Streaming Engine `[install-dir]/lib/`

## Configuration

Add the following Module Definition to your Application configuration. See [Configure modules](http://www.wowza.com/forums/content.php?625-How-to-get-started-as-a-Wowza-Streaming-Engine-Manager-administrator#configModules) for details.

Name | Description | Fully Qualified Class Name
-----|-------------|---------------------------
ModuleS3Upload | Upload recordings to Amazon S3. | com.wowza.wms.plugin.s3upload.ModuleS3Upload

## Properties

Adjust the default settings by adding the following properties to your application. See [Configure properties](http://www.wowza.com/forums/content.php?625-How-to-get-started-as-a-Wowza-Streaming-Engine-Manager-administrator#configProperties) for details.

Path | Name | Type | Value | Notes
-----|------|------|-------|------
Root/Application | s3UploadAccessKey | String | [your-s3-access-key] | The S3 Access key for your AWS Account (default: not set).
Root/Application | s3UploadSecretKey | String | [your-s3-secret-key] | The S3 Secret key for your AWS Account (default: not set).
Root/Application | s3UploadBucketName | String | [your-s3-bucket name] | The S3 Bucket that you are going to upload the files to (default: not set).
Root/Application | s3UploadResumeUploads | Boolean | true | Should S3 Upload resume any interrupted files after a restart (default: true).
Root/Application | s3UploadDeletOriginalFiles | Boolean | false | Should S3 Upload delete the original files after uploading (default: false).

## Usage

When a recording is complete, a temporary file (recording-name.upload) is created to track the recording and sort any data that may be needed to resume the upload later if it's interrupted. The AWS TransferManager is then used to complete the upload, splitting the upload into a Multipart Upload if required. When the upload is complete, the temporary file is deleted.
When the Wowza application starts or restarts, S3 Upload will check to see if there are any existing uploads that need to be completed. Any single part uploads that didn't originally complete will be restarted from the beginning and any Multipart uploads will be resumed from the last complete part. If the module is set to not resume uploads, any incomplete Multipart uploads will be deleted from the S3 bucket

## API Reference

[Wowza Streaming Engine Server-Side API Reference](http://www.wowza.com/resources/WowzaStreamingEngine_ServerSideAPI.pdf)

## Contact
Wowza Media Systems, LLC
Wowza Media Systems provides developers with a platform to create streaming applications and solutions. See [Wowza Developer Tools](https://www.wowza.com/resources/developers) to learn more about our APIs and SDK.
(Note this web page is being re-done and will be landing page for all developer resources).

## License

TODO: Add legal text here or LICENSE.txt file

