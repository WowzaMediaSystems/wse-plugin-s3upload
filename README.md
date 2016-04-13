# ModuleS3Upload
The **ModuleS3Upload** module for [Wowza Streaming Engine™ media server software](https://www.wowza.com/products/streaming-engine) automatically uploads finished recordings to an Amazon S3 bucket. It uses the Amazon Web Services (AWS) SDK for Java to upload the recorded files.

## Prerequisites
Wowza Streaming Engine 4.0.0 or later is required.

The Amazon Web Services (AWS) SDK for Java is required to provide the upload functionality.

## Usage
When a recording is finished, a temporary file named **[recording-name].upload** is created to track the recording and sort any data that may be needed to resume the file upload later if it's interrupted. AWS TransferManager uploads the recorded file, splitting it into a multipart upload if required. After the recorded file is uploaded, the temporary **[recording-name].upload** file is deleted.

When the Wowza Streaming Engine application starts or restarts, the module checks to see if any interrupted uploads must be completed. Interrupted single part uploads are restarted from the beginning while interrupted multipart uploads are resumed from the last complete part. If the module is set to not resume uploads after interruptions (**s3UploadResumeUploads** = **false**), incomplete multipart uploads are deleted from the S3 bucket.

## More resources
[Wowza Streaming Engine Server-Side API Reference](https://www.wowza.com/resources/WowzaStreamingEngine_ServerSideAPI.pdf)

[How to extend Wowza Streaming Engine using the Wowza IDE](https://www.wowza.com/forums/content.php?759-How-to-extend-Wowza-Streaming-Engine-using-the-Wowza-IDE)

Wowza Media Systems™ provides developers with a platform to create streaming applications and solutions. See [Wowza Developer Tools](https://www.wowza.com/resources/developers) to learn more about our APIs and SDK.

To use the compiled version of this module, see [How to upload recorded media to an Amazon S3 bucket (ModuleS3Upload)](https://staging.wowza.com/forums/content.php?841-How-to-upload-recorded-media-to-an-Amazon-S3-bucket-%28ModuleS3Upload%29).

## Contact
[Wowza Media Systems, LLC](https://www.wowza.com/contact)

## License
This code is distributed under the [Wowza Public License](https://github.com/WowzaMediaSystems/wse-plugin-s3upload/blob/master/LICENSE.txt).
