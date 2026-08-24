# S3Upload
The **ModuleS3Upload** module for [Wowza Streaming Engine™ media server software](https://www.wowza.com/products/streaming-engine) automatically uploads finished recordings to an S3 bucket — Amazon S3 or any S3-compatible object storage service (Backblaze B2, MinIO, Wasabi, etc.). It uses the Amazon Web Services (AWS) SDK for Java to upload the recorded files.

This repo includes a [compiled version](/lib/wse-plugin-s3upload.jar).

## Prerequisites
Wowza Streaming Engine™ 4.9.0 or later is required. The module is built with Java 17, which is supported by Wowza Streaming Engine 4.9.0 and later.

The [AWS SDK for Java](https://aws.amazon.com/sdk-for-java/) files that the module depends on are included with each [release](https://github.com/WowzaMediaSystems/wse-plugin-s3upload/releases), so there's nothing else to download. Copy the module jar and the accompanying SDK jars into the Wowza Streaming Engine `lib` folder. Only the SDK jars that Wowza Streaming Engine doesn't already ship with are included.

## Usage
When a recording is finished, a temporary file named **[recording-name].upload** is created to track the recording and sort any data that may be needed to resume the file upload later if it's interrupted. AWS TransferManager uploads the recorded file, splitting it into a multipart upload if required. After the recorded file is uploaded, the temporary **[recording-name].upload** file is deleted.

When the Wowza Streaming Engine application starts or restarts, the module checks to see if any interrupted uploads must be completed. Interrupted single part uploads are restarted from the beginning while interrupted multipart uploads are resumed from the last complete part. If the module is set to not resume uploads after interruptions (**s3UploadResumeUploads** = **false**), incomplete multipart uploads are deleted from the S3 bucket.

## S3-compatible services
To upload to an S3-compatible service instead of Amazon S3, set **s3UploadEndpoint** to the service's S3-compatible endpoint. The module then targets that endpoint directly instead of resolving an AWS region. If the region can't be derived from the endpoint host (`s3.<region>.<host>`), set **s3UploadRegion** to the signing region the service expects; otherwise `us-east-1` is used.

Set **s3UploadPathStyleAccess** to **true** for services that address buckets by path rather than by hostname (for example, a self-hosted MinIO server).

Example: Backblaze B2 (credentials are a B2 application key ID and application key):

```xml
<Property><Name>s3UploadEndpoint</Name><Value>https://s3.us-west-004.backblazeb2.com</Value></Property>
<Property><Name>s3UploadBucketName</Name><Value>my-recordings</Value></Property>
<Property><Name>s3UploadAccessKey</Name><Value>004xxxxxxxxxxxx0000000001</Value></Property>
<Property><Name>s3UploadSecretKey</Name><Value>K004xxxxxxxxxxxxxxxxxxxxxxxxxxx</Value></Property>
```

**s3UploadProfile** and **s3UploadProfilePath** are provider-neutral aliases for **s3UploadAwsProfile** and **s3UploadAwsProfilePath**; all credential options work with any provider because they use the standard SDK credentials file and provider chain.

## More resources
To use the compiled version of this module, see [How to upload recorded media to an Amazon S3 bucket (S3Upload)](https://www.wowza.com/docs/how-to-upload-recorded-media-to-an-amazon-s3-bucket-modules3upload).

[Wowza Streaming Engine Server-Side API Reference](https://www.wowza.com/resources/serverapi/)

[How to extend Wowza Streaming Engine using the Wowza IDE](https://www.wowza.com/docs/how-to-extend-wowza-streaming-engine-using-the-wowza-ide)

Wowza Media Systems™ provides developers with a platform to create streaming applications and solutions. See [Wowza Developer Tools](https://www.wowza.com/developer) to learn more about our APIs and SDK.

## Contact
[Wowza Media Systems, LLC](https://www.wowza.com/contact)

## License
This code is distributed under the [Wowza Public License](/LICENSE.txt).
