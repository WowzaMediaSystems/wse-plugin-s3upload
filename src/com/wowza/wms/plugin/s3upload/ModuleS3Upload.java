/*
 * This code and all components (c) Copyright 2006 - 2016, Wowza Media Systems, LLC. All rights reserved.
 * This code is licensed pursuant to the Wowza Public License version 1.0, available at www.wowza.com/legal.
 */
package com.wowza.wms.plugin.s3upload;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.PersistableTransfer;
import com.amazonaws.services.s3.transfer.PersistableUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.internal.S3ProgressListener;
import com.wowza.util.StringUtils;
import com.wowza.wms.application.IApplicationInstance;
import com.wowza.wms.application.WMSProperties;
import com.wowza.wms.logging.WMSLogger;
import com.wowza.wms.logging.WMSLoggerFactory;
import com.wowza.wms.logging.WMSLoggerIDs;
import com.wowza.wms.module.ModuleBase;
import com.wowza.wms.stream.IMediaStream;
import com.wowza.wms.stream.IMediaWriterActionNotify;

public class ModuleS3Upload extends ModuleBase
{
	private class MyFilter implements FileFilter
	{

		private String suffix;

		MyFilter(String suffix)
		{
			this.suffix = suffix;
		}

		@Override
		public boolean accept(File pathname)
		{
			return pathname.isDirectory() || pathname.getName().toLowerCase().endsWith(suffix.toLowerCase());
		}

	}

	private class WriteListener implements IMediaWriterActionNotify
	{

		@Override
		public void onWriteComplete(IMediaStream stream, File file)
		{
			if (transferManager == null)
			{
				logger.error(MODULE_NAME + ".WriteListener.onWriteComplete Cannot upload file because S3 Transfoer Manager isn't loaded: [" + appInstance.getContextStr() + "/" + file.getName() + "]");
				return;
			}

			boolean doUpload = false;
			File uploadFile = null;
			synchronized(lock)
			{
				uploadFile = new File(file.getPath() + ".upload");
				if (!uploadFile.exists())
				{
					try
					{
						uploadFile.createNewFile();
					}
					catch (IOException e)
					{
						logger.error(MODULE_NAME + ".WriteListener.onWriteComplete Cannot create .upload file: [" + appInstance.getContextStr() + "/" + file.getName() + "]", e);
					}
				}
				if (uploadFile.exists() && !shuttingDown)
					doUpload = true;
			}
			if (doUpload)
				startUpload(uploadFile);

		}

		@Override
		public void onFLVAddMetadata(IMediaStream stream, Map<String, Object> extraMetadata)
		{
			// no-op
		}
	}

	private class ProgressListener implements S3ProgressListener
	{
		final String mediaName;
		long lastTouch = -1;
		long touchTimeout = appInstance.getApplicationInstanceTouchTimeout() / 2;

		ProgressListener(String mediaName)
		{
			this.mediaName = mediaName;
		}

		@Override
		public void progressChanged(ProgressEvent progressEvent)
		{
			if (progressEvent.getEventType().isTransferEvent())
			{
				ProgressEventType type = progressEvent.getEventType();
				System.out.println("progressChanged: " + type.toString());
				switch (type)
				{
				case TRANSFER_COMPLETED_EVENT:
					synchronized(lock)
					{
						File uploadFile = new File(storageDir, mediaName + ".upload");
						uploadFile.delete();
					}
					if (deleteOriginalFiles)
					{
						File mediaFile = new File(appInstance.getStreamStorageDir(), mediaName);
						mediaFile.delete();
					}
					break;

				case TRANSFER_FAILED_EVENT:
					logger.warn(MODULE_NAME + ".ProgerssListener [" + appInstance.getContextStr() + "/" + mediaName + "] transfer failed", WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
					synchronized(lock)
					{
						if (shuttingDown)
							break;
					}

					if (restartFailedUploads)
					{
						Timer t = new Timer();
						t.schedule(new TimerTask()
						{

							@Override
							public void run()
							{
								boolean doUpload = false;
								File uploadFile = null;
								synchronized(lock)
								{
									uploadFile = new File(storageDir, mediaName + ".upload");
									if (uploadFile.exists() && !shuttingDown)
										doUpload = true;
								}

								if (doUpload)
									startUpload(uploadFile);
							}
						}, restartFailedUploadsTimeout);
					}
					break;

				default:
					break;
				}
			}

			// touch the appInstance so it doesn't timeout while we are still uploading.
			long now = System.currentTimeMillis();
			if (now - touchTimeout >= lastTouch)
			{
				appInstance.touch();
				lastTouch = now;
			}
		}

		@Override
		public void onPersistableTransfer(final PersistableTransfer transfer)
		{
			System.out.println("onPersistableTransfer: " + transfer.serialize());
			appInstance.getVHost().getThreadPool().execute(new Runnable()
			{

				@Override
				public void run()
				{
					synchronized(lock)
					{
						FileOutputStream fos = null;
						File tmp = new File(storageDir, mediaName + ".upload");
						try
						{
							if (!tmp.exists())
								tmp.createNewFile();
							fos = new FileOutputStream(tmp);
							transfer.serialize(fos);
						}
						catch (Exception e)
						{

						}
						finally
						{
							if (fos != null)
								try
								{
									fos.close();
								}
								catch (Exception e)
								{
								}
						}
					}
				}
			});
		}

	}

	public static final String MODULE_NAME = "ModuleS3Upload";
	public static final String PROP_NAME_PREFIX = "s3Upload";

	private WMSLogger logger = null;
	private IApplicationInstance appInstance = null;

	private TransferManager transferManager = null;
	private AccessControlList acl = null;

	private String accessKey = null;
	private String secretKey = null;
	private String bucketName = null;
	private String endpoint = null;
	private String filePrefix = null
	private File storageDir = null;

	private boolean shuttingDown = false;
	private boolean resumeUploads = true;
	private boolean deleteOriginalFiles = false;
	private boolean restartFailedUploads = true;

	private long restartFailedUploadsTimeout = 60000l;

	private Object lock = new Object();

	public void onAppStart(IApplicationInstance appInstance)
	{
		logger = WMSLoggerFactory.getLoggerObj(appInstance);
		this.appInstance = appInstance;
		storageDir = new File(appInstance.getStreamStorageDir());

		try
		{
			WMSProperties props = appInstance.getProperties();
			accessKey = props.getPropertyStr("s3UploadAccessKey", accessKey);
			secretKey = props.getPropertyStr("s3UploadSecretKey", secretKey);
			bucketName = props.getPropertyStr("s3UploadBucketName", bucketName);
			endpoint = props.getPropertyStr("s3UploadEndpoint", endpoint);
			filePrefix = props.getPropertyStr("s3FilePrefix", filePrefix);
			resumeUploads = props.getPropertyBoolean("s3UploadResumeUploads", resumeUploads);
			restartFailedUploads = props.getPropertyBoolean("s3UploadRestartFailedUploads", restartFailedUploads);
			restartFailedUploadsTimeout = props.getPropertyLong("s3UploadRestartFailedUploadTimeout", restartFailedUploadsTimeout);
			deleteOriginalFiles = props.getPropertyBoolean("s3UploadDeletOriginalFiles", deleteOriginalFiles);
			// fix typo in property name
			deleteOriginalFiles = props.getPropertyBoolean("s3UploadDeleteOriginalFiles", deleteOriginalFiles);

			// This value should be the URI representation of the "Group Grantee" found here http://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html under "Amazon S3 Predefined Groups"
			String aclGroupGranteeUri = props.getPropertyStr("s3UploadACLGroupGranteeUri");
			// This should be a string that represents the level of permissions we want to grant to the "Group Grantee" access to the file to be uploaded
			String aclPermissionRule = props.getPropertyStr("s3UploadACLPermissionRule");

			GroupGrantee grantee = null;
			Permission permission = null;

			// With the passed property, check if it maps to a specified GroupGrantee
			if (!StringUtils.isEmpty(aclGroupGranteeUri))
				grantee = GroupGrantee.parseGroupGrantee(aclGroupGranteeUri);
			// In order for the parsing to work correctly, we will go ahead and force uppercase on the string passed'
			if (!StringUtils.isEmpty(aclPermissionRule))
				permission = Permission.parsePermission(aclPermissionRule.toUpperCase());

			// If we have properties for specifying permisions on the file upload, create the AccessControlList object and set the Grantee and Permissions
			if (grantee != null && permission != null)
			{
				acl = new AccessControlList();
				acl.grantPermission(grantee, permission);
			}

			if (StringUtils.isEmpty(accessKey) || StringUtils.isEmpty(secretKey))
			{
				logger.warn(MODULE_NAME + ".onAppStart: [" + appInstance.getContextStr() + "] missing S3 credentials", WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
				return;
			}

			AmazonS3 s3Client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));

			if (!StringUtils.isEmpty(endpoint))
				s3Client.setEndpoint(endpoint);

			if (!StringUtils.isEmpty(bucketName))
			{
				boolean hasBucket = false;
				List<Bucket> buckets = s3Client.listBuckets();
				for (Bucket bucket : buckets)
				{
					if (bucket.getName().equals(bucketName))
					{
						hasBucket = true;
						break;
					}
				}
				if (!hasBucket)
				{
					logger.warn(MODULE_NAME + ".onAppStart: [" + appInstance.getContextStr() + "] missing S3 bucket: " + bucketName, WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
					return;
				}
			}

			logger.info(MODULE_NAME + ".onAppStart [" + appInstance.getContextStr() + "] S3 Bucket Name: " + bucketName + ", Resume Uploads: " + resumeUploads + ", Delete Original Files: " + deleteOriginalFiles, WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
			transferManager = new TransferManager(s3Client);
			resumeUploads();

			appInstance.addMediaWriterListener(new WriteListener());
		}
		catch (AmazonS3Exception ase)
		{
			logger.error(MODULE_NAME + ".onAppStart [" + appInstance.getContextStr() + "] AmazonS3Exception: " + ase.getMessage());
		}
		catch (Exception e)
		{
			logger.error(MODULE_NAME + ".onAppStart [" + appInstance.getContextStr() + "] exception: " + e.getMessage(), e);
		}
		catch (Throwable t)
		{
			logger.error(MODULE_NAME + ".onAppStart [" + appInstance.getContextStr() + "] throwable exception: " + t.getMessage(), t);
		}
	}

	public void onAppStop(IApplicationInstance appInstance)
	{
		synchronized(lock)
		{
			shuttingDown = true;
		}

		try
		{
			if (transferManager != null)
			{
				transferManager.shutdownNow(false);
			}
		}
		catch (Exception e)
		{
			logger.error(MODULE_NAME + ".onAppStop [" + appInstance.getContextStr() + "] exception: " + e.getMessage(), e);
		}
	}

	private void resumeUploads()
	{
		if (transferManager != null && !resumeUploads)
		{
			transferManager.abortMultipartUploads(bucketName, new Date());
			return;
		}

		List<File> uploadFiles = getMatchingFiles(storageDir, ".upload");

		for (File uploadFile : uploadFiles)
		{
			startUpload(uploadFile);
		}
	}

	private void startUpload(File uploadFile)
	{
		if (uploadFile == null || !uploadFile.exists())
			return;

		String mediaName = uploadFile.getPath().replace(storageDir.getPath(), "");
		if (mediaName.startsWith(File.separator))
			mediaName = mediaName.substring(File.separator.length());

		mediaName = mediaName.substring(0, mediaName.indexOf(".upload"));
		
		if(!StringUtils.isEmpty(filePrefix))
		{
			mediaName = filePrefix + (filePrefix.endsWith("/") ? "" : "/") +  mediaName;
		}

		if (transferManager != null)
		{
			Upload upload = null;
			FileInputStream fis = null;
			try
			{
				if (uploadFile.length() == 0)
				{
					File mediaFile = new File(storageDir, mediaName);
					if (mediaFile.exists())
					{
						// In order to support setting ACL permissions for the file upload, we will wrap the upload properties in a PutObjectRequest
						PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, mediaName, mediaFile);

						// If the user has specified ACL properties, setup the putObjectRequest with the acl permissions generated
						if (acl != null)
						{
							putObjectRequest.withAccessControlList(acl);
						}

						upload = transferManager.upload(putObjectRequest);
					}
					else
					{
						uploadFile.delete();
					}
				}
				else
				{
					fis = new FileInputStream(uploadFile);
					// Deserialize PersistableUpload information from disk.
					PersistableUpload persistableUpload = PersistableTransfer.deserializeFrom(fis);
					upload = transferManager.resumeUpload(persistableUpload);
				}
				if (upload != null)
					upload.addProgressListener(new ProgressListener(mediaName));
			}
			catch (Exception e)
			{
				logger.error(MODULE_NAME + ".startUpload error starting or resuming upload: [" + appInstance.getContextStr() + "/" + uploadFile.getName() + "]", e);
			}
			finally
			{
				if (fis != null)
				{
					try
					{
						fis.close();
					}
					catch (IOException e)
					{
					}
				}
			}
		}
		else
		{
			logger.error(MODULE_NAME + ".startUpload error starting or resuming upload: [" + appInstance.getContextStr() + "/" + uploadFile.getName() + "] Amazon S3 TransferManager not running.");
		}
	}

	private List<File> getMatchingFiles(File dir, String suffix)
	{
		List<File> ret = new ArrayList<File>();

		File[] files = dir.listFiles(new MyFilter(suffix));

		for (File file : files)
		{
			if (file.isDirectory())
				ret.addAll(getMatchingFiles(file, suffix));
			else
				ret.add(file);
		}

		Collections.sort(ret);

		return ret;
	}
}
