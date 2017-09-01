/*
 * This code and all components (c) Copyright 2006 - 2017, Wowza Media Systems, LLC. All rights reserved.
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.PersistableTransfer;
import com.amazonaws.services.s3.transfer.PersistableUpload;
import com.amazonaws.services.s3.transfer.Transfer.TransferState;
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
	private class UploadTask extends TimerTask
	{
		private final String mediaName;
		private final long delay;
		private long lastAge = 0;

		UploadTask(String mediaName, long delay, long age)
		{
			this.mediaName = mediaName;
			this.delay = delay;
			lastAge = age;
		}

		@Override
		public void run()
		{
			boolean doUpload = false;
			boolean finished = false;

			synchronized(lock)
			{
				while (true)
				{
					if (shuttingDown)
					{
						if (debugLog)
							logger.info(MODULE_NAME + ".UploadTask.run() shutting down [" + appInstance.getContextStr() + "/" + mediaName + "]", WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
						finished = true;
						break;
					}

					File uploadFile = new File(storageDir, mediaName + ".upload");
					if (!uploadFile.exists())
					{
						logger.warn(MODULE_NAME + ".UploadTask.run() .uploadfile missing [" + appInstance.getContextStr() + "/" + mediaName + "]", WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
						finished = true;
						break;
					}

					long age = getFileAge(mediaName);
					if (age < lastAge)
					{
						logger.warn(MODULE_NAME + ".UploadTask.run() media file has been modified [" + appInstance.getContextStr() + "/" + mediaName + "] age: " + age + " < lastAge: " + lastAge, WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
						finished = true;
						break;
					}
					lastAge = age;

					if (age >= delay)
					{
						if (debugLog)
							logger.info(MODULE_NAME + ".UploadTask.run() age >= delay [" + appInstance.getContextStr() + "/" + mediaName + "] age: " + age + ", delay: " + delay, WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
						finished = true;
						doUpload = true;
						break;
					}
					if (debugLog)
						logger.info(MODULE_NAME + ".UploadTask.run() age < delay [" + appInstance.getContextStr() + "/" + mediaName + "] age: " + age + ", delay: " + delay, WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
					break;
				}
				if (finished)
				{
					if (debugLog)
						logger.info(MODULE_NAME + ".UploadTask.run() removing timer [" + appInstance.getContextStr() + "/" + mediaName + "]", WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
					uploadTimers.remove(mediaName);
					cancel();
				}
			}

			if (doUpload)
			{
				if (debugLog)
					logger.info(MODULE_NAME + ".UploadTask.run() starting upload [" + appInstance.getContextStr() + "/" + mediaName + "]", WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
				startUpload(mediaName);
			}
			else
			{
				// touch the appInstance so it doesn't timeout while we are still uploading.
				long now = System.currentTimeMillis();
				if (now - touchTimeout >= lastTouch)
				{
					if (debugLog)
						logger.info(MODULE_NAME + ".UploadTask.run() touching appInstance [" + appInstance.getContextStr() + "/" + mediaName + "]", WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
					appInstance.touch();
					lastTouch = now;
				}
			}
		}
	}

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
			String mediaName = getMediaName(file.getPath());
			if (debugLog)
				logger.info(MODULE_NAME + ".onWriteComplete [" + appInstance.getContextStr() + "/" + mediaName + "]", WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);

			if (transferManager == null)
			{
				logger.error(MODULE_NAME + ".WriteListener.onWriteComplete Cannot upload file because S3 Transfer Manager isn't loaded: [" + appInstance.getContextStr() + "/" + mediaName + "]");
				return;
			}

			File uploadFile = null;
			synchronized(lock)
			{
				try
				{
					uploadFile = new File(file.getPath() + ".upload");
					if (uploadFile.exists())
					{
						if (debugLog)
							logger.info(MODULE_NAME + ".onWriteComplete .upload file exists (deleting) [" + appInstance.getContextStr() + "/" + mediaName + "]", WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
						uploadFile.delete();
					}
					uploadFile.createNewFile();
					if (!shuttingDown)
						startUpload(mediaName, uploadDelay);
				}
				catch (IOException e)
				{
					logger.error(MODULE_NAME + ".WriteListener.onWriteComplete Cannot create .upload file: [" + appInstance.getContextStr() + "/" + mediaName + "]", e);
				}
			}
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
				switch (type)
				{
				case TRANSFER_COMPLETED_EVENT:
					if (debugLog)
						logger.info(MODULE_NAME + ".ProgressListener.progressChanged [" + appInstance.getContextStr() + "/" + mediaName + "] event: " + type.toString(), WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
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
					if (debugLog)
						logger.warn(MODULE_NAME + ".ProgressListener.progressChanged [" + appInstance.getContextStr() + "/" + mediaName + "] event: " + type.toString(), WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
					synchronized(lock)
					{
						if (debugLog)
							logger.info(MODULE_NAME + ".ProgressListener.progressChanged [" + appInstance.getContextStr() + "/" + mediaName + "] event: " + type.toString() + ", shutting down", WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
						if (shuttingDown)
							break;
					}

					if (restartFailedUploads)
					{
						if (debugLog)
							logger.info(MODULE_NAME + ".ProgressListener.progressChanged [" + appInstance.getContextStr() + "/" + mediaName + "] event: " + type.toString() + ", restarting upload", WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
						long age = getFileAge(mediaName);
						startUpload(mediaName, restartFailedUploadsTimeout + age);
					}
					break;

				default:
					if (debugLog)
						logger.info(MODULE_NAME + ".ProgressListener.progressChanged [" + appInstance.getContextStr() + "/" + mediaName + "] event: " + type.toString(), WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
					break;
				}
			}

			// touch the appInstance so it doesn't timeout while we are still uploading.
			long now = System.currentTimeMillis();
			if (now - touchTimeout >= lastTouch)
			{
				if (debugLog)
					logger.info(MODULE_NAME + ".ProgressListener.progressChanged touching appInstance [" + appInstance.getContextStr() + "/" + mediaName + "]", WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
				appInstance.touch();
				lastTouch = now;
			}
		}

		@Override
		public void onPersistableTransfer(final PersistableTransfer transfer)
		{
			if (debugLog)
				logger.info(MODULE_NAME + ".ProgressListener.onPersistableTransfer() [" + appInstance.getContextStr() + "/" + mediaName + "] data: " + transfer.serialize(), WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
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
	private String filePrefix = null;
	private String endpoint = null;
	private File storageDir = null;
	private Map<String, Timer> uploadTimers = new HashMap<String, Timer>();

	private boolean debugLog = false;
	private boolean shuttingDown = false;
	private boolean resumeUploads = true;
	private boolean versionFile = false;
	private boolean useLegacyVersioning = false;
	private boolean deleteOriginalFiles = false;
	private boolean restartFailedUploads = true;

	private long restartFailedUploadsTimeout = 60000l;
	private long uploadDelay = 0l;
	private long lastTouch = -1;
	private long touchTimeout = 2500;

	private Object lock = new Object();

	public void onAppStart(IApplicationInstance appInstance)
	{
		this.appInstance = appInstance;
		logger = WMSLoggerFactory.getLoggerObj(appInstance);
		storageDir = new File(appInstance.getStreamStorageDir());
		touchTimeout = appInstance.getApplicationInstanceTouchTimeout() / 2;

		try
		{
			WMSProperties props = appInstance.getProperties();
			accessKey = props.getPropertyStr("s3UploadAccessKey", accessKey);
			secretKey = props.getPropertyStr("s3UploadSecretKey", secretKey);
			bucketName = props.getPropertyStr("s3UploadBucketName", bucketName);
			filePrefix = props.getPropertyStr("s3UploadFilePrefix", filePrefix);
			endpoint = props.getPropertyStr("s3UploadEndpoint", endpoint);
			debugLog = props.getPropertyBoolean("s3UploadDebugLog", debugLog);
			resumeUploads = props.getPropertyBoolean("s3UploadResumeUploads", resumeUploads);
			restartFailedUploads = props.getPropertyBoolean("s3UploadRestartFailedUploads", restartFailedUploads);
			restartFailedUploadsTimeout = props.getPropertyLong("s3UploadRestartFailedUploadTimeout", restartFailedUploadsTimeout);
			versionFile = props.getPropertyBoolean("s3UploadVersionFile", versionFile);
			useLegacyVersioning = props.getPropertyBoolean("s3UploadUseLegacyVersioning", useLegacyVersioning);
			deleteOriginalFiles = props.getPropertyBoolean("s3UploadDeletOriginalFiles", deleteOriginalFiles);
			// fix typo in property name
			deleteOriginalFiles = props.getPropertyBoolean("s3UploadDeleteOriginalFiles", deleteOriginalFiles);
			uploadDelay = props.getPropertyLong("s3UploadDelay", uploadDelay);

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

			logger.info(MODULE_NAME + ".onAppStart [" + appInstance.getContextStr() + " : build #47]");
			logger.info(MODULE_NAME + ".onAppStart [" + appInstance.getContextStr() + "] S3 Bucket Name: " + bucketName + ", Resume Uploads: " + resumeUploads + ", Delete Original Files: " + deleteOriginalFiles + ", Version Files: " + versionFile + ", Upload Delay: " + uploadDelay,
					WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
			transferManager = new TransferManager(s3Client);

			appInstance.getVHost().getThreadPool().execute(new Runnable()
			{

				@Override
				public void run()
				{
					resumeUploads();
				}
			});

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
		logger.info(MODULE_NAME + ".onAppStop [" + appInstance.getContextStr() + "]", WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
		synchronized(lock)
		{
			shuttingDown = true;
			Iterator<String> iter = uploadTimers.keySet().iterator();
			while (iter.hasNext())
			{
				String mediaName = iter.next();
				Timer t = uploadTimers.get(mediaName);
				if (t != null)
					t.cancel();
				if (debugLog)
					logger.info(MODULE_NAME + ".onAppStop  stopping pending upload [" + appInstance.getContextStr() + "/" + mediaName + "]", WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
				iter.remove();
			}
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
		if (debugLog)
			logger.info(MODULE_NAME + ".resumeUploads " + (resumeUploads ? "resuming" : "aborting") + " unfinished Uploads [" + appInstance.getContextStr() + "]", WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);

		if (transferManager != null && !resumeUploads)
		{
			transferManager.abortMultipartUploads(bucketName, new Date());
		}

		List<File> uploadFiles = getMatchingFiles(storageDir, ".upload");

		for (File uploadFile : uploadFiles)
		{
			if (!resumeUploads)
			{
				uploadFile.delete();
			}
			else
			{
				String mediaName = getMediaName(uploadFile.getPath());
				startUpload(mediaName, uploadDelay);
			}
		}
	}

	private void startUpload(String mediaName, long delay)
	{
		synchronized(lock)
		{
			Timer t = uploadTimers.remove(mediaName);
			if (t != null)
				t.cancel();
			long age = getFileAge(mediaName);
			if (delay > 0 && age != -1 && age < delay)
			{
				t = new Timer("UploadTimer: [" + appInstance.getContextStr() + "/" + mediaName + "]");
				long timerDelay = Math.min(delay - age, touchTimeout);
				t.schedule(new UploadTask(mediaName, delay, age), timerDelay, timerDelay / 2);
				uploadTimers.put(mediaName, t);
				if (debugLog)
					logger.info(MODULE_NAME + ".startUpload (delayed) for [" + appInstance.getContextStr() + "/" + mediaName + "] age: " + age + ", delay: " + delay + ", timerDelay: " + timerDelay, WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
			}
			else
			{
				if (debugLog)
					logger.info(MODULE_NAME + ".startUpload (now) for [" + appInstance.getContextStr() + "/" + mediaName + "] age: " + age + ", delay: " + delay, WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
				startUpload(mediaName);
			}
		}
	}

	private void startUpload(String mediaName)
	{
		File uploadFile = new File(storageDir, mediaName + ".upload");
		if (uploadFile == null || !uploadFile.exists())
			return;

		if (transferManager != null)
		{
			Upload upload = null;
			FileInputStream fis = null;
			try
			{
				if (uploadFile.length() == 0)
				{
					if (debugLog)
						logger.info(MODULE_NAME + ".startUpload new or single part upload for [" + appInstance.getContextStr() + "/" + mediaName + "]", WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);

					File mediaFile = new File(storageDir, mediaName);

					if (mediaFile.exists())
					{
						if (!StringUtils.isEmpty(filePrefix))
						{
							mediaName = filePrefix + (filePrefix.endsWith("/") ? "" : "/") + mediaName;
						}
						String uploadName = mediaName;
						if (versionFile)
						{
							uploadName = getMediaNameVersion(mediaName);
						}
						// In order to support setting ACL permissions for the file upload, we will wrap the upload properties in a PutObjectRequest
						PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, uploadName, mediaFile);

						// If the user has specified ACL properties, setup the putObjectRequest with the acl permissions generated
						if (acl != null)
						{
							putObjectRequest.withAccessControlList(acl);
						}

						upload = transferManager.upload(putObjectRequest);
					}
					else
					{
						logger.warn(MODULE_NAME + ".startUpload mediaFile doesn't exist [" + appInstance.getContextStr() + "/" + mediaName + "]", WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
						uploadFile.delete();
					}
				}
				else
				{
					if (debugLog)
						logger.info(MODULE_NAME + ".startUpload resuming multipart upload for [" + appInstance.getContextStr() + "/" + mediaName + "]", WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
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
			{
				if (debugLog)
					logger.info(MODULE_NAME + ".getMatchingFile add file: " + file.getName(), WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
				ret.add(file);
			}
		}

		Collections.sort(ret);

		return ret;
	}

	private long getFileAge(String mediaFile)
	{
		long age = -1;

		File file = new File(storageDir, mediaFile);
		if (file.exists())
			age = System.currentTimeMillis() - file.lastModified();

		return age;
	}

	private String getMediaName(String path)
	{
		String mediaName = path.replace(storageDir.getPath(), "");
		if (mediaName.startsWith(File.separator))
			mediaName = mediaName.substring(File.separator.length());
		if (mediaName.endsWith(".upload"))
			mediaName = mediaName.substring(0, mediaName.indexOf(".upload"));

		return mediaName;
	}

	private String getMediaNameVersion(String mediaName)
	{
		if (!transferManager.getAmazonS3Client().doesObjectExist(bucketName, mediaName))
			return mediaName;

		String newName = mediaName;
		String oldName = mediaName;
		String oldExt = "";
		int oldExtIndex = oldName.lastIndexOf(".");
		if (oldExtIndex >= 0)
		{
			oldExt = oldName.substring(oldExtIndex);
			oldName = oldName.substring(0, oldExtIndex);
		}

		int version = 0;
		while (true)
		{
			newName = oldName + "_" + Integer.toString(version) + oldExt;
			boolean exists = transferManager.getAmazonS3Client().doesObjectExist(bucketName, newName);
			if (debugLog)
				logger.info(MODULE_NAME + ".getMediaNameVersion doesObjectExist: " + newName + ": " + Boolean.toString(exists), WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
			if (!exists)
				break;
			version++;
		}
		if (debugLog)
			logger.info(MODULE_NAME + ".getMediaNameVersion using: " + newName, WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
		return newName;
	}
}
