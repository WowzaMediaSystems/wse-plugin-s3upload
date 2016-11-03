/*
 * This code and all components (c) Copyright 2006 - 2016, Wowza Media Systems, LLC. All rights reserved.
 * This code is licensed pursuant to the Wowza Public License version 1.0, available at www.wowza.com/legal.
 */
package com.wowza.wms.plugin.s3upload;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
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
	private class WriteListener implements IMediaWriterActionNotify
	{

		@Override
		public void onWriteComplete(IMediaStream stream, File file)
		{
			String mediaName = file.getPath().replace(appInstance.getStreamStorageDir(), "");
			if (mediaName.startsWith("/"))
				mediaName = mediaName.substring(1);

			if (transferManager == null)
				return;
			synchronized(lock)
			{
				File tmp = new File(file.getPath() + ".upload");
				if (!tmp.exists())
				{
					try
					{
						tmp.createNewFile();
					}
					catch (IOException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			Upload upload = transferManager.upload(bucketName, mediaName, file);
			upload.addProgressListener(new ProgressListener(mediaName));
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
				System.out.println("progressChanged: " + type.toString());
				switch (type)
				{
				case TRANSFER_COMPLETED_EVENT:
					synchronized(lock)
					{
						File tmp = new File(appInstance.getStreamStorageDir(), mediaName + ".upload");
						tmp.delete();
					}
					if (deleteOriginalFiles)
					{
						File file = new File(appInstance.getStreamStorageDir(), mediaName);
						file.delete();
					}
					break;

				case TRANSFER_FAILED_EVENT:
					logger.warn(MODULE_NAME + ".ProgerssListener [" + appInstance.getContextStr() + "/" + mediaName + "] transfer failed", WMSLoggerIDs.CAT_application, WMSLoggerIDs.EVT_comment);
					break;

				default:
					break;
				}
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
						File tmp = new File(appInstance.getStreamStorageDir(), mediaName + ".upload");
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
	private String accessKey = null;
	private String secretKey = null;
	private String bucketName = null;
	private String endpoint = null;

	private boolean resumeUploads = true;
	private boolean deleteOriginalFiles = false;

	private Object lock = new Object();

	public void onAppStart(IApplicationInstance appInstance)
	{
		logger = WMSLoggerFactory.getLoggerObj(appInstance);
		this.appInstance = appInstance;

		try
		{
			WMSProperties props = appInstance.getProperties();
			accessKey = props.getPropertyStr("s3UploadAccessKey", accessKey);
			secretKey = props.getPropertyStr("s3UploadSecretKey", secretKey);
			bucketName = props.getPropertyStr("s3UploadBucketName", bucketName);
			endpoint = props.getPropertyStr("s3UploadEndpoint", endpoint);
			resumeUploads = props.getPropertyBoolean("s3UploadResumeUploads", resumeUploads);
			deleteOriginalFiles = props.getPropertyBoolean("s3UploadDeletOriginalFiles", deleteOriginalFiles);
			// fix typo in property name
			deleteOriginalFiles = props.getPropertyBoolean("s3UploadDeleteOriginalFiles", deleteOriginalFiles);

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
		try
		{
			if (transferManager != null)
			{
				transferManager.shutdownNow();
			}
		}
		catch (Exception e)
		{
			logger.error(MODULE_NAME + ".onAppStop [" + appInstance.getContextStr() + "] exception: " + e.getMessage(), e);
		}
	}

	private void resumeUploads()
	{
		if (!resumeUploads)
		{
			transferManager.abortMultipartUploads(bucketName, new Date());
			return;
		}

		File storageDir = new File(appInstance.getStreamStorageDir());
		File[] files = storageDir.listFiles(new FilenameFilter()
		{

			@Override
			public boolean accept(File dir, String name)
			{
				return name.toLowerCase().endsWith(".upload");
			}
		});

		for (File file : files)
		{
			String mediaName = file.getPath().replace(storageDir.getPath(), "");
			if (mediaName.startsWith("/"))
				mediaName = mediaName.substring(1);

			mediaName = mediaName.substring(0, mediaName.indexOf(".upload"));
			Upload upload = null;
			FileInputStream fis = null;
			try
			{
				if (file.length() == 0)
				{
					File mediaFile = new File(storageDir, mediaName);
					if (mediaFile.exists())
					{
						upload = transferManager.upload(bucketName, mediaName, mediaFile);
					}
					else
					{
						file.delete();
					}
				}
				else
				{
					fis = new FileInputStream(file);
					// Deserialize PersistableUpload information from disk.
					PersistableUpload persistableUpload = PersistableTransfer.deserializeFrom(fis);
					upload = transferManager.resumeUpload(persistableUpload);
				}
				if (upload != null)
					upload.addProgressListener(new ProgressListener(mediaName));
			}
			catch (Exception e)
			{
				logger.error(MODULE_NAME + ".resumeUploads error resuming upload: [" + appInstance.getContextStr() + "/" + file.getName() + "]", e);
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
	}
}
