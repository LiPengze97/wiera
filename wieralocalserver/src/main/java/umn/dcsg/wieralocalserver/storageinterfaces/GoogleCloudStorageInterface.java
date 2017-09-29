package umn.dcsg.wieralocalserver.storageinterfaces;

import java.io.*;
import java.net.URLConnection;
import java.util.Collections;

import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;

public class GoogleCloudStorageInterface extends StorageInterface
{
	private final java.io.File DATA_STORE_DIR =
			new java.io.File(System.getProperty("user.home"), ".store/googlecloudstorage");
	private Storage m_storage;
	private String m_strBucketName;

	//Google Cloud Storage
	public static final String GS_CLIENT_ID = "YOUR_OWN_ID";		//ClientID
	public static final String GS_CLIENT_SECRET = "YOUR_OWN_SECRET";

	public static final String GS_US_EAST1 = "wiera-us-east1";
	public static final String GS_US_WEST1 = "wiera-us-west1";

	public GoogleCloudStorageInterface(String strClientID, String strClientSecret, String strBucketName) throws Exception
	{
		HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
		JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
		//GoogleCredential credential = GoogleCredential.getApplicationDefault(httpTransport, jsonFactory);

		// set up authorization code flow
		GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
				httpTransport, jsonFactory, strClientID, strClientSecret,
				Collections.singleton(StorageScopes.DEVSTORAGE_FULL_CONTROL))
				.setApprovalPrompt("auto").setDataStoreFactory(new FileDataStoreFactory(DATA_STORE_DIR)).build();

//		// authorize
		Credential credential = new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");

		m_storage = new Storage.Builder(httpTransport, jsonFactory, credential).setApplicationName("Wiera-GoogleAccess/1.0").build();
		m_strBucketName = strBucketName;
	}

	public boolean put(String key, byte[] value)
	{
		InputStream stream = new ByteArrayInputStream(value);

		try
		{
			//long start_time = System.currentTimeMillis();
			String contentType = URLConnection.guessContentTypeFromStream(stream);
			InputStreamContent content = new InputStreamContent(contentType, stream);

			Storage.Objects.Insert insert = m_storage.objects().insert(m_strBucketName, null, content);
			insert.setName(key);

			if(insert.execute() != null)
			{
				//long end_time = System.currentTimeMillis();
				//double elapsed = end_time - start_time;
				//System.out.printf("Elapsed for a putObject into GCS: %f\n", elapsed);

				return true;
			}
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				stream.close();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}

		return false;
	}

	public byte[] get(String key)
	{
		/*for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
			System.out.println(ste);
		}*/

		ByteArrayOutputStream stream = new ByteArrayOutputStream(8192);
		byte[] value = null;

		try
		{
			long start_time = System.currentTimeMillis();

			Storage.Objects.Get get = m_storage.objects().get(m_strBucketName, key);
			get.getMediaHttpDownloader().setDirectDownloadEnabled(true);
			get.executeMediaAndDownloadTo(stream);
			value = stream.toByteArray();

			long end_time = System.currentTimeMillis();
			double elapsed = end_time - start_time;
			System.out.printf("Elapsed for a getObject from GCS: %f\n", elapsed);
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				stream.close();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}
		return value;
	}

	public boolean delete(String key)
	{
		try
		{
			m_storage.objects().delete(m_strBucketName, key).execute();
			return true;
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}

		return false;
	}

	@Override
	protected boolean growTier(int byPercent)
	{
		return true;
	}

	@Override
	protected boolean shrinkTier(int byPercent)
	{
		return true;
	}
}