package umn.dcsg.wieralocalserver.storageinterfaces;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;

public class AzureStorageInterface extends StorageInterface {
	CloudBlobContainer m_container;

	//Azure Locations (17 regions for now)
	public static final String AS_US_EAST = "useast";                            // East US
	public static final String AS_US_EAST_KEY = "YOUR_OWN_KEY";
	public static final String AS_US_WEST = "uswest";
	public static final String AS_US_WEST_KEY = "YOUR_OWN_KEY";

	public AzureStorageInterface(String strAzureStorageName, String strAzureStoragekey, String strAzureStorageFolder) {
		String strStorageConnectionString = String.format("DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s"
				, strAzureStorageName, strAzureStoragekey);

		try {
			CloudStorageAccount account = CloudStorageAccount.parse(strStorageConnectionString);
			CloudBlobClient serviceClient = account.createCloudBlobClient();
			m_container = serviceClient.getContainerReference(strAzureStorageFolder);
			m_container.createIfNotExists();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		} catch (InvalidKeyException e) {
			e.printStackTrace();
		} catch (StorageException e) {
			e.printStackTrace();
		}
	}

	public boolean put(String key, byte[] value) {
		try {
			InputStream stream = new ByteArrayInputStream(value);

			// Upload an image file.
			CloudBlockBlob blob = m_container.getBlockBlobReference(key);
			blob.upload(stream, value.length);
		} catch (Exception e) {
			System.out.println("Write to Azure Storage failed");
			e.printStackTrace();
			return false;
		}

		return true;
	}

	public byte[] get(String key) {
		byte[] value = null;
		try {
			CloudBlockBlob blob = m_container.getBlockBlobReference(key);
			ByteArrayOutputStream output = new ByteArrayOutputStream(16384);
			blob.download(output);
			value = output.toByteArray();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		} catch (StorageException e) {
			e.printStackTrace();
		}

		return value;
	}

	public boolean delete(String key) {
		try {
			CloudBlockBlob blob = m_container.getBlockBlobReference(key);
			blob.delete();

			return true;
		} catch (URISyntaxException e) {
			e.printStackTrace();
		} catch (StorageException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	protected boolean growTier(int byPercent) {
		return true;
	}

	@Override
	protected boolean shrinkTier(int byPercent) {
		return true;
	}
}