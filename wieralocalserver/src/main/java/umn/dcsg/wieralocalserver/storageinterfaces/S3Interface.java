package umn.dcsg.wieralocalserver.storageinterfaces;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.google.common.io.ByteStreams;

/**
 * Created with IntelliJ IDEA.
 * User: ajay
 * Date: 21/02/13
 * Time: 11:44 PM
 * To change this template use File | Settings | File Templates.
 */
public class S3Interface extends StorageInterface {
    private AmazonS3Client s3Client = null;
    private String s3Folder = null;
    private String s3BucketName = null;

    //S3 Locations (14 regions for now)
    public static final String S3_US_EAST = "us-east";            // Virginia
    public static final String S3_US_WEST = "us-west";            // Northen California

    public static final String S3_KEY = "YOUR_OWN_KEY";
    public static final String S3_SECRET = "YOUR_OWN_SECRET";

    public S3Interface(String s3Key, String s3Secret, String s3Bucket,
                       String s3Folder) {
        BasicAWSCredentials bawsc = new BasicAWSCredentials(s3Key, s3Secret);
        s3Client = new AmazonS3Client(bawsc);
        this.s3BucketName = s3Bucket;
        this.s3Folder = s3Folder;

        //Check / character
        if (s3Folder.endsWith("/") == false) {
            this.s3Folder += "/";
        }
    }

    public boolean put(String key, byte[] value) {
        try {
            InputStream stream = new ByteArrayInputStream(value);
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(stream.available());
            s3Client.putObject(s3BucketName, s3Folder + key, stream, metadata);
        } catch (Exception e) {
            System.out.println("Write to S3 failed BucketName: " + s3BucketName);
            e.printStackTrace();
            return false;
        }

        return true;
    }

    public byte[] get(String key) {
        S3Object S3data;
        byte[] value = null;

        try {
            S3data = s3Client.getObject(new GetObjectRequest(s3BucketName, s3Folder + key));
            value = ByteStreams.toByteArray(S3data.getObjectContent());
        } catch (Exception e) {
            System.out.println("Get from S3 failed");
            e.printStackTrace();
        }

        return value;
    }

    public boolean delete(String key) {
        s3Client.deleteObject(s3BucketName, key);
        return true;
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