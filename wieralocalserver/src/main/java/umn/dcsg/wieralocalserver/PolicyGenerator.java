package umn.dcsg.wieralocalserver;

import org.json.JSONObject;
import umn.dcsg.wieralocalserver.TierInfo;
import umn.dcsg.wieralocalserver.storageinterfaces.S3Interface;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by with IntelliJ IDEA.
 * User: Kwangsung
 * Date: 7/31/2017 12:43 PM
 */
public class PolicyGenerator {
    public static Map createEvents() {
        Map events = new LinkedHashMap();

        //Events and responses are hardcoded now.

        /////////////////////////////////////////////////////////////////
        //Start with actionPut Event
        List response_list = new LinkedList();
        Map response = new LinkedHashMap();
        Map parameters = new LinkedHashMap();

        //Set param
        parameters.put(TO, "memory");

        //Set response with param
        response.put(RESPONSE_TYPE, STORE_RESPONSE);
        response.put(RESPONSE_PARAMETERS, parameters);

        //Add response into response list
        response_list.add(response);

        ///////////////////////
        //New response
        response = new LinkedHashMap();
        parameters = new LinkedHashMap();

        //Set param
        parameters.put(TO, ALL);

        //Set response with param
        response.put(RESPONSE_TYPE, QUEUE_RESPONSE);
        response.put(RESPONSE_PARAMETERS, parameters);

        //Add response into response list
        response_list.add(response);

        //Set event param (e.g., Threshold)
        Map event = new LinkedHashMap();
        Map eventParams = new LinkedHashMap();

        event.put(EVENT_CONDITIONS, eventParams);
        event.put(RESPONSES, response_list);

        //Set event with reponses
        events.put(ACTION_PUT_EVENT, event);

        //////////////////////////////////////////////////////////////////////////
        ///////////////////////////////////////////
        //Timer event
        response_list = new LinkedList();
        response = new LinkedHashMap();
        parameters = new LinkedHashMap();

        //Set param
        parameters.put(TO, "ebs-st1");
        parameters.put(FROM, "memory");
        parameters.put(DIRTY, true);

        //Set response with param
        response.put(RESPONSE_TYPE, COPY_RESPONSE);
        response.put(RESPONSE_PARAMETERS, parameters);

        //Add response into response list
        response_list.add(response);

        //Set event param (e.g., Timer Threshold)
        event = new LinkedHashMap();
        eventParams = new LinkedHashMap();
        eventParams.put(TIMER_PERIOD, 10000); //Every 10 seconds checks.

        event.put(EVENT_CONDITIONS, eventParams);
        event.put(RESPONSES, response_list);

        //Set event with reponses
        events.put(TIMER_EVENT, event);
        ///////////////////////////////////////////////////////////////////////

        return events;
    }

    //for TripS Test
    public static List createTiers(LinkedList<String> storageList) {
        List tiers = null;
        String strTierName;
        long lExpectedLatency = 0;
        int nStorage = storageList.size();

        tiers = new LinkedList();

        for (int j = 0; j < nStorage; j++) {
            strTierName = storageList.get(j);

            if (strTierName.startsWith("ebs") == true) {
                if (strTierName.compareTo("ebs-sc1") == 0) {
                    lExpectedLatency = 25;
                } else if (strTierName.compareTo("ebs-st1") == 0) {
                    lExpectedLatency = 10;
                } else if (strTierName.compareTo("ebs-gp2") == 0) {
                    lExpectedLatency = 1;
                } else {
                    //Not supported
                    lExpectedLatency = 100;
                }

                tiers.add(createDiskStorage(strTierName, strTierName, "2GB", lExpectedLatency));
            }
            //AWS disk storage
            else if (strTierName.compareTo("standard-disk") == 0) {
                lExpectedLatency = 10;
                tiers.add(createDiskStorage(strTierName, strTierName, "2GB", lExpectedLatency));
            } else if (strTierName.compareTo("premium-p10") == 0) {
                lExpectedLatency = 1;
                tiers.add(createDiskStorage(strTierName, strTierName, "2GB", lExpectedLatency));
            } else if (strTierName.startsWith("s3") == true) {
                //String strArg1 = strRegion.replace("aws", "wiera");
                tiers.add(createCloudStorage("s3", TierInfo.STORAGE_PROVIDER.S3, S3Interface.S3_KEY, S3Interface.S3_SECRET, "wieralocal", "wieradata"));
            } else if (strTierName.startsWith("blob-") == true) {
                //String strArg1 = strRegion.replace("azure", "wiera");
                //tiers.add(createCloudStorage(strTierName, TierInfo.STORAGE_PROVIDER.AS,  S3Interface.S3_KEY, S3Interface.S3_SECRET, strArg1, "wieradata"));
            }
        }

        return tiers;
    }

    public static JSONObject loadPolicy(String strPolicyPath) {
        File policyFile = new File(strPolicyPath);
        String strPolicy;

		if (policyFile.exists() == true && policyFile.isDirectory() == false) {
            byte[] encoded = new byte[0];
            try {
                encoded = Files.readAllBytes(policyFile.toPath());
                strPolicy = new String(encoded);

                //Read policy file.
                return new JSONObject(strPolicy);

            } catch (IOException e) {
                e.printStackTrace();
            }

        } else {
            System.out.println("Failed to find policy file \"" + strPolicyPath + "\"");
        }

        return null;
    }

    public static JSONObject createPolicy(String strPolicyID, int nFault, String strPrimary) {
        JSONObject jsonPolicy = new JSONObject();
        jsonPolicy.put(ID, strPolicyID);
        jsonPolicy.put(DESC, "Wiera policy example");
        //jsonPolicy.put(DATA_DISTRIBUTION, dataDistributionType.getDistributionType());
        //jsonPolicy.put(DATA_DISTRIBUTION_CONSISTENCY, TripS.getDistributionType()); //This is only for TripS
        jsonPolicy.put(FAULT_TOLERANCE, nFault);

        // temp variable for each policy
//		jsonPolicy.put(Constants.PRIMARY, strPrimary);
//		jsonPolicy.put(Constants.PERIOD, 3000);
//		jsonPolicy.put(Constants.PREFER_STORAGE_TYPE, "cheapest");

        LinkedList ec2List = new LinkedList();
        LinkedList ec2StorageList = new LinkedList();

        //Host set
        ec2List.add("aws-us-east");
        ec2List.add("aws-us-west");
/*
		ec2List.add("aws-us-west");
		ec2List.add("aws-us-west-2");
		ec2List.add("aws-ca-central");
*/
//		ec2List.add("aws-eu-west-2");
		/*hostList.add("aws-eu-west");
		hostList.add("aws-asia-ne");
		hostList.add("aws-asia-se");
*/
        //Storage set
        ec2StorageList.add("ebs-st1");
//		ec2StorageList.add("ebs-gp2");
        ec2StorageList.add("s3");

        Map ec2InstanceList = new HashMap();

        //Set Storage Tiers
        List storageTierList = createTiers(ec2StorageList);

        //Set event
        Map eventsList = createEvents();

        //Policy will be hard coded for now.
        for (int i = 0; i < ec2List.size(); i++) {
            //Create new instance into json
            Map new_instance = new HashMap();
            new_instance.put("storage_tiers", storageTierList);
            new_instance.put("events", eventsList);

            ec2InstanceList.put(ec2List.get(i), new_instance);
        }

        //System.out.println(ec2HostnameList);
//		LinkedList azureList = new LinkedList();
//		LinkedList azureStorageList = new LinkedList();

//		azureList.add("azure-us-east");
        //	azureList.add("azure-us-west");
        //	azureList.add("azure-uk-south");

        //Azure Storage set
        //azureStorageList.add("blob-lrs-hot");
//		azureStorageList.add("premium-p10");
//		azureStorageList.add("standard-disk");

//		Map azureHostnameList = createTiers(azureList, azureStorageList);

        //System.out.println(azureHostnameList);

        Map localInstances = new LinkedHashMap();
        localInstances.putAll(ec2InstanceList);
//		hostnameList.putAll(azureHostnameList);

        if (localInstances.size() > 0) {
            jsonPolicy.put(LOCAL_INSTANCES, localInstances);
        }

        return jsonPolicy;
    }

    //for TripS Test
    public static List createTiers(List<String> storageList) {
        List tiers = null;
        String strTierName;
        long lExpectedLatency = 0;
        int nStorage = storageList.size();

        tiers = new LinkedList();

        for (int j = 0; j < nStorage; j++) {
            strTierName = storageList.get(j);

            if (strTierName.startsWith("ebs") == true) {
                if (strTierName.compareTo("ebs-sc1") == 0) {
                    lExpectedLatency = 25;
                } else if (strTierName.compareTo("ebs-st1") == 0) {
                    lExpectedLatency = 10;
                } else if (strTierName.compareTo("ebs-gp2") == 0) {
                    lExpectedLatency = 1;
                } else {
                    //Not supported
                    lExpectedLatency = 100;
                }

                tiers.add(createDiskStorage(strTierName, strTierName, "2GB", lExpectedLatency));
            }
            //AWS disk storage
            else if (strTierName.compareTo("standard-disk") == 0) {
                lExpectedLatency = 10;
                tiers.add(createDiskStorage(strTierName, strTierName, "2GB", lExpectedLatency));
            } else if (strTierName.compareTo("premium-p10") == 0) {
                lExpectedLatency = 1;
                tiers.add(createDiskStorage(strTierName, strTierName, "2GB", lExpectedLatency));
            } else if (strTierName.startsWith("s3") == true) {
                //String strArg1 = strRegion.replace("aws", "wiera");
                tiers.add(createCloudStorage("s3", TierInfo.STORAGE_PROVIDER.S3, S3Interface.S3_KEY, S3Interface.S3_SECRET, "wiera", "wieradata"));
            } else if (strTierName.startsWith("blob-") == true) {
                //String strArg1 = strRegion.replace("azure", "wiera");
                //tiers.add(createCloudStorage(strTierName, TierInfo.STORAGE_PROVIDER.AS,  S3Interface.S3_KEY, S3Interface.S3_SECRET, strArg1, "wieradata"));
            }
        }

        return tiers;
    }

    public static Map createCloudStorage(String strTierName, TierInfo.STORAGE_PROVIDER provider, String strAccount, String strKey, String strArg1, String strArg2) {
        Map cloudStorageTier = new LinkedHashMap();
        cloudStorageTier.put(TIER_NAME, strTierName);
        cloudStorageTier.put(TIER_SIZE, "10GB");    //Size Temp
        cloudStorageTier.put(TIER_TYPE, TierInfo.TIER_TYPE.CLOUD_STORAGE.getTierType());
        cloudStorageTier.put(STORAGE_ARG1, strArg1);        //bucket
        cloudStorageTier.put(STORAGE_ARG2, strArg2);
        cloudStorageTier.put(STORAGE_ID1, strAccount);        //ClientID
        cloudStorageTier.put(STORAGE_ID2, strKey);        //ClientSecret
        cloudStorageTier.put(STORAGE_PROVIDER, provider.getType());        //0 => S3. 1 => Azure, 2 => Google cloud
        cloudStorageTier.put(TIER_EXPECTED_LATENCY, 60);    //Size Temp

        return cloudStorageTier;
    }

    public static Map createDiskStorage(String strTierName, String strTierList, String strSize, long lExpectedLatency) {
        Map diskStorageTier = new LinkedHashMap();
        diskStorageTier.put(TIER_NAME, strTierName);
        diskStorageTier.put(TIER_SIZE, strSize);
        diskStorageTier.put(TIER_TYPE, TierInfo.TIER_TYPE.HDD.getTierType());
        diskStorageTier.put(TIER_LOCATION, strTierList + "/");
        diskStorageTier.put(TIER_EXPECTED_LATENCY, lExpectedLatency);

        return diskStorageTier;
    }
}
