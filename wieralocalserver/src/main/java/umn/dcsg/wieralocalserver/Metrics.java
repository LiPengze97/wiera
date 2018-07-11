package umn.dcsg.wieralocalserver;

import static umn.dcsg.wieralocalserver.Constants.*;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import umn.dcsg.wieralocalserver.info.OperationLatency;
import umn.dcsg.wieralocalserver.info.Latency;
import umn.dcsg.wieralocalserver.utils.Utils;
import org.json.JSONObject;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is for maintaining the various LocalInstance activity statistics
 */
//This class  might be converted to DB
public class Metrics {
    public static final int MAX_LIST_CNT = 100;

    //Check local storage tier latency continuously.
    TierLatencyMonitor m_tierLatencyMonitor;

    //StorageTiername and latency for each storage
    //This will be shared with other instances //Last 10 operation?
    //StorageTierName, operation_type (put, get), latencies
    private Map<String, Map<String, ConcurrentLinkedDeque<Latency>>> m_localTiersLatencyInfo;

    //Tier violation time_checker.
    //For both remote and local
    private Map<String, Map<String, Long>> m_tiersViolationPeriodInfo;

    //Latency for the operation with distribution type including lock, distribution, and local operation
    //May not be used for now
    //Operations (put and get), latencies info (Operation, Lock, distribution)
    private Map<String, ConcurrentLinkedDeque<OperationLatency>> m_operationLatencyInfo;

    //Network Latency from local instance
    private Map<String, ConcurrentLinkedDeque<Latency>> m_betweenDCsLatenciesInfo;

    //This will be updated by ping in peermanager
    //Hostname, TierName, value (put_latency, get_latency, free_space)
    private Map<String, Map<String, Map<String, Double>>> m_remoteTiersInfo;
    private Map<String, Map<String, Double>> m_remoteNetworkInfo;
    private Map<String, Map<String, Long>> m_remoteAccessInfo;

    //From Where, To where, Cost/GB
    private Map<String, Map<String, Double>> m_networkCostInformation;

    //Target DC, Target Storage, Pricing Item, and Cost.
    private Map<String, Map<String, Map<String, Double>>> m_storageCostInformation;

    //Todo This is only for Wiera Journal version. (latency with only Hostname)
    private Map<String, ConcurrentLinkedDeque<Latency>> m_remoteInstanceWriteLatency;

    //Applications SLA Information
    //SLA goals
    long m_lStoragePercentile = 0;
    long m_lNetworkPercentile = 0;

    long m_lGetSLA = 0;
    long m_lPutSLA = 0;
    String m_strWieraLocation = null;
    public long m_lCheckPeriod = 30; //30 seconds

    //Local Tiers
    public LocalInstance m_localInstance;

    //Thread for checking latency for local tier. -> Free except S3
    public Map<String, Map<String, Map<String, Double>>> getRemoteTiers() {
        return m_remoteTiersInfo;
    }

    public Map<String, Map<String, ConcurrentLinkedDeque<Latency>>> getLocalTiers() {
        return m_localTiersLatencyInfo;
    }

    //For cost information.
    //Write from this instance here to Locale (including forwarding and broadcasting)
    protected static Map<Locale, AtomicInteger> m_getCntForCost = new ConcurrentHashMap<>();
    protected static Map<Locale, AtomicInteger> m_putCntForCost = new ConcurrentHashMap<>();

    //for Requests count only from applications
    protected static LinkedBlockingDeque<Long> m_putRequests = new LinkedBlockingDeque<>();
    protected static LinkedBlockingDeque<Long> m_getRequests = new LinkedBlockingDeque<>();

    //For forwarded requests from other peers
    protected static Map<String, LinkedBlockingDeque<Long>> m_getForwaredRquests = new ConcurrentHashMap<String, LinkedBlockingDeque<Long>>();
    protected static Map<String, LinkedBlockingDeque<Long>> m_putForwaredRquests = new ConcurrentHashMap<String, LinkedBlockingDeque<Long>>();

    public Metrics(LocalInstance localInstance) {
        m_localInstance = localInstance;

        //Pre-create the map for operation latency historical info.
        //This will store the latencies for local storage tier
        Latency latency;
        ConcurrentLinkedDeque list;
        long lStart = 0;
        long lExpectedLatency = 0;
        m_localTiersLatencyInfo = new ConcurrentHashMap<String, Map<String, ConcurrentLinkedDeque<Latency>>>();

        //For checking tier latency violation.
        m_tiersViolationPeriodInfo = new ConcurrentHashMap<>();

        m_tiersViolationPeriodInfo.put(GET_LATENCY, new ConcurrentHashMap<String, Long>());
        m_tiersViolationPeriodInfo.put(PUT_LATENCY, new ConcurrentHashMap<String, Long>());

        for (String strTierName : m_localInstance.m_tiers.m_tierManagerByName.keySet()) {
            ConcurrentHashMap<String, ConcurrentLinkedDeque<Latency>> operationMap = new ConcurrentHashMap<String, ConcurrentLinkedDeque<Latency>>();

            //For the first operation->Get pseudo (expected) latency
            lExpectedLatency = m_localInstance.m_tiers.m_tierManagerByName.get(strTierName).getExpectedLatency();

            lStart = System.currentTimeMillis();
            latency = new Latency(lStart, lStart + lExpectedLatency);
            list = new ConcurrentLinkedDeque<Latency>();
            list.addLast(latency);
            operationMap.put(PUT_LATENCY, list);

            lStart = System.currentTimeMillis();
            latency = new Latency(lStart, lStart + lExpectedLatency);
            list = new ConcurrentLinkedDeque<Latency>();
            list.addLast(latency);
            operationMap.put(GET_LATENCY, list);

            //Set to each storage tier (get and put map)
            m_localTiersLatencyInfo.put(strTierName, operationMap);
        }

        //This will store the whole time for handling operation (including time for lock, distribution, and operation)
        m_operationLatencyInfo = new ConcurrentHashMap<String, ConcurrentLinkedDeque<OperationLatency>>();
        m_operationLatencyInfo.put(GET_LATENCY, new ConcurrentLinkedDeque<OperationLatency>());
        m_operationLatencyInfo.put(PUT_LATENCY, new ConcurrentLinkedDeque<OperationLatency>());

        //Network latency between instance (Can be moved to LocalInstance Server)
        m_betweenDCsLatenciesInfo = new ConcurrentHashMap<String, ConcurrentLinkedDeque<Latency>>();

        //This stores other instances storages information retrieved by ping
        //Contains average latency for each storage and free-space
        m_remoteTiersInfo = new ConcurrentHashMap<String, Map<String, Map<String, Double>>>();

        //Periodically check DCs latencies for finding centralized DC for Cluster mode
        m_remoteNetworkInfo = new ConcurrentHashMap<String, Map<String, Double>>();

        //Periodically check access pattern of peers (in set period)
        m_remoteAccessInfo = new ConcurrentHashMap<String, Map<String, Long>>();

        //Update pricing information when register to Wiera
        //From where to where cost/GB
        m_networkCostInformation = new ConcurrentHashMap<String, Map<String, Double>>();

        //Target DC, Target Storage, Pricing Item, and Cost.
        m_storageCostInformation = new ConcurrentHashMap<String, Map<String, Map<String, Double>>>();

        //Keep updating local storage latency
        m_tierLatencyMonitor = new TierLatencyMonitor(this);
        m_tierLatencyMonitor.startMonitoring();

        //Todo Will be used for Wiera Journal paper
        m_remoteInstanceWriteLatency = new ConcurrentHashMap<String, ConcurrentLinkedDeque<Latency>>();
    }

    public void setGoals(JSONObject goals) {
        try {
            m_lStoragePercentile = Utils.convertToLong(goals.get(STORAGE_PERCENTILE));
            m_lNetworkPercentile = Utils.convertToLong(goals.get(NETWORK_PERCENTILE));

            m_lGetSLA = Utils.convertToLong(goals.get(GET_SLA));
            m_lPutSLA = Utils.convertToLong(goals.get(PUT_SLA));

            m_strWieraLocation = (String) goals.get("wiera_location");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Failed to goals in Metrics");
        }
    }

    //Can be updated later
    public void setCostInfo(JSONObject costInfo) {
        JSONObject jsonHost;
        JSONObject jsonList;
        JSONObject jsonTierItems;
        Type type = new TypeToken<ConcurrentHashMap<String, Double>>() {
        }.getType();
        Gson gson = new Gson();

        try {
            for (String strTargetHostName : costInfo.keySet()) {
                jsonHost = (JSONObject) costInfo.get(strTargetHostName);
                jsonList = (JSONObject) jsonHost.get(NETWORK_COST);
                ConcurrentHashMap<String, Double> networkCost = gson.fromJson(jsonList.toString(), type);

                //Set network cost
                m_networkCostInformation.put(strTargetHostName, networkCost);

                //Now set storage cost
                jsonList = (JSONObject) jsonHost.get(STORAGE_COST);

                //For Storage Tier
                ConcurrentHashMap<String, Map<String, Double>> storageCost = new ConcurrentHashMap<>();

                for (String strTierName : jsonList.keySet()) {
                    jsonTierItems = (JSONObject) jsonList.get(strTierName);
                    ConcurrentHashMap<String, Double> itemList = gson.fromJson(jsonTierItems.toString(), type);
                    storageCost.put(strTierName, itemList);
                }

                m_storageCostInformation.put(strTargetHostName, storageCost);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Failed to cost info in Metrics");
        }
    }

    public boolean addNetworkLatency(String strHostname, Latency latency) {
        ConcurrentLinkedDeque<Latency> list = null;

        if (m_betweenDCsLatenciesInfo.containsKey(strHostname) == false) {
            list = new ConcurrentLinkedDeque<Latency>();
            m_betweenDCsLatenciesInfo.put(strHostname, list);
        } else {
            list = m_betweenDCsLatenciesInfo.get(strHostname);
        }

        if (list.size() >= MAX_LIST_CNT) {
            list.pollFirst();
        }

        list.addLast(latency);
        return true;
    }

    public boolean addTierLatency(String strTierName, String strOPType, Latency latency) {
        if (m_localTiersLatencyInfo.containsKey(strTierName) == false) {
            //Should not happen, but leave as is for now.
            System.out.println("Tier map does not exist. This should not happen as it was created in constructor.");
            return false;
        }

        Map<String, ConcurrentLinkedDeque<Latency>> tierMap = m_localTiersLatencyInfo.get(strTierName);
        ConcurrentLinkedDeque<Latency> list = tierMap.get(strOPType);

        if (tierMap == null) {
            //Should not happen, but leave as is for now.
            System.out.println("Tier latency list does not exist. This should not happen as it should be created in constructor.");
            return false;
        }

        if (list.size() >= MAX_LIST_CNT) {
            list.pollFirst();
        }

        //Todo Shold be removed
        //To avoid cache effect.
        if (strTierName.contains("ebs-st1") == true || strTierName.contains("standard-disk") == true) {
            //In nano seconds
            //10ms
            latency.setAdjustTime(-10000000);
        } else if (strTierName.contains("ebs-sc1") == true) {
            //In nano seconds
            //20ms
            latency.setAdjustTime(-20000000);
        }
        if (strTierName.contains("s3") == true) {
            if (strOPType.compareTo(GET_LATENCY) == 0) {
                if (latency.getLatencyInMills() > 50) {
                    return true;
                }
            } else {
                if (latency.getLatencyInMills() > 120) {
                    return true;
                }
            }
        }

//		System.out.printf("Tier %s %s Latency: %d ms\n", strTierName, strOPType,  latency.getLatencyInMills());
        list.addLast(latency);
        return true;
    }

    public OperationLatency addOperationLatency(String strKey, String strTierName, String strEventName, String strOPType) {
        ConcurrentLinkedDeque<OperationLatency> list = m_operationLatencyInfo.get(strOPType);

        if (list == null) {
            //Should not happen, but leave as is for now.
            System.out.println("Map for operation does not exist. This should not happen as it should be created in constructor.");
            return null;
        }

        OperationLatency latencyInfo = new OperationLatency(strKey, strTierName, strEventName);

        list.addLast(latencyInfo);
        return latencyInfo;
    }

    public double getLocalTierLatency(String strTierName, String strOPType) {
        Latency latency;
        ConcurrentLinkedDeque<Latency> latencies;
        Map<String, ConcurrentLinkedDeque<Latency>> mapType = m_localTiersLatencyInfo.get(strTierName);
        latencies = mapType.get(strOPType);
        Iterator iter = latencies.iterator();
        int nSize = latencies.size();
        double[] latData = new double[nSize];
        int i = 0;

        while (iter.hasNext()) {
            latency = (Latency) iter.next();
            latData[i] = latency.getLatencyInMills();
            i++;

            //Avoid exception for now.
            if (nSize == i) {
                break;
            }
        }

        return Utils.getPercentile(latData, m_lStoragePercentile, m_localInstance.m_tiers.getTier(strTierName).getExpectedLatency());
    }

    public double getLocalTierFreeSpace(String strTierName) {
        return m_localInstance.m_tiers.m_tierManagerByName.get(strTierName).getFreeSpace();
    }

    //Remote tier only store average
    public double getRemoteTierAveLatency(String strHostName, String strTierName, String strOPType) {
        return m_remoteTiersInfo.get(strHostName).get(strTierName).get(strOPType);
    }

    public double getRemoteTierFreeSpace(String strHostName, String strTierName) {
        return m_remoteTiersInfo.get(strHostName).get(strTierName).get(FREE_SPACE);
    }

    //How to get this? With last 10 times? or With last 10mins?
    //This will be sent as JSON format for other instances
    //Hostname, info (put_latency, get_latency, free_space), Info (average_latency or free_space)
    public Map<String, Map<String, Double>> getStorageInfo(boolean bIncludingFreeSpace) {
        Map<String, Map<String, Double>> info = new ConcurrentHashMap<String, Map<String, Double>>();

        //Result
        ConcurrentHashMap<String, Double> storageInfo;

        //Try to retrieve local tier itself information. (different from m_operationLatencyInfo)
        //m_operationLatencyInfo includes all time for lock, distribution and operation
        for (String strTierName : m_localTiersLatencyInfo.keySet()) {
            //Set Tiername as the first result
            storageInfo = new ConcurrentHashMap<String, Double>();

            //getting free-space for each
            if (bIncludingFreeSpace == true) {
                storageInfo.put(FREE_SPACE, getLocalTierFreeSpace(strTierName));
            }

            //getting Get, Put latency
            storageInfo.put(PUT_LATENCY, getLocalTierLatency(strTierName, PUT_LATENCY));
            storageInfo.put(GET_LATENCY, getLocalTierLatency(strTierName, GET_LATENCY));

            info.put(strTierName, storageInfo);
        }

        return info;
    }

    public ConcurrentHashMap<String, Double> getDCsLatencyInfo() {
        ConcurrentHashMap<String, Double> latencyInfo = new ConcurrentHashMap<String, Double>();

        //Try to retrieve local tier itself information. (different from m_operationLatencyInfo)
        //m_operationLatencyInfo includes all time for lock, distribution and operation
        for (String strHostName : m_betweenDCsLatenciesInfo.keySet()) {
            latencyInfo.put(strHostName, getNetworkLatency(strHostName));
        }

        return latencyInfo;
    }

    public double getLargestNetworkLatency() {
        double dbLargest = 0;
        double dbLatency = 0;

        for (String strHostName : m_betweenDCsLatenciesInfo.keySet()) {
            dbLatency = getNetworkLatency(strHostName);

            if (dbLatency > dbLargest) {
                dbLargest = dbLatency;
            }
        }

        return dbLargest;
    }

    public double getNetworkLatency(String strHostnameTo) {
        ConcurrentLinkedDeque<Latency> latencies;
        Latency latency;
        double[] latData;

        latencies = m_betweenDCsLatenciesInfo.get(strHostnameTo);

        //No latency available now.
        if (latencies.size() == 0) {
            return -1;
        }

        int nSize = latencies.size();
        latData = new double[nSize];
        Iterator iter = latencies.iterator();
        int i = 0;

        while (iter.hasNext()) {
            latency = (Latency) iter.next();
            latData[i] = latency.getLatencyInMills();
            i++;

            //Avoid exception for now.
            if (nSize == i) {
                break;
            }
        }

        //Assume that default network is the worst case 200 ms
        return Utils.getPercentile(latData, m_lNetworkPercentile, 200);
    }

    public double getLargestRemoteNetworkLatency(String strRemoteHostName) {
        Map<String, Double> latencyList = m_remoteNetworkInfo.get(strRemoteHostName);
        double dbLargest = 0;

        for (double latency : latencyList.values()) {
            if (latency > dbLargest) {
                dbLargest = latency;
            }
        }

        return dbLargest;
    }

    public double getRemoteNetworkLatency(String strHostNameFrom, String strHostNameTo) {
        if (m_remoteAccessInfo.containsKey(strHostNameFrom) == true && m_remoteNetworkInfo.get(strHostNameFrom).containsKey(strHostNameTo) == true) {
            return m_remoteNetworkInfo.get(strHostNameFrom).get(strHostNameTo);
        } else {
            return -1;
        }
    }

    public Map<String, Double> getNetworkLatencyBetweenDCs() {
        Map<String, Double> ret = new ConcurrentHashMap<String, Double>();

        for (String strHostName : m_betweenDCsLatenciesInfo.keySet()) {
            double latency = getNetworkLatency(strHostName);
            ret.put(strHostName, latency);
        }

        //Network latency to local is zero
        ret.put(LocalServer.getHostName(), (double) 0);

        return ret;
    }

    public void updatePeerInformation(String strHostName, String strStorageInfo, String strNetworkInfo, String strAccessInfo) {
        //Storage info
        Gson gson = new Gson();
        Type type;

        if(strStorageInfo != null) {
            type = new TypeToken<ConcurrentHashMap<String, ConcurrentHashMap<String, Double>>>() {
            }.getType();
            Map<String, Map<String, Double>> storageInfo = gson.fromJson(strStorageInfo, type);
            m_remoteTiersInfo.put(strHostName, storageInfo);
        }

        if(strNetworkInfo != null) {
            //Network Info
            type = new TypeToken<ConcurrentHashMap<String, Double>>() {
            }.getType();

            ConcurrentHashMap<String, Double> networkInfo = gson.fromJson(strNetworkInfo, type);
            m_remoteNetworkInfo.put(strHostName, networkInfo);
        }

        if(strAccessInfo != null) {
            //Access Information (Put and Get Cnt)
            type = new TypeToken<ConcurrentHashMap<String, Double>>() {
            }.getType();
            ConcurrentHashMap<String, Long> accessInfo = gson.fromJson(strAccessInfo, type);
            m_remoteAccessInfo.put(strHostName, accessInfo);
        }
    }

    public long getLatestOperationTime(String strTierName, String strOP) {
        ConcurrentLinkedDeque<Latency> list = m_localTiersLatencyInfo.get(strTierName).get(strOP);

        if (list.size() == 0) {
            return 0;
        } else {
            Latency last = list.getLast();
            return last.getEndDate();
        }
    }

    public ArrayList getLocalTierList() {
        return m_localInstance.m_tiers.getTierNameList();
    }

    public String getMonitoringData() {
        Map<String, Double> network_data = getNetworkLatencyBetweenDCs();
        Map<String, Map<String, Double>> storage_data = getStorageInfo(false);

        JSONObject latencyData = new JSONObject();
        latencyData.put(NETWORK_LATENCY, network_data);
        latencyData.put(STORAGE_LATENCY, storage_data);

        return latencyData.toString();
    }

    public double getLatency(String strHostName, String strTierName, String strOPType, boolean bIncludingBroadcasting) {
        //Check with local instance tiers.
        if (strHostName.compareTo(LocalServer.getHostName()) == 0) {
            if (bIncludingBroadcasting == true) {
                if (strOPType.compareTo(GET_LATENCY) == 0) {
                    return 2 * getNetworkLatency(m_strWieraLocation) + getLocalTierLatency(strTierName, strOPType);
                } else {
                    //Time for global lock
                    return 2 * getNetworkLatency(m_strWieraLocation) + getLocalTierLatency(strTierName, strOPType) + getLargestNetworkLatency();
                }
            } else {
                return getLocalTierLatency(strTierName, strOPType);
            }
        } else {        //Check remote latency
            String strRemoteHostName = strHostName;
            String strRemoteTierName = strTierName;

            //Check whether ping information exist or not
            if (m_remoteNetworkInfo.containsKey(strRemoteHostName) == false || m_remoteTiersInfo.containsKey(strRemoteHostName) == false ||
                    m_remoteTiersInfo.get(strRemoteHostName).containsKey(strTierName) == false) {
                System.out.printf("Remote host: %s and tiername: %s information is not available yet\n", strRemoteHostName, strRemoteTierName);
                return -1;
            }

            if (bIncludingBroadcasting == true) {
                if (strOPType.compareTo(GET_LATENCY) == 0) {
                    return getRemoteTierAveLatency(strRemoteHostName, strRemoteTierName, strOPType) +
                            getRemoteNetworkLatency(strRemoteHostName, LocalServer.getHostName()) +
                            (getRemoteNetworkLatency(strRemoteHostName, m_strWieraLocation) * 2);
                } else {
                    return getRemoteTierAveLatency(strRemoteHostName, strRemoteTierName, strOPType) +
                            getNetworkLatency(strRemoteHostName) +
                            getLargestRemoteNetworkLatency(strRemoteHostName) +
                            (getRemoteNetworkLatency(strRemoteHostName, m_strWieraLocation) * 2);
                }
            } else {
                double dbRemoteTierAveLatency = getRemoteTierAveLatency(strRemoteHostName, strRemoteTierName, strOPType);
                double dbRemoteNetworkLatency = getRemoteNetworkLatency(strRemoteHostName, LocalServer.getHostName());
                double dbNetworkLatency = getNetworkLatency(strRemoteHostName);

                if (strOPType.compareTo(GET_LATENCY) == 0) {
//					System.out.printf("%s Latency: %f - SLA: %d \n", strOPType, dbRemoteTierAveLatency + dbRemoteNetworkLatency, m_lGetSLA);

                    return getRemoteTierAveLatency(strRemoteHostName, strRemoteTierName, strOPType) +
                            getRemoteNetworkLatency(strRemoteHostName, LocalServer.getHostName());
                } else {
//					System.out.printf("%s Latency: %f - SLA: %d \n", strOPType, dbRemoteTierAveLatency + dbNetworkLatency, m_lPutSLA);
                    return getRemoteTierAveLatency(strRemoteHostName, strRemoteTierName, strOPType) +
                            getNetworkLatency(strRemoteHostName);
                }
            }
        }
    }

    public boolean checkLatencyWithinSLA(String strHostName, String strTierName, String strOPType, boolean bIncludingBroadcast) {
        double dbLatency = getLatency(strHostName, strTierName, strOPType, bIncludingBroadcast);

        if (dbLatency <= 0) {
            return false;
        }

        if (strOPType.compareTo(GET_LATENCY) == 0) {
            return dbLatency <= m_lGetSLA;
        } else {
            return dbLatency <= m_lPutSLA;
        }
    }

    //Get operation cost from local instance
    //Put cost include broadcasting cost
    // A puts B then B broadcasts to all others in the list
    public double getCost(String strTargetHostName, String strTierName, String strOPType, List<Locale> targetLocaleList, long lSize) {
        String strLocalHost = LocalServer.getHostName();
        double dbNetworkCost = 0;
        double dbRequestCost = 0;
        double dbOperCost = 0;

        //Get operation
        if (strOPType.compareTo(GET_LATENCY) == 0) {
            dbNetworkCost = (m_networkCostInformation.get(strTargetHostName).get(strLocalHost) * lSize) / GB_TO_BYTE;
            dbRequestCost = m_storageCostInformation.get(strTargetHostName).get(strTierName).get(GET_REQUEST_COST);
            dbOperCost = (m_storageCostInformation.get(strTargetHostName).get(strTierName).get(DATA_RETRIEVAL) * lSize) / GB_TO_BYTE;
        } else {
            //This would include operation cost for local (if local and target is the same, no network cost)
            if (targetLocaleList != null) {
                for (Locale targetLocale : targetLocaleList) {
                    dbNetworkCost += (m_networkCostInformation.get(strTargetHostName).get(targetLocale.getHostName()) * lSize) / GB_TO_BYTE;
                    dbRequestCost += m_storageCostInformation.get(targetLocale.getHostName()).get(targetLocale.getTierName()).get(PUT_REQUEST_COST);
                    dbOperCost += (m_storageCostInformation.get(targetLocale.getHostName()).get(targetLocale.getTierName()).get(DATA_WRITE) * lSize) / GB_TO_BYTE;
                }
            }
        }

        return dbNetworkCost + dbRequestCost + dbOperCost;
    }

    //This function will be called in primary for TripS to send access pattern to TripS as a parameter
    public Map<String, Map<String, Long>> getAllAccessInfoFrom(long lCheckFrom) {
        long lCheckPeriod = System.currentTimeMillis() - lCheckFrom;
        Map<String, Long> localAccessInfo = getLocalAccessInfo(lCheckPeriod, m_lCheckPeriod);

        m_remoteAccessInfo.put(LocalServer.getHostName(), localAccessInfo);
        return m_remoteAccessInfo;
    }

    public Map<String, Integer> getLatestForwaredCnt() {
        return getForwaredCnt(0, m_lCheckPeriod);
    }

    //This function for other peer (sending this to others)
    public Map<String, Integer> getForwaredCnt(long lFrom, long lPeriod) {
        long lCurTime = System.currentTimeMillis() - lFrom;
        Map<String, Integer> accessInfo = new ConcurrentHashMap<>();

        accessInfo.put(GET_ACCESS_CNT, getForwardedGetReqCntInPeriod(lCurTime, lPeriod));
        accessInfo.put(PUT_ACCESS_CNT, getForwardedPutReqCntInPeriod(lCurTime, lPeriod));

        return accessInfo;
    }

    public Map<String, Long> getLatestLocalAccessInfo() {
        return getLocalAccessInfo(0, 90);
    }

    //This function for other peer (sending this to others)
    public Map<String, Long> getLocalAccessInfo(long lFrom, long lPeriod) {
        long lCurTime = System.currentTimeMillis() - lFrom;
        ConcurrentHashMap<String, Long> accessInfo = new ConcurrentHashMap<>();

        //Todo Should be removed
        //For test purpose
        long lGetReq = getGetReqCntInPeriod(lCurTime, lPeriod);
        long lPutReq = getPutReqCntInPeriod(lCurTime, lPeriod);

        if (lGetReq == 0 && lPutReq == 0) {
            lGetReq = 1;
            lPutReq = 1;
        }

        accessInfo.put(GET_ACCESS_CNT, lGetReq);
        accessInfo.put(PUT_ACCESS_CNT, lPutReq);

        return accessInfo;
    }

    public long getRemoteAccessInfo(String strHostName, String strOPType) {
        if (m_remoteAccessInfo.containsKey(strHostName) == true) {
            return Utils.convertToLong(m_remoteAccessInfo.get(strHostName).get(strOPType));
        } else {
            return 0;
        }

    }

    /////////////////////////////////////////////////////////////////////////////////////////
    //For experiment 2
    public void incrementalRequestCnt(String strLatencyType) {
        try {
            if (strLatencyType.compareTo(GET) == 0) {
                m_getRequests.addLast(System.currentTimeMillis());
            } else {
                m_putRequests.addLast(System.currentTimeMillis());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void incrementalForwardedRequestCnt(String strHostname, String strOPType) {
        LinkedBlockingDeque<Long> forwaredRequest;

        try {
            if (strOPType.compareTo(GET) == 0) {
                if(m_getForwaredRquests.containsKey(strHostname) == false) {
                    forwaredRequest = new LinkedBlockingDeque<>();
                    m_getForwaredRquests.put(strHostname, forwaredRequest);
                } else {
                    forwaredRequest = m_getForwaredRquests.get(strHostname);
                }

            } else {
                if(m_putForwaredRquests.containsKey(strHostname) == false) {
                    forwaredRequest = new LinkedBlockingDeque<>();
                    m_putForwaredRquests.put(strHostname, forwaredRequest);
                } else {
                    forwaredRequest = m_putForwaredRquests.get(strHostname);
                }
            }
            forwaredRequest.addLast(System.currentTimeMillis());

            //System.out.printf("get forwared: %d,  put forwared: %d\n", m_getForwaredRquests.size(), m_putForwaredRquests.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void clearRequestsCnt() {
        m_getRequests.clear();
        m_putRequests.clear();
    }

    public int getGetReqCntInPeriod(long lCurTime, long lPeriodInSec) {
        return findReqCntInPeriod(lCurTime, m_getRequests, lPeriodInSec);
    }

    public int getPutReqCntInPeriod(long lCurTime, long lPeriodInSec) {
        return findReqCntInPeriod(lCurTime, m_putRequests, lPeriodInSec);
    }

    public int getForwardedGetReqCntInPeriod(String strHostname, long lCurTime, long lPeriodInSec) {
        if(m_getForwaredRquests.containsKey(strHostname) == true) {
            return findReqCntInPeriod(lCurTime, m_getForwaredRquests.get(strHostname), lPeriodInSec);
        }

        return 0;
    }

    //All forwarded get cnt
    public int getForwardedGetReqCntInPeriod(long lCurTime, long lPeriodInSec) {
        int nSum = 0;

        for(String strHostname: m_getForwaredRquests.keySet()) {
            nSum += getForwardedGetReqCntInPeriod(strHostname, lCurTime, lPeriodInSec);
        }

        return nSum;
    }

    public int getForwardedPutReqCntInPeriod(String strHostname, long lCurTime, long lPeriodInSec) {
        if(m_putForwaredRquests.containsKey(strHostname) == true) {
            return findReqCntInPeriod(lCurTime, m_putForwaredRquests.get(strHostname), lPeriodInSec);
        }

        return 0;
    }

    //All forwarded put cnt
    public int getForwardedPutReqCntInPeriod(long lCurTime, long lPeriodInSec) {
        int nSum = 0;

        for(String strHostname: m_putForwaredRquests.keySet()) {
            nSum += getForwardedPutReqCntInPeriod(strHostname, lCurTime, lPeriodInSec);
        }

        return nSum;
    }


    public int findReqCntInPeriod(long lCurTime, LinkedBlockingDeque<Long> list, long lPeriodInSec) {
        long lForwardedTime = 0;
        int nSize = list.size();
        long lElapse = 0;

        int nCnt = 0;
        Iterator<Long> it = list.iterator();

        while(it.hasNext()) {
            lForwardedTime = it.next();
            lElapse = lCurTime - lForwardedTime;

            if (lElapse > lPeriodInSec * 1000) {
                return nSize - (nCnt + 1);
            }

            nCnt++;
        }

        return nSize;
    }

    public void incrementalGetForCost(Locale locale) {
        AtomicInteger cnt;

        if (m_getCntForCost.containsKey(locale) == false) {
            cnt = new AtomicInteger(0);
            m_getCntForCost.put(locale, cnt);
        } else {
            cnt = m_getCntForCost.get(locale);
        }

        cnt.incrementAndGet();
    }

    //For cost information from this instnace. only increased
    public void incrementalPutForCost(Locale locale) {
        AtomicInteger cnt;

        if (m_putCntForCost.containsKey(locale) == false) {
            cnt = new AtomicInteger(0);
            m_putCntForCost.put(locale, cnt);
        } else {
            cnt = m_putCntForCost.get(locale);
        }
        cnt.incrementAndGet();
    }

    public void printLatencyInfo() {
        double dbGetLatency = 0;
        double dbPutLatency = 0;

        for (String strTierName : m_localTiersLatencyInfo.keySet()) {
            Latency latency;
            ConcurrentLinkedDeque<Latency> latencies = m_localTiersLatencyInfo.get(strTierName).get(GET_LATENCY);
            Iterator<Latency> iter = latencies.iterator();

            int nSize = latencies.size();

            double[] latData = new double[nSize];
            int i = 0;

            while (iter.hasNext()) {
                latency = iter.next();
                latData[i] = latency.getLatencyInMills();
                i++;

                //Avoid exception for now.
                if (nSize == i) {
                    break;
                }
            }

            //Get Latency for each tier
            dbGetLatency = Utils.getPercentile(latData, m_lStoragePercentile, m_localInstance.m_tiers.getTier(strTierName).getExpectedLatency());

            latencies = m_localTiersLatencyInfo.get(strTierName).get(PUT_LATENCY);
            iter = latencies.iterator();

            latData = new double[latencies.size()];
            i = 0;

            while (iter.hasNext()) {
                latency = iter.next();
                latData[i] = latency.getLatencyInMills();
                i++;
            }

            dbPutLatency = Utils.getPercentile(latData, m_lStoragePercentile, m_localInstance.m_tiers.getTier(strTierName).getExpectedLatency());
            System.out.printf("%s: %f %f\n", strTierName, dbGetLatency, dbPutLatency);
        }
    }

    public void printAllInfo() {
        Gson gson = new Gson();

//		System.out.println("Local Tiers Information\n" + gson.toJson(m_localTiersLatencyInfo).toString());
//		System.out.println("Peers' Tiers Information\n" + gson.toJson(m_remoteTiersInfo).toString());
//		System.out.println("Operations Information\n" + gson.toJson(m_operationLatencyInfo).toString());
//		System.out.println("DC latencies Information\n" + gson.toJson(m_betweenDCsLatenciesInfo).toString());
//		System.out.println("Remote DC's Tiers Information\n" + gson.toJson(m_remoteTiersInfo.toString()));
//		System.out.println("Remote DC latencies Information\n" + gson.toJson(m_remoteNetworkInfo.toString()));
//		System.out.println("Remote DC latencies Information\n" + gson.toJson(m_remoteAccessInfo.toString()));
        System.out.println(String.format("Total get count: %d, put count: %d", m_getRequests.size(), m_putRequests.size()));
        System.out.println(String.format("Total forwarded get count: %d, forwarded put count: %d", m_getForwaredRquests.size(), m_putForwaredRquests.size()));
        System.out.println(String.format("Last 30 seconds get count: %d, Put count: %d",
                getLatestLocalAccessInfo().get(GET_ACCESS_CNT), getLatestLocalAccessInfo().get(PUT_ACCESS_CNT)));
        System.out.println(String.format("Last 30 seconds forwarded get count: %d, Put count: %d",
                getLatestForwaredCnt().get(GET_ACCESS_CNT), getLatestForwaredCnt().get(PUT_ACCESS_CNT)));
        System.out.println("Last 30 seconds Remote Access Cnt" + m_remoteAccessInfo.toString());
        System.out.println(String.format("Get count for cost %s\nPut count for cost %s\n", m_getCntForCost.toString(), m_putCntForCost.toString()));
    }

    //Need to be elaborated
    public void printOperationLatency() {
        ConcurrentLinkedDeque<OperationLatency> latencies = m_operationLatencyInfo.get(GET_LATENCY);
        OperationLatency operationLatency;
        Iterator<OperationLatency> iter = latencies.iterator();

        while (iter.hasNext()) {
            operationLatency = iter.next();
            operationLatency.printAllLatencyInfo();
        }

        latencies = m_operationLatencyInfo.get(PUT_LATENCY);
        iter = latencies.iterator();

        while (iter.hasNext()) {
            operationLatency = iter.next();
            operationLatency.printAllLatencyInfo();
        }
    }

    public void printOperationTime() {
        double dbGetOperationWithinSLA = 0;
        double dbGetOperationWithoutSLA = 0;
        double dbPutOperationWithinSLA = 0;
        double dbPutOperationWithoutSLA = 0;

        OperationLatency latency;
        ConcurrentLinkedDeque<OperationLatency> latencies = m_operationLatencyInfo.get(GET_LATENCY);
        Iterator<OperationLatency> iter = latencies.iterator();

        double[] getWithinSLA = new double[latencies.size()];
        double[] getWithoutSLA = new double[latencies.size()];
        int nWithin = 0;
        int nWithout = 0;

        while (iter.hasNext()) {
            latency = iter.next();

            if (latency.getLatencyInMills() <= m_lGetSLA) {
                getWithinSLA[nWithin] = latency.getLatencyInMills();
                nWithin++;
            } else {
                getWithoutSLA[nWithout] = latency.getLatencyInMills();
                nWithout++;
            }
        }

        double[] inGetSLA = Arrays.copyOfRange(getWithinSLA, 0, nWithin);
        double[] outGetSLA = Arrays.copyOfRange(getWithoutSLA, 0, nWithout);

        dbGetOperationWithinSLA = Utils.getPercentile(inGetSLA, m_lStoragePercentile, 0);
        dbGetOperationWithoutSLA = Utils.getPercentile(outGetSLA, m_lStoragePercentile, 0);

        ///////////////////////////////////////////////////////
        //Now put
        latencies = m_operationLatencyInfo.get(PUT_LATENCY);
        iter = latencies.iterator();

        double[] putWithinSLA = new double[latencies.size()];
        double[] putWithoutSLA = new double[latencies.size()];
        nWithin = 0;
        nWithout = 0;

        while (iter.hasNext()) {
            latency = iter.next();

            if (latency.getLatencyInMills() <= m_lPutSLA) {
                putWithinSLA[nWithin] = latency.getLatencyInMills();
                nWithin++;
            } else {
                putWithoutSLA[nWithout] = latency.getLatencyInMills();
                nWithout++;
            }
        }

        double[] inPutSLA = Arrays.copyOfRange(putWithinSLA, 0, nWithin);
        double[] outPutSLA = Arrays.copyOfRange(putWithoutSLA, 0, nWithout);

        dbPutOperationWithinSLA = Utils.getPercentile(inPutSLA, m_lStoragePercentile, 0);
        dbPutOperationWithoutSLA = Utils.getPercentile(outPutSLA, m_lStoragePercentile, 0);

        System.out.println("%Total\tG_SLA\t90th\tG_NOSLA\t90th\tP_SLA\t90th\tP_NOSLA\t90th");
        System.out.printf("%d\t%f\t%d\t%f\t%d\t%f\t%d\t%f\t\n", inGetSLA.length, dbGetOperationWithinSLA, outGetSLA.length, dbGetOperationWithoutSLA,
                inPutSLA.length, dbPutOperationWithinSLA, outPutSLA.length, dbPutOperationWithoutSLA);
    }

    public void clearAggregatedCostInfo() {
        m_getCntForCost.clear();
        m_putCntForCost.clear();
    }

    public void printAggregatedCostInfo() {
        //Get requested number to local and remote to make sum of cost.
        //For get request first.
        AtomicInteger nCnt;

        double totalNetworkCost = 0;
        double totalGetRequestCost = 0;
        double totalPutRequestCost = 0;
        double totalGetOperationCost = 0;
        double totalPutOperationCost = 0;
        double totalStorageCost = 0;

        double[] result = new double[3];

        for (Locale locale : m_getCntForCost.keySet()) {
            nCnt = m_getCntForCost.get(locale);

            if (nCnt != null) {
                result = calculateGetCost(locale, nCnt.intValue(), DATA_SIZE);

                totalNetworkCost += result[0];
                totalGetRequestCost += result[1];
                totalGetOperationCost += result[2];
            }
        }

        for (Locale locale : m_putCntForCost.keySet()) {
            nCnt = m_putCntForCost.get(locale);

            if (nCnt != null) {
                result = calculatePutCost(locale, nCnt.intValue(), DATA_SIZE);

                totalNetworkCost += result[0];
                totalPutRequestCost += result[1];
                totalPutOperationCost += result[2];

                //lStoragePeriod: 1 -> per/Month
                //lStoragePeriod: 30 -> per/Day
                //lStoragePeriod: 7200 -> per/hour

                totalStorageCost += calculateStorageCost(locale, nCnt.intValue(), DATA_SIZE, 30);
            }
        }

        System.out.println("Network\tGet_Req\tPut_Req\tWrite\tRetrieval\tStorage");

        String strCost = String.format("%.9f %.9f %.9f %.9f %.9f %.9f\n", totalNetworkCost, totalGetRequestCost, totalPutRequestCost, totalGetOperationCost, totalPutOperationCost, totalStorageCost);
        System.out.println(strCost);

        try {
            PrintWriter writer = new PrintWriter("Cost.txt", "UTF-8");
            writer.write(strCost);
            writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    public double[] calculateGetCost(Locale targetLocale, int nCnt, long lDataSize) {
        double[] dbGetCost = new double[3];
        String strTargetHostName;
        String strMyHostName = LocalServer.getHostName();

        //Network cost
        dbGetCost[0] = nCnt * lDataSize * (m_networkCostInformation.get(targetLocale.getHostName()).get(strMyHostName).doubleValue() / GB_TO_BYTE);

        //Get Request cost
        dbGetCost[1] = nCnt * m_storageCostInformation.get(targetLocale.getHostName()).get(targetLocale.getTierName()).get(GET_REQUEST_COST).doubleValue();

        //Data Retrival cost
        dbGetCost[2] = nCnt * lDataSize * (m_storageCostInformation.get(targetLocale.getHostName()).get(targetLocale.getTierName()).get(DATA_RETRIEVAL).doubleValue() / GB_TO_BYTE);

        return dbGetCost;
    }

    //Put cost includes broadcasting cost as broadcasting take a part away with several put operations
    public double[] calculatePutCost(Locale targetLocale, int nCnt, long lDataSize) {
        double[] dbPutCost = new double[3];
        String strTargetHostName;
        String strMyHostName = LocalServer.getHostName();

        //Need to consider network cost
        dbPutCost[0] = nCnt * lDataSize * (m_networkCostInformation.get(strMyHostName).get(targetLocale.getHostName()).doubleValue() / GB_TO_BYTE);

        //Request Cost
        dbPutCost[1] = nCnt * m_storageCostInformation.get(targetLocale.getHostName()).get(targetLocale.getTierName()).get(PUT_REQUEST_COST).doubleValue();

        //Data Write Cost
        dbPutCost[2] = nCnt * lDataSize * (m_storageCostInformation.get(targetLocale.getHostName()).get(targetLocale.getTierName()).get(DATA_WRITE).doubleValue() / GB_TO_BYTE);

        return dbPutCost;
    }

    public double calculateStorageCost(Locale targetLocaleString, int nCnt, long lDataSize, long lStoragePeriod) {
        double dbStorageCost = 0;
        String strTargetHostName;

        //Simple to calculate
        dbStorageCost = nCnt * lDataSize * (m_storageCostInformation.get(targetLocaleString.getHostName()).get(targetLocaleString.getTierName()).get(STORAGE_COST).doubleValue() / GB_TO_BYTE) / lStoragePeriod;
        return dbStorageCost;
    }

    public Map<String, ConcurrentLinkedDeque<OperationLatency>> getOperationInfo() {
        return m_operationLatencyInfo;
    }

    //Find the complete latency information
    //This is because if operation latency is not completed yet,
    //The latency shows negative value
    public OperationLatency getLatestOperationInfo(String strOpType) {
        if (m_operationLatencyInfo.get(strOpType).isEmpty() == false) {
            Iterator<OperationLatency> iter = m_operationLatencyInfo.get(strOpType).descendingIterator();
            OperationLatency latency;
            while (iter.hasNext()) {
                latency = iter.next();

                if (latency.isDone() == true) {
                    return latency;
                }
            }
        }

        return null;
    }

    public void setViolatedTier(String strHostName, String strTierName, String strOPType) {
        if (m_tiersViolationPeriodInfo.get(strOPType).containsKey(strHostName + ":" + strTierName) == false) {
            m_tiersViolationPeriodInfo.get(strOPType).put(strHostName + ":" + strTierName, System.currentTimeMillis());
        }
    }

    public void removeViolatedTier(String strHostName, String strTierName, String strOPType) {
        if (m_tiersViolationPeriodInfo.get(strOPType).containsKey(strHostName + ":" + strTierName) == true) {
            m_tiersViolationPeriodInfo.get(strOPType).remove(strHostName + ":" + strTierName);
        }
    }

    public boolean isViolatedMoreThanPeriod(String strHostName, String strTierName, String strOPType) {
        if (m_tiersViolationPeriodInfo.get(strOPType).containsKey(strHostName + ":" + strTierName) == true) {
            //Todo should be back to 10000
            //Set period 10seconds
            if (System.currentTimeMillis() - m_tiersViolationPeriodInfo.get(strOPType).get(strHostName + ":" + strTierName) > (10000 / 6)) {
                return true;
            }
        }

        return false;
    }

    //Todo For Wiera Jounal
    public void addRemoteLatency(String strHostname, Latency latency) {
        ConcurrentLinkedDeque<Latency> list = m_remoteInstanceWriteLatency.get(strHostname);

        if (list == null) {
            list = new ConcurrentLinkedDeque<>();
            m_remoteInstanceWriteLatency.put(strHostname, list);
        }

        if (list.size() >= MAX_LIST_CNT) {
            list.pollFirst();
        }

        list.addLast(latency);
    }

    //Todo For Wiera Jounal
    public double getRemoteWriteLatency(String strHostName) {
        Latency latency;
        ConcurrentLinkedDeque<Latency> latencies = m_remoteInstanceWriteLatency.get(strHostName);
        Iterator iter = latencies.iterator();
        int nSize = latencies.size();
        double[] latData = new double[nSize];
        int i = 0;

        while (iter.hasNext()) {
            latency = (Latency) iter.next();
            latData[i] = latency.getLatencyInMills();
            i++;

            //Avoid exception for now.
            if (nSize == i) {
                break;
            }
        }

        return Utils.getPercentile(latData, m_lStoragePercentile, 250);
    }

    /*//Other Distributiontype can call this function. with broadcastingList to null
    //And get operation don't need to use broadcasting List so it will be null
    public String findCheapestStorage(HashMap<String, TierInfo.TIER_TYPE> targetList, String strOPType, HashMap<String, TierInfo.TIER_TYPE> broadcastingList) {
        return findCheapestStorage(targetList, strOPType, broadcastingList, m_dataDistributionType);
    }

    //Check current latency status -> if violation can happen, find next one
    //For now this is called directly only for TripS
    public String findCheapestStorage(HashMap<String, TierInfo.TIER_TYPE> targetList, String strOPType, HashMap<String, TierInfo.TIER_TYPE> broadcastingList, DataDistributionUtil.DATA_DISTRIBUTION_TYPE distribution_type) {
        //only one tier in TTL
        if (targetList.size() == 1) {
            //Return hostname
            String strTierName = (String) targetList.keySet().toArray()[0];
            TierInfo.TIER_TYPE tierType = targetList.get(strTierName);

            if (tierType == TierInfo.TIER_TYPE.REMOTE_TIER) {
                return strTierName;
            } else {
                return LocalServer.getHostName() + ":" + strTierName;
            }
        } else {
            Latency latency = new Latency();
            latency.start();
            double dbLowestCost = 999999;
            double dbLowestCostWithin = 999999;
            double dbLowestLatency = 999999;
            double dbPrice = 0;
            boolean bWithinSLA = false;
            String strCheapestTierName = null;
            String strCheapestHostName = null;
            String strCheapestTierNameWithinSLA = null;
            String strCheapestHostNameWithinSLA = null;
            TierInfo.TIER_TYPE type;
            String strHostName;

            //Search local storage tier first.
            for (String strTierName : targetList.keySet()) {
                try {
                    type = targetList.get(strTierName);

                    if (type == TierInfo.TIER_TYPE.REMOTE_TIER) {
                        strHostName = strTierName.split(":")[0];
                        strTierName = strTierName.split(":")[1];
                    } else        //This is local tier.
                    {
                        strHostName = LocalServer.getHostName();
                    }

                    bWithinSLA = m_localInstance.m_localInfo.checkLatencyWithinSLA(strHostName, strTierName, strOPType, distribution_type);

                    //Todo this should be moved to Event
                    //This means there is dynamic in that host or tier check period
                    if (bWithinSLA == false) {
                        m_localInstance.m_localInfo.setViolatedTier(strHostName, strTierName, strOPType);

                        if (m_localInstance.m_localInfo.isViolatedMoreThanPeriod(strHostName, strTierName, strOPType) == false) {
                            //Keep using it.
                            bWithinSLA = true;
                        }
                    } else {
                        m_localInstance.m_localInfo.removeViolatedTier(strHostName, strTierName, strOPType);
                    }

                    dbPrice = m_localInstance.m_localInfo.getCost(strHostName, strTierName, strOPType, broadcastingList, Constants.DATA_SIZE);

                    if (bWithinSLA == true) {
                        if (dbPrice < dbLowestCostWithin) {
                            dbLowestCostWithin = dbPrice;
                            strCheapestHostNameWithinSLA = strHostName;
                            strCheapestTierNameWithinSLA = strTierName;
                        }
                    } else {
                        //Only check the cheapest one
                        if (dbPrice < dbLowestCost) {
                            dbLowestCost = dbPrice;
                            strCheapestHostName = strHostName;
                            strCheapestTierName = strTierName;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (strCheapestHostNameWithinSLA != null && strCheapestTierNameWithinSLA != null) {
                latency.stop();
                //System.out.printf("%d ms elapsed to find the cheapest storage %s:%s\n", latency.getLatencyInMills(), strCheapestTierHostName, strCheapestTierName);
                String strFound = String.format("%s:%s", strCheapestHostNameWithinSLA, strCheapestTierNameWithinSLA);
                //System.out.printf("Found: %s\n", strFound);
                return strFound;
            } else if (strCheapestHostName != null && strCheapestHostName != null) {
                System.out.println("For now there is no storage tier within SLA. But return cheapest one" + strCheapestHostName + ":" + strCheapestTierName);
                return String.format("%s:%s", strCheapestHostName, strCheapestHostName);
            } else {
                return null;
            }
        }
    }*/
}