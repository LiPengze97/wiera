package umn.dcsg.wieralocalserver.responses;

import umn.dcsg.wieralocalserver.*;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by Kwangsung on 3/27/2017.
 *
 */
//Need to re-write
public class StoreAdaptivelyResponse extends Response {
    public final static String[] m_supportedStorage = new String[]{"fastest", "cheapest", "workload_aware"};
    String m_strTierName = null;
    boolean m_bSupported = false;
    double m_dbLatencyThreshold = 0;
    double m_dbPeriodThreshold = 0;
    //private ReentrantLock m_lock = null;

/*
    public StoreAdaptivelyResponse(LocalInstance instance, String strTierName, double dbLatencyThreshold)//, double dbPeriodThreshold)
    {
        super(instance);

        m_strTierName = strTierName;
        m_bSupported = isSupportedStorage(strTierName);
        m_dbLatencyThreshold = dbLatencyThreshold;
//		m_dbPeriodThreshold = dbPeriodThreshold;
    }
*/

    public StoreAdaptivelyResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
    }


    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(KEY);
        m_lstRequiredParams.add(VALUE);
    }

    protected boolean isSupportedStorage(String strTierName) {
        for (int i = 0; i < m_supportedStorage.length; i++) {
            if (m_supportedStorage[i].compareTo(strTierName) == 0) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        String strKey = (String) responseParams.get(KEY);
        byte[] value = (byte[]) responseParams.get(VALUE);

        //Temp, need to set tag if needed.
        String strTag = "";

        if (responseParams.containsKey(TAG) == true) {
            strTag = (String) responseParams.get(TAG);
        }

        if (m_bSupported == true) {
            String strTierName;

            if (m_strTierName.compareTo(CHEAPEST) == 0) {
                strTierName = findCheapestStorage(strKey, m_dbLatencyThreshold);

                //System.out.println("Found storage name: " + strTierName);

                if (strTierName != null) {
                    if (m_localInstance.getLocalStorageTierType(strTierName) == TierInfo.TIER_TYPE.REMOTE_TIER) {
                        ForwardPutResponse forward = new ForwardPutResponse(m_localInstance, m_strRelatedEventType, m_initParams);
                        forward.respond(responseParams);
                    } else {
                        //Get data from local instance
                        //return distribution.put(strKey, value, strTierName, strTag, true);
                    }
                } else {
                    System.out.println("Failed to find storage tier adaptively for the key: " + strKey);
                }
            } else {
                System.out.println("Not yet supported storage: " + m_strTierName);
            }
        } else {
            System.out.println("Not supported storage type: " + m_strTierName);
        }

        return false;
        //       return null;
    }

    public String findFastestStorage(String strTierName) {
        String strFoundStorageTier = strTierName;

        //Local first.
        Double dbMinimumLatency = m_localInstance.m_localInfo.getLocalTierLatency(strTierName, Constants.PUT_LATENCY);
        Double dbLatency = 0.0;

        //find storage tier from remote
        //Get remote instance name List
        LinkedList<String> peerList = new LinkedList<String>(m_localInstance.m_peerInstanceManager.getPeersList().keySet());
        String strHostName;

        for (int i = 0; i < peerList.size(); i++) {
            strHostName = peerList.get(i);
            dbLatency = m_localInstance.m_localInfo.getRemoteWriteLatency(strHostName);

            if (dbLatency < dbMinimumLatency) {
                strFoundStorageTier = strHostName;
                break;
            }
        }

        return strFoundStorageTier;
    }

    //Assume that write operation latency propotioned with read operation.
    //Assume that data is broadcasted to all instances.
    public String findCheapestStorage(String strKey, double dbLatencyThreshold) {
        MetaObjectInfo meta = m_localInstance.getMetadata(strKey);
        Locale locale;
        String strFoundStorageTier;
        Double dbLatency = 0.0;

        //Data is stored locally
        if (meta != null && (locale = meta.getLocale(false)) != null) {
            //Local first.
            strFoundStorageTier = locale.getTierName();
            dbLatency = m_localInstance.m_localInfo.getLocalTierLatency(strFoundStorageTier, Constants.PUT_LATENCY);
        } else {
            //Data is being stored at first.
            //Since cloud storage charges request cost
            //Write memory and disk can be cheaper.
            try {
                ArrayList tierList = m_localInstance.m_tiers.getTierList();
                Tier tierInfo = null;
                Tier target = null;

                for (int i = 0; i < tierList.size(); i++) {
                    tierInfo = (Tier) tierList.get(i);

                    if (tierInfo.getExpectedLatency() <= m_dbLatencyThreshold) {
                        System.out.println("checked tiername: " + tierInfo.getTierName());
                        switch (tierInfo.getTierType()) {
                            case MEMORY:
                                if (target == null || target.getTierType() == TierInfo.TIER_TYPE.HDD) {
                                    target = tierInfo;
                                }
                                break;
                            case HDD:
                                if (target == null || target.getTierType() == TierInfo.TIER_TYPE.CLOUD_STORAGE) {
                                    target = tierInfo;
                                }
                                break;
                            case CLOUD_STORAGE:
                                if (target == null) {
                                    target = tierInfo;
                                }
                                break;
                            case CLOUD_ARCHIVAL:
                                return tierInfo.getTierName();
                        }
                    }
                }

                if (target != null) {
                    return target.getTierName();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            return null;
        }

        //System.out.printf("Local tier: %s latency: %f, threshold: %f\n", strTierName, dbLatency, dbLatencyThreshold);

        //latency check
        if (dbLatency >= dbLatencyThreshold) {
            //find storage tier from remote
            //Get remote instance name List
            LinkedList<String> peerList = new LinkedList<String>(m_localInstance.m_peerInstanceManager.getPeersList().keySet());
            String strHostName;

            for (int i = 0; i < peerList.size(); i++) {
                strHostName = peerList.get(i);
                dbLatency = m_localInstance.m_localInfo.getRemoteWriteLatency(strHostName);

                //System.out.printf("Remote host: %s latency: %f, threshold: %f\n", strHostName, dbLatency, dbLatencyThreshold);

                if (dbLatency < dbLatencyThreshold) {
                    strFoundStorageTier = strHostName;
                    break;
                }
            }
        }

        return strFoundStorageTier;
    }

    public String queryStorage(Object... args) {
        String strFoundStorageTier = null;
        //DataDistributionUtil consistencyPolicy = m_localInstance.m_peerInstanceManager.getDataDistribution();
        return strFoundStorageTier;
    }

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {

    }
}