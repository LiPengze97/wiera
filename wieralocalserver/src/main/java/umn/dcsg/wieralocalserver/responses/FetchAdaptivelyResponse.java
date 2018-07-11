package umn.dcsg.wieralocalserver.responses;

import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.Locale;
import umn.dcsg.wieralocalserver.MetaObjectInfo;

import java.util.LinkedList;
import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by Kwangsung on 3/27/2017.
 */
public class FetchAdaptivelyResponse extends Response {
    public final static String[] m_supportedStorage = new String[]{"fastest", "cheapest", "workload_aware"};
    String m_strTierName = null;
    boolean m_bSupported = false;
    double m_dbLatencyThreshold = 0;
    //private ReentrantLock m_lock = null;

    public FetchAdaptivelyResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);

        m_strTierName = (String)params.get(TIER_NAME);
        m_bSupported = isSupportedStorage(m_strTierName);
        m_dbLatencyThreshold = (double)params.get(LATENCY_THRESHOLD);
    }

    protected boolean isSupportedStorage(String strTierName) {
        boolean bSupported = false;

        for (int i = 0; i < m_supportedStorage.length; i++) {
            if (m_supportedStorage[i].compareTo(strTierName) == 0) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(KEY);
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
/*        String strKey = (String) responseParams.get(KEY);

        if (m_bSupported == true) {
            String strTierName;

            if (m_strTierName.compareTo(CHEAPEST) == 0) {
                strTierName = findCheapestStorage(strKey, m_dbLatencyThreshold);

                if (strTierName != null) {
                    DataDistributionUtil distribution = m_localInstance.m_peerInstanceManager.getDataDistribution();
                    if (m_localInstance.getLocalStorageTierType(strTierName) == TierInfo.TIER_TYPE.REMOTE_TIER) {
                        //This case this will include hostname.
                        //System.out.println("Request has been forwarded to " + strTierName);
                        return distribution.forwardingGet(strKey, strTierName, "");
                    } else {
                        //Get data from local instance
                        return distribution.get(strKey);
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

        return null;*/
        return false;
    }

    @Override
    public boolean doCheckResponseConditions(Map<String, Object> responseParams) {
        return true;
    }

    public String findFastestStorage(String strTierName) {
        String strFoundStorageTier = strTierName;

        //Local first.
        Double dbMinimumLatency = m_localInstance.m_localInfo.getLocalTierLatency(strTierName, PUT_LATENCY);
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
        if (meta != null) {
            locale = meta.getLocale(false);

            //Local first.
            dbLatency = m_localInstance.m_localInfo.getLocalTierLatency(locale.getTierName(), PUT_LATENCY);
            strFoundStorageTier = locale.getTierName();
        } else {
            System.out.println("This should not happen. Cannot find meta info for the key:" + strKey);
            return null;
        }

        //System.out.printf("Local tier: %s latency: %f, threshold: %f\n", strTierName, dbLatency, dbLatencyThreshold);

        //check
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
        //String strFoundStorageTier = null;
        //DataDistributionUtil consistencyPolicy = m_localInstance.m_peerInstanceManager.getDataDistribution();
        return null;
    }

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {

    }
}