package umn.dcsg.wieralocalserver.responses;

import umn.dcsg.wieralocalserver.*;
import umn.dcsg.wieralocalserver.datadistribution.LazyUpdateManager;
import umn.dcsg.wieralocalserver.responses.peers.BroadcastResponse;
import umn.dcsg.wieralocalserver.responses.peers.ForwardPutResponse;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by Kwangsung on 3/27/2017.
 *
 */
//Need to re-write
public class StoreAdaptivelyResponse extends Response {
    //DYNAMIC_LOCALES uses param to find target locale
    public final static String[] m_supportedStorage = new String[]{FASTEST, CHEAPEST, DYNAMIC_LOCALES};
    String m_strTargetLocale = DYNAMIC_LOCALES;
    boolean m_bSupported = false;
    boolean m_bSync = false;
    double m_dbLatencyThreshold = 0;
    double m_dbPeriodThreshold = 0;

    //This is for broadcasting targets Locales
    LazyUpdateManager m_lazyUpdater = null;

    public StoreAdaptivelyResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
        m_lazyUpdater = new LazyUpdateManager(instance, 64);
    }

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {
        Map<String, Object> map = null;

        //Runtime params has higher priority.
        if (responseParams.containsKey(TO) == true) {
            map = responseParams;
        } else if (m_initParams.containsKey(TO) == true) {
            map = m_initParams;
        }

        if (map != null) {
            if (map.get(TO) instanceof List) {
                List list = (List) map.get(TO);
                if (list.contains(DYNAMIC_LOCALES) == true) {
                    m_strTargetLocale = DYNAMIC_LOCALES;
                }
            } else if (map.get(TO) instanceof String) {
                m_strTargetLocale = (String) map.get(TO);
            }
        }

        if (responseParams.containsKey(SYNC) == true) {
            m_bSync = (boolean) responseParams.get(SYNC);
        }

        m_bSupported = isSupportedStorage(m_strTargetLocale);

        if(m_bSupported == true && m_strTargetLocale.equals(DYNAMIC_LOCALES) == true &&
                responseParams.containsKey(TARGET_LOCALE) == false) {
            //No target is set. Store only locally.
            Locale localLocale = Locale.getLocalesWithoutTierName(LocalServer.getHostName());
            LinkedList<Locale> list = new LinkedList<Locale>();
            list.add(localLocale);
            responseParams.put(TARGET_LOCALE, list);
        }

        if (responseParams.containsKey(SYNC) == true) {
            m_bSync = (boolean) responseParams.get(SYNC);
        } else if (m_initParams.containsKey(SYNC) == true) {
            m_bSync = (boolean) m_initParams.get(SYNC);
        } else {
            m_bSync = true;
        }
    }

    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(KEY);
        m_lstRequiredParams.add(VALUE);
        m_lstRequiredParams.add(TARGET_LOCALE);
    }

    protected boolean isSupportedStorage(String strTierName) {
        for (int i=0; i < m_supportedStorage.length; i++) {
            if (m_supportedStorage[i].compareTo(strTierName) == 0) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        String strKey = (String) responseParams.get(KEY);
        boolean bRet = false;
        String strReason = NOT_HANDLED + " in " + getClass().getSimpleName();

        //Temp, need to set tag if needed.
        String strTag = "";

        if (responseParams.containsKey(TAG) == true) {
            strTag = (String) responseParams.get(TAG);
        }

        if (m_bSupported == true) {
            String strTierName;
            byte[] value = (byte[]) responseParams.get(VALUE);

            switch (m_strTargetLocale) {
                case CHEAPEST: {
                    strTierName = findCheapestStorage(strKey, m_dbLatencyThreshold);
                    //System.out.println("Found storage name: " + strTierName);

                    if (strTierName != null) {
                        if (m_localInstance.getLocalStorageTierType(strTierName) == TierInfo.TIER_TYPE.REMOTE_TIER) {
                            ForwardPutResponse forward = new ForwardPutResponse(m_localInstance, m_strEventName, m_initParams);
                            bRet = forward.respond(responseParams);
                        } else {
                            //Get data from local instance
                            //return distribution.put(strKey, value, strTierName, strTag, true);
                        }
                    } else {
                        strReason = "Failed to find storage tier adaptively for the key: " + strKey;
                        System.out.println(strReason);
                    }
                    break;
                }
                case DYNAMIC_LOCALES: {
                    List<Locale> targetLocales = (List<Locale>) responseParams.get(TARGET_LOCALE);
                    Locale localLocale = Locale.getLocalesWithoutTierName(LocalServer.getHostName());

                    if(targetLocales.size() == 0) {
                        strReason = "No target locale found.";
                    } else {
                        boolean bLocalWrite = true;
                        //Check local locale. Simple put with store response
                        if(targetLocales.contains(localLocale) == true) {
                            responseParams.put(TARGET_LOCALE, localLocale);

                            //Remove from current list
                            targetLocales.remove(localLocale);

                            //Store locally
                            bRet = Response.respondAtRuntimeWithClass(m_localInstance, StoreResponse.class, responseParams);

                            if (bRet == false) {
                                bLocalWrite = false;
                                strReason = "Failed to store: " + responseParams.get(REASON) + "in " + getClass().getSimpleName();
                                System.out.println(strReason);
                            } else {
                                strReason = "Putting data OK in AdaptivelyResponse!";
                            }
                        }

                        //Check there is any locale in list if not. do nothing
                        if(bLocalWrite == true && targetLocales.size() > 0) {
                            //Set info for broadcasting
                            responseParams.put(VERSION, 0);
                            responseParams.put(LAST_MODIFIED_TIME, 0L);

                            //Swith hostname into Locale
                            responseParams.put(TARGET_LOCALE_LIST, targetLocales);
                            responseParams.put(TO, responseParams.get(TARGET_LOCALE));

                            if(m_bSync == true) {
                                //call broadcasting response
                                bRet = Response.respondAtRuntimeWithClass(m_localInstance, BroadcastResponse.class, responseParams);
                                if (bRet == false) {
                                    strReason = "Failed to broadcast: " + responseParams.get(REASON) + "in " + getClass().getSimpleName();
                                    System.out.println(strReason);
                                } else {
                                    strReason = "Broadcasting is done";
                                }
                            } else {
                                bRet = m_lazyUpdater.putToQueue(responseParams);

                                //Queue does not add Size into param but Broadcast
                                responseParams.put(SIZE, value.length);
                                strReason = "Broadcasting will be done in background.";
                            }
                        }
                    }

                    break;
                }
            }
        } else {
            strReason = "Not yet supported storage: " + m_strTargetLocale;
            System.out.println("Not supported storage type: " + m_strTargetLocale);
        }

        responseParams.put(RESULT, bRet);
        responseParams.put(REASON, strReason);

        return bRet;
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
    public boolean doCheckResponseConditions(Map<String, Object> responseParams) {
        return true;
    }
}