package umn.dcsg.wieralocalserver.responses;

import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.Locale;
import umn.dcsg.wieralocalserver.datadistribution.LazyUpdateManager;

import java.util.List;
import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by Kwangsung on 7/18/2017.
 */
public class QueueResponse extends Response {
    private List m_targetHostnameList = null;
    private LazyUpdateManager m_lazyUpdater = null;
    private int m_nWorkerCnt = 5; //Default worker thread count

    public QueueResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);

        if (instance.isStandAloneMode() == false) {
            if (params.containsKey(TO) == true) {
                m_targetHostnameList = (List) params.get(TO);
            }

            if (m_initParams.containsKey(WORKER_CNT) == true) {
                m_nWorkerCnt = (int) m_initParams.get(WORKER_CNT);
            }

            m_lazyUpdater = new LazyUpdateManager(instance, m_nWorkerCnt);
        }
    }

    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(KEY);
        m_lstRequiredParams.add(VALUE);
        m_lstRequiredParams.add(VERSION);
        m_lstRequiredParams.add(TAG);
        //m_lstRequiredParams.add(ONLY_META_INFO);
        //m_lstRequiredParams.add(TIER_NAME); //Local tier name to retrieve object (to save memory)
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        if (m_localInstance.isStandAloneMode() == true) {
            //Result
            responseParams.put(RESULT, true);
            responseParams.put(REASON, getClass().getSimpleName() + " cannot response in LocalInstance stand-alone mode");
            return true;
        }

        boolean bRet;
        String strReason = NOT_HANDLED;

        try {
            List lstPeerInstances;

            //If target changed at run-time
            if (responseParams.containsKey(TARGET_LOCALES) == true) {
                lstPeerInstances = (List) responseParams.get(TARGET_LOCALES);
            } else {
                //Broadcast to all
                if (m_targetHostnameList == null || ((m_targetHostnameList.size() == 1) && (m_targetHostnameList.get(0).equals(ALL) == true))) {
                    m_targetHostnameList = m_localInstance.m_peerInstanceManager.getPeersHostnameList();
                }

                lstPeerInstances = Locale.getLocalesWithoutTierName(m_targetHostnameList);
            }

            //Need to check conflict on each instance which gets update
            responseParams.put(CONFLICT_CHECK, true);

            bRet = m_lazyUpdater.putToQueue(lstPeerInstances, responseParams);//strKey, lVer, value, strLocalTierName, strTag, operationLatency);

            if (bRet == false) {
                strReason = "Failed to put request into the queue";
            }
        } catch (Exception e) {
            bRet = false;
            strReason = e.getMessage();
        }

        //Result
        responseParams.put(RESULT, bRet);
        if (bRet == false && strReason != null) {
            responseParams.put(REASON, strReason);
        }

        return bRet;
    }

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {
    }
}