package umn.dcsg.wieralocalserver.responses.peers;

import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.Locale;
import umn.dcsg.wieralocalserver.datadistribution.LazyUpdateManager;
import umn.dcsg.wieralocalserver.responses.Response;

import java.util.List;
import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by Kwangsung on 7/18/2017.
 */
public class QueueResponse extends PeerResponse {
    //private List m_peerHostnameList = null;
    private LazyUpdateManager m_lazyUpdater = null;
    private int m_nWorkerCnt = 5; //Default worker thread count

    public QueueResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);

        if (instance.isStandAloneMode() == false) {
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

        boolean bRet;
        String strReason = NOT_HANDLED  + " in " + getClass().getSimpleName();

        try {
//            List lstPeerInstances;
/*            //In case when target has been changed at run-time
            if (responseParams.containsKey(TARGET_LOCALE_LIST) == true) {
                lstPeerInstances = (List) responseParams.get(TARGET_LOCALE_LIST);
            } else {
                //Broadcast to all
                if (m_peerHostnameList == null || ((m_peerHostnameList.size() == 1) && (m_peerHostnameList.get(0).equals(ALL) == true))) {
                    m_peerHostnameList = getPeersHostnameList();
                }

                lstPeerInstances = Locale.getLocalesWithoutTierName(m_peerHostnameList);
            }*/

            //Need to check conflict on each instance which gets update
            responseParams.put(CONFLICT_CHECK, true);
            bRet = m_lazyUpdater.putToQueue(responseParams);//strKey, lVer, value, strLocalTierName, strTag, operationLatency);

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

    @Override
    public boolean doCheckPeerResponseConditions(Map<String, Object> responseParams) {
        return true;
    }
}