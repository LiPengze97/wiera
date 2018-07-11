package umn.dcsg.wieralocalserver.responses.peers;

import com.google.gson.Gson;
import com.sleepycat.persist.impl.Store;
import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.LocalServer;
import umn.dcsg.wieralocalserver.Locale;
import umn.dcsg.wieralocalserver.datadistribution.ParallelPeerRequest;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by Kwangsung on 7/26/2017.
 */
public class BroadcastResponse extends PeerResponse {
    public BroadcastResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
    }

    @Override
    public boolean doCheckPeerResponseConditions(Map<String, Object> responseParams) {
        return true;
    }

    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(KEY);
        //m_lstRequiredParams.add(ALL_PEER_HOSTNAMES);
        //m_lstRequiredParams.add(ONLY_META_INFO);
        m_lstRequiredParams.add(VALUE);
        m_lstRequiredParams.add(VERSION);
        m_lstRequiredParams.add(TAG);
    }

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {
        //Set target locale
        if (responseParams.containsKey(TARGET_LOCALE_LIST) == false) {
            if (m_peerHostnameList == null || m_peerHostnameList.size() == 0 ||
                    ((m_peerHostnameList.size() == 1) && (m_peerHostnameList.get(0).equals(ALL) == true))) {
                m_peerHostnameList = getPeersHostnameList();
            }

            responseParams.put(TARGET_LOCALE_LIST, Locale.getLocalesWithoutTierName(m_peerHostnameList));
        }
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        boolean bRet;

        String strKey = (String) responseParams.get(KEY);
        int nVer = (int) responseParams.get(VERSION);
        byte[] value = (byte[]) responseParams.get(VALUE);
        long lSize = value.length;
        long lLastModifiedTime = (long) responseParams.get(LAST_MODIFIED_TIME);
        String strTag = (String) responseParams.get(TAG);
        List<Locale> lstTargetLocales = (List) responseParams.get(TARGET_LOCALE_LIST);
        boolean bShareMeta = false;

        if(responseParams.containsKey(SHARE_META_INFO) == true) {
            bShareMeta = (boolean)responseParams.get(SHARE_META_INFO);
        }

        //Check local locale
        //Check local locale first if local is included
        if(lstTargetLocales.contains(Locale.getLocalesWithoutTierName(LocalServer.getHostName())) == true) {
            //Check locally written in response sequential
            if(checkMetaInResponse(strKey, responseParams) == false) {
                //The key has not been written in locally. (-> no store response before)
                if(respondAtRuntimeWithClass(m_localInstance, Store.class, responseParams) == false) {
                    //If failed to write locally return false.
                    responseParams.put(RESULT, false);
                    responseParams.put(SIZE, lSize);
                    return false;
                }
            }

            lstTargetLocales.remove(Locale.getLocalesWithoutTierName(LocalServer.getHostName()));
        }

        //Send to all but check whether a peer is included in the target list
        //if not only meta data needs to be broadcast
        List<Thread> lstThread;
        List<ParallelPeerRequest> lstReq;

        Map<Thread, ParallelPeerRequest> reqSent = sendRequestInParallel(PUT_PEER, lstTargetLocales,
                strKey, nVer, lSize, value, strTag, lLastModifiedTime, false, false);
        lstThread = new LinkedList<>(reqSent.keySet());
        lstReq = new LinkedList<>(reqSent.values());

        //Send meta data for rest of region if set
        List<String> lstForMeta = getPeersHostnameList();
        for(ParallelPeerRequest req: lstReq) {
            lstForMeta.remove(req.getTargetHostname());
        }

        //if meta data is needed to be broadcasted
        if(bShareMeta == true && lstForMeta.size() > 0) {
            Gson gson = new Gson();
            String strList = gson.toJson(lstTargetLocales);

            List<Locale> restLocaleList = Locale.getLocalesWithoutTierName(lstForMeta);

            Map<Thread, ParallelPeerRequest> reqMetaSent = sendRequestInParallel(PUT_PEER, restLocaleList,
                    strKey, nVer, 0, strList.getBytes(), strTag, lLastModifiedTime, true, false);

            //For wait
            List<Thread> lstMetaThread = new LinkedList<> (reqMetaSent.keySet());
            lstThread.addAll(lstMetaThread);

            //For results
            List<ParallelPeerRequest> lstMetaReq = new LinkedList<> (reqMetaSent.values());
            lstReq.addAll(lstMetaReq);
        }

        waitResult(lstThread);

        //Check there is any failed request
        List<ParallelPeerRequest> results = getFailedList(lstReq);

        if(results.size() > 0) {
            printFailedHostnameAndReason(getFailedList(results));
            bRet = false;
        } else {
            bRet = true;
        }

        responseParams.put(RESULT, bRet);
        responseParams.put(SIZE, lSize);
        return bRet;
    }
}