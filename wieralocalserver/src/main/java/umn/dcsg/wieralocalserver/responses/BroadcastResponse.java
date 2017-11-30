package umn.dcsg.wieralocalserver.responses;

import com.google.gson.Gson;
import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.Locale;
import umn.dcsg.wieralocalserver.datadistribution.Updater;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Bugs:
 *  1. Some
 *
 * Created by Kwangsung on 7/26/2017.
 */
public class BroadcastResponse extends Response {
    private List m_targetHostnameList = null;

    public BroadcastResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);

        if (instance.isStandAloneMode() == false) {
            if (params.containsKey(TO) == true) {
                m_targetHostnameList = (List) params.get(TO);
            }
        }
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
        String strReason;

        String strKey = (String) responseParams.get(KEY);
        int nVer = (int) responseParams.get(VERSION);
        byte[] value = (byte[]) responseParams.get(VALUE);
        long lSize = value.length;
        long lLastModifiedTime = (long) responseParams.get(LAST_MODIFIED_TIME);
        String strTag = (String) responseParams.get(TAG);
        List<Locale> targetLocaleList;

        //if target needs to be changed at run-time (e.g., TripS support)
        if (responseParams.containsKey(TARGET_LOCALES) == true) {
            targetLocaleList = (List) responseParams.get(TARGET_LOCALES);
        } else {
            if (m_targetHostnameList == null || ((m_targetHostnameList.size() == 1) && (m_targetHostnameList.get(0).equals(ALL) == true))) {
                m_targetHostnameList = m_localInstance.m_peerInstanceManager.getPeersHostnameList();
            }

            targetLocaleList = Locale.getLocalesWithoutTierName(m_targetHostnameList);
        }

        //For selective broadcasting
        //LinkedList<String> lstForMeta = (LinkedList)((LinkedList) responseParams.get(ALL_PEER_HOSTNAMES)).clone();
        LinkedList<String> lstForMeta = (LinkedList) m_localInstance.m_peerInstanceManager.getPeersHostnameList();

        Thread senderThread;
        Updater updater = null;
        Map<Thread, Updater> senderList = new HashMap<>();

        //Send to all but check whether a peer is included in the target list
        //if not only meta data needs to be broadcasted
        for (Locale locale : targetLocaleList) {
            //System.out.println("Sent to :" + locale.getHostName() + " tier: " + locale.getTierName());
            //If target is in the targetlist send value.
            updater = new Updater(m_localInstance.m_peerInstanceManager, locale.getHostName(), strKey, nVer, lSize, value, locale.getTierName(), strTag, lLastModifiedTime, false);

            //Remove from the lstForMeta for sending meta
            lstForMeta.remove(locale.getHostName());
        }

        //Now send meta data to the rest of peer
        for (String strTargetHostname : lstForMeta) {
            //System.out.println("For meta update: " + strTargetHostname);

            //Need to send metadata (where replicas are stored to be accessed later)
            Gson gson = new Gson();
            String strList = gson.toJson(targetLocaleList);
            updater = new Updater(m_localInstance.m_peerInstanceManager, strTargetHostname, strKey, nVer, 0, "".getBytes(), strList, strTag, lLastModifiedTime, true);
        }

        if (updater != null) {
            //Thread run
            senderThread = new Thread(updater);
            senderThread.start();

            //Add to map
            senderList.put(senderThread, updater);
        }

        //Wait all - should be no much overhead for waiting each instance
        for (Thread senderT : senderList.keySet()) {
            try {
                bRet = true;
                senderT.join();
            } catch (InterruptedException e) {
                bRet = false;
                strReason = e.getMessage();
            }
        }

        //System.out.format("[debug]%d ms takes to broadcast in %s.\n", latency.getBroadcastTime(), m_strDistributionTypeName);
        //All done. Now check the reason
        //Wait all - should be no much overhead for wating each instance
        Map<String, String> failedHost = new HashMap<>();

        for (Updater updaterV : senderList.values()) {
            if (updaterV.getReason() != null) {
                failedHost.put(updaterV.getTargetHostName(), updaterV.getReason());
            }
        }

        if (failedHost.size() > 0) {
            StringBuilder builder = new StringBuilder();
            builder.append("Updating value fails on below peers.\n");
            builder.append("-------------------------------------\n");

            for (String strHostName : failedHost.keySet()) {
                strReason = failedHost.get(strHostName);
                builder.append(strHostName + " Reason: " + strReason + '\n');
            }

            //Result
            responseParams.put(REASON, builder.toString());
            bRet = false;
        } else {
            bRet = true;
        }

        responseParams.put(RESULT, bRet);
        return bRet;
    }
}