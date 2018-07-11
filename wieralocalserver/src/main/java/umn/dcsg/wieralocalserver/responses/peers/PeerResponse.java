package umn.dcsg.wieralocalserver.responses.peers;

import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.Locale;
import umn.dcsg.wieralocalserver.PeerInstancesManager;
import umn.dcsg.wieralocalserver.datadistribution.ParallelPeerRequest;
import umn.dcsg.wieralocalserver.responses.Response;
import umn.dcsg.wieralocalserver.thriftinterfaces.LocalInstanceToPeerIface;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by with IntelliJ IDEA.
 * User: Kwangsung
 * Date: 7/3/2018 12:15 PM
 */
public abstract class PeerResponse extends Response {
    /**
     * Constructor with response param
     * Response can be generated with policy or other response dynamically
     *
     * @param localInstance LocalInstance localInstance
     * @param strEventName
     * @param params        Reponse class to handle the request (or event)  @return void
     * @see Response
     */

    protected PeerInstancesManager m_peerInstanceManager = null;
    protected List m_peerHostnameList = null;

    public PeerResponse(LocalInstance localInstance, String strEventName, Map<String, Object> params) {
        super(localInstance, strEventName, params);
        m_peerInstanceManager = localInstance.m_peerInstanceManager;
    }

    protected LocalInstanceToPeerIface.Client getPeerClient(String strHostname) {
        return m_peerInstanceManager.getPeerClient(strHostname);
    }

    protected void releasePeerClient(String strHostName, LocalInstanceToPeerIface.Client peerClient) {
        m_peerInstanceManager.releasePeerClient(strHostName, peerClient);
    }

    protected List<String> getPeersHostnameList() {
        return m_peerInstanceManager.getPeersHostnameList();
    }

    protected Map<Thread, ParallelPeerRequest> sendRequestInParallel(String strRequestType,
                                        List<Locale> lstTargetLocales,
                                        String strKey, int nVer,
                                        long lSize, byte[] value, String strTag,
                                        long lLastModifiedTime, boolean bOnlyMetaInfo, boolean bWaitResult) {
        Thread senderThread;
        ParallelPeerRequest request = null;
        Map<Thread, ParallelPeerRequest> senderList = new HashMap<>();

        //Send request to all
        for (Locale locale : lstTargetLocales) {

            request = new ParallelPeerRequest(m_localInstance.m_peerInstanceManager, strRequestType,
                    locale.getHostName(), strKey, nVer, lSize, value, locale.getTierName(), strTag, lLastModifiedTime, bOnlyMetaInfo);
            if (request != null) {
                //Thread run
                senderThread = new Thread(request);
                senderThread.start();

                //Add to map
                senderList.put(senderThread, request);
            }
        }

        if(bWaitResult == true) {
            waitResult(new LinkedList<>(senderList.keySet()));
        }

        return senderList;
    }

    protected boolean waitResult(List<Thread> lstThread) {
        //Wait all - should be no much overhead for waiting each instance
        for (Thread senderT : lstThread) {
            try {
                senderT.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
                return false;
            }
        }

        return true;
    }

    protected List<ParallelPeerRequest> getFailedList(List<ParallelPeerRequest> lstRequest) {
        //Wait all - should be no much overhead for waiting each instance
        List<ParallelPeerRequest> failedHost = new LinkedList<>();

        for (ParallelPeerRequest req : lstRequest) {
            if (req.getResult() == false) {
                failedHost.add(req);
            }
        }

        return failedHost;
    }

    protected void printFailedHostnameAndReason (List<ParallelPeerRequest> lstRequest) {
        if (lstRequest.size() > 0) {
            StringBuilder builder = new StringBuilder();
            builder.append("Key: ");
            builder.append(" - Failed on below peers.\n");
            builder.append("-------------------------------------\n");

            for (ParallelPeerRequest req : lstRequest) {
                builder.append(req.getTargetHostname() + " Reason: " + req.getResult() + '\n');
            }

            System.out.println(builder.toString());
        }
    }

    @Override
    public boolean doCheckResponseConditions(Map<String, Object> responseParams) {
        if (m_localInstance.isStandAloneMode() == true) {
            //Result
            responseParams.put(RESULT, true);
            responseParams.put(REASON, getClass().getSimpleName() + " cannot response in LocalInstance stand-alone mode");
            return false;
        }

        return doCheckPeerResponseConditions(responseParams);
    }

    public abstract boolean doCheckPeerResponseConditions(Map<String, Object> responseParams);
}
