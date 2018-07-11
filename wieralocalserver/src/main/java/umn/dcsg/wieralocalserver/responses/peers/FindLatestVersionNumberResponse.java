package umn.dcsg.wieralocalserver.responses.peers;

import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.LocalServer;
import umn.dcsg.wieralocalserver.Locale;
import umn.dcsg.wieralocalserver.datadistribution.ParallelPeerRequest;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.*;
import static umn.dcsg.wieralocalserver.MetaObjectInfo.LATEST_VERSION;
import static umn.dcsg.wieralocalserver.MetaObjectInfo.NO_SUCH_VERSION;

/**
 * Created by with IntelliJ IDEA.
 * User: Kwangsung
 * Date: 6/28/2018 6:26 PM
 */
public class FindLatestVersionNumberResponse extends PeerResponse {
    int m_nLatestVersion = NO_SUCH_VERSION;

    public FindLatestVersionNumberResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
        m_peerHostnameList = new LinkedList<>();
    }

    @Override
    public boolean doCheckPeerResponseConditions(Map<String, Object> responseParams) {
        return true;
    }

    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(KEY);
     }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        //if target needs to be changed at run-time (e.g., TripS support)
        //Broadcast all
        //This instance can be reused
        m_nLatestVersion = NO_SUCH_VERSION;

        String strKey = (String) responseParams.get(KEY);
//      if(Response.respondAtRuntimeWithClass(m_localInstance, FindLocalesResponse.class, responseParams) == true) {
        m_peerHostnameList.clear();
        List<Locale> targetLocaleList = (List<Locale>)responseParams.get(TARGET_LOCALE_LIST);

        //Check local locale first if local is included
        if(targetLocaleList.contains(Locale.getLocalesWithoutTierName(LocalServer.getHostName())) == true) {
            m_nLatestVersion = m_localInstance.getLatestVersion(strKey);
            m_peerHostnameList.add(LocalServer.getHostName());

            //Remove for peer
            targetLocaleList.remove(Locale.getLocalesWithoutTierName(LocalServer.getHostName()));
        }

        //Assume that the global lock is alread acquired
        Map<Thread, ParallelPeerRequest> reqSent = sendRequestInParallel(GET_LASTEST_VERSION_PEER, targetLocaleList,
                strKey, LATEST_VERSION, 0, "".getBytes(), null, 0, false, true);

        //Find cloeset target host that has the latest version of value
        getHostnameWithLatestVersion(new LinkedList<>(reqSent.values()));

        //Set for next response
        responseParams.put(VERSION, m_nLatestVersion);
        responseParams.put(TARGET_LOCALE_LIST, Locale.getLocalesWithoutTierName(m_peerHostnameList));

        //For now item in the first.
        //This can be local or remote
        responseParams.put(TARGET_LOCALE, Locale.getLocalesWithoutTierName((String)m_peerHostnameList.get(0)));
        return true;
/*    }
       else {
          return false;
        }*/
    }

    public void getHostnameWithLatestVersion(List<ParallelPeerRequest> lstRequest) {
        int nVer = NO_SUCH_VERSION;

        for(ParallelPeerRequest req: lstRequest) {
            nVer = req.getVersion();

            if(nVer >= m_nLatestVersion) {
                if (nVer > m_nLatestVersion) {
                    m_nLatestVersion = nVer;
                    m_peerHostnameList.clear();
                }

                m_peerHostnameList.add(req.getTargetHostname());
            }
        }
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
}