package umn.dcsg.wieralocalserver.responses.peers;

import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.LocalServer;
import umn.dcsg.wieralocalserver.Locale;
import umn.dcsg.wieralocalserver.utils.Utils;

import java.util.List;
import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.*;
import static umn.dcsg.wieralocalserver.Constants.TARGET_LOCALE_LIST;

/**
 * Created by with IntelliJ IDEA.
 * User: Kwangsung
 * Date: 7/5/2018 2:32 PM
 * This response decrease -1 as local instance is chosen by default -> can be added later
 */
public class FindLocalesResponse extends PeerResponse {
    public FindLocalesResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
    }

    @Override
    public boolean doCheckPeerResponseConditions(Map<String, Object> responseParams) {
        return true;
    }

    @Override
    protected void InitRequiredParams() {

    }

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {

    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        //if target needs to be changed at run-time (e.g., TripS support)
        //Broadcast all
        List<Locale> targetLocaleList;
        int nPeerCnt = Utils.convertToInteger(responseParams.get(QUORUM));

        List<String> peersList = m_peerInstanceManager.getRandomPeerHostname(nPeerCnt-1);
        targetLocaleList = Locale.getLocalesWithoutTierName(peersList);
        targetLocaleList.add(Locale.getLocalesWithoutTierName(LocalServer.getHostName()));
        responseParams.put(TARGET_LOCALE_LIST, targetLocaleList);

        //Found
        for(int i=0;i<targetLocaleList.size();i++) {
            System.out.print(targetLocaleList.get(i).getHostName() + " ");
        }
        System.out.printf("\n");

        return true;
    }
}