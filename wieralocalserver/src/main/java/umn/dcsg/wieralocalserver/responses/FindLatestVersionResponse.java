package umn.dcsg.wieralocalserver.responses;

import umn.dcsg.wieralocalserver.LocalInstance;

import javax.crypto.Cipher;
import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by ajay on 7/13/13.
 */
public class FindLatestVersionResponse extends Response {
    public FindLatestVersionResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
    }

    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(KEY);
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        int nTotalPeerCnt = m_localInstance.m_peerInstanceManager.getPeersList().size();
        int read_quorum_version = nTotalPeerCnt / 2 + 1;


        return false;
    }

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {

    }
}