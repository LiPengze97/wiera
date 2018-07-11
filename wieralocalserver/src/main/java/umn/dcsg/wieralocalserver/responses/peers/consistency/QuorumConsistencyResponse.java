package umn.dcsg.wieralocalserver.responses.peers.consistency;

import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.responses.Response;
import umn.dcsg.wieralocalserver.responses.peers.PeerResponse;
import umn.dcsg.wieralocalserver.utils.Utils;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by Kwangsung on 11/28/2016.
 * This is an example for strong consistency using Quorum.
 */
public class QuorumConsistencyResponse extends PeerResponse {
    Map<String, LinkedList<Response>> m_opResponse = new HashMap<>();
    List<Response> m_lstResponse = new LinkedList<>();

    //Set static
    static protected int m_nReadQuorum = 0;
    static protected int m_nWriteQuorum = 0;

    public QuorumConsistencyResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);

        if(strEventName.equals(ACTION_PUT_EVENT) == true) {
            //Put Operation
            //Get write lock
            m_lstResponse.add(Response.createResponse(instance, LOCK_GLOBAL_WRITE_RESPONSE, QUORUM_CONSISTENCY, params));
            //Get random instance for latest version
            m_lstResponse.add(Response.createResponse(instance, FIND_LOCALES, QUORUM_CONSISTENCY, params));
            //find the latest version
            m_lstResponse.add(Response.createResponse(instance, FIND_LATEST_VERSION_NUMBER, QUORUM_CONSISTENCY, params));
            //Increase version number
            m_lstResponse.add(Response.createResponse(instance, INCREASE_VERSION, QUORUM_CONSISTENCY, params));
            //Store locally
            m_lstResponse.add(Response.createResponse(instance, STORE_RESPONSE, QUORUM_CONSISTENCY, params));
            //Get random instance for broadcasting
            m_lstResponse.add(Response.createResponse(instance, FIND_LOCALES, QUORUM_CONSISTENCY, params));
            //Broadcasting to targets
            m_lstResponse.add(Response.createResponse(instance, BROADCAST_RESPONSE, QUORUM_CONSISTENCY, params));
            //Releasing write lock
            m_lstResponse.add(Response.createResponse(instance, UNLOCK_GLOBAL_RESPONSE, QUORUM_CONSISTENCY, params));
        } else if (strEventName.equals(ACTION_GET_EVENT) == true) {
            m_lstResponse.add(Response.createResponse(instance, LOCK_GLOBAL_READ_RESPONSE, QUORUM_CONSISTENCY, params));
            m_lstResponse.add(Response.createResponse(instance, FIND_LOCALES, QUORUM_CONSISTENCY, params));
            m_lstResponse.add(Response.createResponse(instance, FIND_LATEST_VERSION_NUMBER, QUORUM_CONSISTENCY, params));
            m_lstResponse.add(Response.createResponse(instance, RETRIEVE_RESPONSE, QUORUM_CONSISTENCY, params));
            m_lstResponse.add(Response.createResponse(instance, UNLOCK_GLOBAL_RESPONSE, QUORUM_CONSISTENCY, params));
        }

        setQuorum();
    }

    void setQuorum() {
        //Set quorum.
        if(m_initParams.containsKey(QUORUM) == true) {
            if(m_strEventName.equals(ACTION_PUT_EVENT) == true) {
                m_nWriteQuorum = Utils.convertToInteger(m_initParams.get(QUORUM));
            } else if (m_strEventName.equals(ACTION_GET_EVENT) == true) {
                m_nReadQuorum = Utils.convertToInteger(m_initParams.get(QUORUM));
            }
        } else {
            //From global configuration
            m_nWriteQuorum = m_localInstance.getPolicyConfInt(WRITE_QUORUM);
            m_nReadQuorum = m_localInstance.getPolicyConfInt(READ_QUORUM);
        }

        //Setting is not good for strong consistency
        //Then consiter number of peers and set half write and half read (+1)
        if(isValidQuorum() == false) {
            int nPeerNumber = m_peerInstanceManager.getExpectedPeerInstanceCnt();
            m_nWriteQuorum = nPeerNumber/2;
            m_nReadQuorum = nPeerNumber/2;

            if(nPeerNumber%2 == 0) {
                m_nReadQuorum++;
            } else {
                m_nWriteQuorum++;
                m_nReadQuorum++;
            }
        }
    }

    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(KEY);

        //Only for PUT operation
        if (m_strEventName.equals(ACTION_PUT_EVENT) == true) {
            m_lstRequiredParams.add(VALUE);
        }
    }

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {
        //RunTime update
        if(responseParams.containsKey(QUORUM) == true) {
            int nQuorum = Utils.convertToInteger(responseParams.get(QUORUM));

            if(m_strEventName.equals(ACTION_PUT_EVENT) == true) {
                m_nWriteQuorum = nQuorum;
            } else if (m_strEventName.equals(ACTION_GET_EVENT) == true) {
                m_nReadQuorum = nQuorum;
            }
        } else {
            if(m_strEventName.equals(ACTION_PUT_EVENT) == true) {
                responseParams.put(QUORUM, m_nWriteQuorum);
            } else if (m_strEventName.equals(ACTION_GET_EVENT) == true) {
                responseParams.put(QUORUM, m_nReadQuorum);
            }
        }
    }

    @Override
    public boolean doCheckPeerResponseConditions(Map<String, Object> responseParams) {
        if (isValidQuorum() == false) {
            String strReason = String.format("Quorum is not valid: r:%d w:%d, total: %d", m_nReadQuorum, m_nWriteQuorum, m_peerInstanceManager.getPeersList().size());
            responseParams.put(RESULT, false);
            responseParams.put(REASON, strReason);
            return false;
        }

        if (m_localInstance.isVersionSupported() == false) {
            responseParams.put(RESULT, false);
            responseParams.put(REASON, "Versioning is not supported in local instances.");
            return false;
        }

        return true;
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        return Response.respondSequentiallyWithInstance(m_localInstance, m_lstResponse, responseParams);
   }

    private boolean isValidQuorum() {
        int nTotalPeerCnt = m_peerInstanceManager.getPeersList().size();
        return m_nReadQuorum + m_nWriteQuorum > nTotalPeerCnt && m_nWriteQuorum > nTotalPeerCnt / 2;
    }
}