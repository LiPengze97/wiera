package umn.dcsg.wieralocalserver.responses.peers.consistency;

import umn.dcsg.wieralocalserver.Locale;
import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.responses.*;
import umn.dcsg.wieralocalserver.responses.peers.PeerResponse;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by Kwangsung on 9/29/2015.
 */
public class MultiplePrimariesConsistencyResponse extends PeerResponse {
    List <Response> m_lstResponse = new LinkedList<>();

    public MultiplePrimariesConsistencyResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);

        if(strEventName.equals(ACTION_PUT_EVENT) == true) {
            //Put Operation
            m_lstResponse.add(Response.createResponse(instance, LOCK_GLOBAL_WRITE_RESPONSE, MULTIPLE_PRIMARIES_CONSISTENCY, params));
            m_lstResponse.add(Response.createResponse(instance, STORE_RESPONSE, MULTIPLE_PRIMARIES_CONSISTENCY, params));
            m_lstResponse.add(Response.createResponse(instance, BROADCAST_RESPONSE, MULTIPLE_PRIMARIES_CONSISTENCY, params));
            m_lstResponse.add(Response.createResponse(instance, UNLOCK_GLOBAL_RESPONSE, MULTIPLE_PRIMARIES_CONSISTENCY, params));
        } else if (strEventName.equals(ACTION_GET_EVENT) == true){
            //Get Operation
            m_lstResponse.add(Response.createResponse(instance, LOCK_GLOBAL_READ_RESPONSE, MULTIPLE_PRIMARIES_CONSISTENCY, params));
            m_lstResponse.add(Response.createResponse(instance, RETRIEVE_RESPONSE, MULTIPLE_PRIMARIES_CONSISTENCY, params));
            m_lstResponse.add(Response.createResponse(instance, UNLOCK_GLOBAL_RESPONSE, MULTIPLE_PRIMARIES_CONSISTENCY, params));
        }
    }

    @Override
    protected void InitRequiredParams() {
        //For all operations type
        m_lstRequiredParams.add(KEY);

        //Only for PUT operation
        if (m_strEventName.equals(ACTION_PUT_EVENT) == true) {
            m_lstRequiredParams.add(VALUE);
        }
    }

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {
        if (responseParams.containsKey(TARGET_LOCALE_LIST) == false) {
            if (m_peerHostnameList == null || m_peerHostnameList.size() == 0 ||
                    ((m_peerHostnameList.size() == 1) && (m_peerHostnameList.get(0).equals(ALL) == true))) {
                m_peerHostnameList = getPeersHostnameList();
            }

            responseParams.put(TARGET_LOCALE_LIST, Locale.getLocalesWithoutTierName(m_peerHostnameList));
        }
    }

    @Override
    public boolean doCheckPeerResponseConditions(Map<String, Object> responseParams) {
        return true;
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        return Response.respondSequentiallyWithInstance(m_localInstance, m_lstResponse, responseParams);
    }
}