package umn.dcsg.wieralocalserver.responses.consistency;

import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.LocalServer;


import umn.dcsg.wieralocalserver.responses.Response;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.*;

public class QuorumConsistencyResponse3  extends Response {
    List m_targetHostnameList = null;
    Map<String, LinkedList<Response>> m_opResponse;
    int m_nReadQuorum;
    int m_nWriteQuorum;
    int mode = 0;
    LinkedList opList = null;

    public QuorumConsistencyResponse3(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
        m_opResponse = new HashMap<>();
        opList = new LinkedList<>();

        if (m_initParams.containsKey(READ_QUORUM) == true) {
            m_nReadQuorum = (int) (double) m_initParams.get(READ_QUORUM);
            opList.add(Response.createResponse(instance, LOCK_GLOBAL_READ_RESPONSE, QUORUM_CONSISTENCY, params));
            opList.add(Response.createResponse(instance, GET_LATEST_DATA_RESPONSE, QUORUM_CONSISTENCY, params));
            opList.add(Response.createResponse(instance, UNLOCK_RESPONSE, QUORUM_CONSISTENCY, params));
            m_opResponse.put(ACTION_GET_EVENT, opList);
            mode = 2;
        }
        if (m_initParams.containsKey(WRITE_QUORUM) == true) {
            m_nWriteQuorum = (int) (double) m_initParams.get(WRITE_QUORUM);
            opList.add(Response.createResponse(instance, LOCK_GLOBAL_WRITE_RESPONSE, QUORUM_CONSISTENCY, params));
            opList.add(Response.createResponse(instance, GET_LATEST_VERSION_RESPONSE, QUORUM_CONSISTENCY, params));
            opList.add(Response.createResponse(instance, SET_LATEST_DATA, QUORUM_CONSISTENCY, params));
            opList.add(Response.createResponse(instance, UNLOCK_RESPONSE, QUORUM_CONSISTENCY, params));
            m_opResponse.put(ACTION_PUT_EVENT, opList);
            mode = 3;
        }


    }

    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(KEY);

        //Only for PUT operation
        if (m_initParams.containsKey(WRITE_QUORUM) == true) {

            m_lstRequiredParams.add(VALUE);
            //m_lstRequiredParams.add(VERSION);

        }
    }

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {

    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        List<String> peersList = null;

        if (mode == 2) {
            //distinguish write and read.
            peersList = m_localInstance.m_peerInstanceManager.getRandomPeers(m_nReadQuorum - 1);
            peersList.add(LocalServer.getHostName()); // also use self
        }else if(mode == 3){
            peersList = m_localInstance.m_peerInstanceManager.getRandomPeers(m_nWriteQuorum - 1);
            peersList.add(LocalServer.getHostName()); // also use self
        }
        responseParams.put(HOSTNAME_LIST, peersList);
        return Response.respondSequentiallyWithInstance(m_localInstance, m_opResponse.get(m_strRelatedEventType), responseParams);
    }
}