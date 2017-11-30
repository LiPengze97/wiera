package umn.dcsg.wieralocalserver.responses;

import org.apache.thrift.TException;
import org.json.JSONObject;
import org.omg.PortableInterceptor.NON_EXISTENT;
import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.thriftinterfaces.LocalInstanceToPeerIface;
import umn.dcsg.wieralocalserver.utils.Utils;

import javax.rmi.CORBA.Util;
import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * DebugResponse:
 *  Print the current params the inherited from last response.
 *
 *  initParam:
 *  in-params:debugID
 *
 *  out-params: result:reason
 * */


public class DebugResponse extends Response {
    private String debugID = "Unknown";
    public DebugResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
    }

    @Override
    protected void InitRequiredParams() {

    }

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {
        if(responseParams.containsKey("debugID")){
            debugID = (String)responseParams.get("debugID");
        }else if(m_initParams.containsKey("debugID")){
            debugID = (String)m_initParams.get("debugID");
        }
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        return testPeerLock(responseParams);
    }
    public boolean testPeerLockUnlock(Map<String, Object> responseParams) {

        /*for (Map.Entry<String, Object> entry : responseParams.entrySet()) {
            System.out.println("[DebugResponse::" + debugID + " -> " + entry.getKey() + " : " + entry.getValue());
        }*/
        //Verify peer's interface
        LocalInstanceToPeerIface.Client peerClient = m_localInstance.m_peerInstanceManager.getPeerClient("linux-vm-4");

        JSONObject lockReq = new JSONObject();
        lockReq.put(KEY, "Hello");
        lockReq.put(IS_WRITE, true);
        try {
            String result2 = peerClient.getClusterLock(lockReq.toString());
            String result = peerClient.releaseClusterLock(lockReq.toString());
            JSONObject data = new JSONObject(result);
            if( data.getBoolean(RESULT)){
                System.out.println("OK");
            }else{
                System.out.println("fails");
            }


            responseParams.put(RESULT, true);
            responseParams.put(REASON, NOT_HANDLED);
        }catch(TException e){
            System.out.println("Error");
        }

        return true;
    }
    public boolean testPeerLock(Map<String, Object> responseParams) {

        /*for (Map.Entry<String, Object> entry : responseParams.entrySet()) {
            System.out.println("[DebugResponse::" + debugID + " -> " + entry.getKey() + " : " + entry.getValue());
        }*/
        //Verify peer's interface
        LocalInstanceToPeerIface.Client peerClient = m_localInstance.m_peerInstanceManager.getPeerClient("linux-vm-4");

        JSONObject lockReq = new JSONObject();
        lockReq.put(KEY, "Hello");
        lockReq.put(IS_WRITE, true);
        try {
            String result = peerClient.getClusterLock(lockReq.toString());
            JSONObject data = new JSONObject(result);
            if( data.getBoolean(RESULT)){
                System.out.println("OK");
            }else{
                System.out.println("fails");
            }


            responseParams.put(RESULT, true);
            responseParams.put(REASON, NOT_HANDLED);
        }catch(TException e){
            System.out.println("Error");
        }

        return true;
    }
    public boolean testPeerPut(Map<String, Object> responseParams) {

        /*for (Map.Entry<String, Object> entry : responseParams.entrySet()) {
            System.out.println("[DebugResponse::" + debugID + " -> " + entry.getKey() + " : " + entry.getValue());
        }*/
        //Verify peer's interface
        LocalInstanceToPeerIface.Client peerClient = m_localInstance.m_peerInstanceManager.getPeerClient("linux-vm-4");
        JSONObject req = new JSONObject();
        JSONObject resultC = new JSONObject();
        req.put(KEY, "ssabc");
        req.put(VERSION, 0);
        req.put(VALUE, Utils.encodeBytes("12hr329ruh".getBytes()));
        try {
            String result = peerClient.put(req.toString());
            JSONObject data = new JSONObject(result);
            if( data.getBoolean(RESULT)){
                System.out.println("OK");
            }else{
                System.out.println("fails");
            }


            responseParams.put(RESULT, true);
            responseParams.put(REASON, NOT_HANDLED);
        }catch(TException e){
            System.out.println("Error");
        }

        return true;
    }
    public boolean testPeerGet(Map<String, Object> responseParams) {

        /*for (Map.Entry<String, Object> entry : responseParams.entrySet()) {
            System.out.println("[DebugResponse::" + debugID + " -> " + entry.getKey() + " : " + entry.getValue());
        }*/
        //Verify peer's interface
       LocalInstanceToPeerIface.Client peerClient = m_localInstance.m_peerInstanceManager.getPeerClient("linux-vm-4");
       JSONObject req = new JSONObject();
       JSONObject resultC = new JSONObject();
       req.put(KEY, "a");
       req.put(FROM, "linux-vm-4");
       try {
            String result = peerClient.get(req.toString());
            JSONObject data = new JSONObject(result);
            String v = data.getString(VALUE);
            byte [] u = Utils.decodeBytes(v);
            System.out.println(new String(u));

            responseParams.put(RESULT, true);
            responseParams.put(REASON, NOT_HANDLED);
       }catch(TException e){
            System.out.println("Error");
       }

        return true;
    }
}