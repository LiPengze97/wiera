
package umn.dcsg.wieralocalserver.client;


import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
// import org.apache.thrift.transport.TTransportException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.mortbay.util.ajax.JSON;
import umn.dcsg.wieralocalserver.PolicyGenerator;

import umn.dcsg.wieralocalserver.thriftinterfaces.ApplicationToWieraIface;
// import umn.dcsg.wieralocalserver.utils.Utils;



import static umn.dcsg.wieralocalserver.Constants.*;

public class WieraCentralClient {
    
    protected ApplicationToWieraIface.Client wieraCentralClient = null;
    protected String strWieraID = "";
    protected JSONArray wieraLocalInstancesList = null;
    protected JSONArray wieraLocalServersList = null;
    protected boolean setpolicy = false;


    public WieraCentralClient(String strIPAddress, int nPort){
        TTransport transport;

        transport = new TSocket(strIPAddress, nPort);
        TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
        ApplicationToWieraIface.Client client = new ApplicationToWieraIface.Client(protocol);

        try {
            transport.open();
        } catch (TException x) {
            System.out.println(x.getMessage());

        }
        this.wieraCentralClient = client;
    }

    public JSONArray getLocalStorageServers(){
        if(wieraCentralClient != null){
            try{
                String strResult = wieraCentralClient.getLocalServerList();
                JSONObject res = new JSONObject(strResult);

                boolean bResult = (boolean) res.get(RESULT);
                if (bResult == true) {
                    wieraLocalServersList = (JSONArray) res.get(VALUE);
                }else{
                    System.out.println("[debug] Failed to get local storage servers.");
                    return null;
                }
            }catch (TException e) {
                e.printStackTrace();
            }
            return wieraLocalServersList;
        }else{
            System.out.println("[debug] Not connected to a Wiera central server.");   
            return null;
        }
    }

    public JSONArray getLocalStorageInstances(){
        if(wieraCentralClient == null || strWieraID.equals("")){
            System.out.println("[debug] ");
            return null;
        }else{
            try {
                String strResult = wieraCentralClient.getInstances(strWieraID);
                //This will contain list of LocalInstance instance as a JsonObject
                JSONObject res = new JSONObject(strResult);
                //getObject list of local instance.
                boolean bResult = (boolean) res.get(RESULT);
                if (bResult == true) {
                    //Get instance List
                    wieraLocalInstancesList = (JSONArray) res.get(VALUE);
                    return wieraLocalInstancesList;
                } else {
                    String reason = (String) res.get(VALUE);
                    System.out.println("[debug] Failed to get instances list:  " + reason);
                    return null;
                }
            } catch (TException e) {
                e.printStackTrace();
            }
            return null;
        }
    }
    public JSONArray getDefaultLocalStorageInstance(){
        if(wieraLocalInstancesList != null){
            return (JSONArray) wieraLocalInstancesList.get(0);
        }else{

        }
        return null;
    }
    public JSONArray getLocalStorageInstances(String hostname) {
        if(wieraLocalInstancesList != null){
            JSONArray instance;
            for(int i = 0; i < wieraLocalInstancesList.length(); i++){
                instance = (JSONArray) wieraLocalInstancesList.get(i);
                if(hostname.equals((String)instance.get(0))){
                    return instance;
                }
            }
        }else{

        }
        return null;

    }
    public WieraLocalInstanceClient getLocalInstance(String strIPAddress, int nPort){
        return new WieraLocalInstanceClient(strIPAddress, nPort);
    }
    
    public void printLocalStorageInstances(){
        if(wieraLocalInstancesList == null){
            getLocalStorageInstances();
        }
        if(wieraLocalInstancesList != null){
            System.out.println("------------------------  Instance List  ------------------------");
            int len = wieraLocalInstancesList.length();
            for (int i = 0; i < len; i++) {
                JSONArray instance = (JSONArray) wieraLocalInstancesList.get(i);
                System.out.format("Hostname: %s, IP: %s, Port: %d\n", instance.get(0), instance.get(1), instance.get(2));
            }
        
        }else{
            System.out.println("[debug] ");
        }
    }
     

    public boolean startPolicy(String strPolicyPath){
        if(wieraCentralClient != null){
            try {
                JSONObject jsonReq = PolicyGenerator.loadPolicy(strPolicyPath);
                if (jsonReq == null) {
                    System.out.println("[debug] Failed to open policy file: " + strPolicyPath);
                    return false;
                }
                String strReq = jsonReq.toString();
                String strResult = wieraCentralClient.startInstances(strReq);
                JSONObject res = new JSONObject(strResult);
                boolean bResult = (boolean) res.get(RESULT);
                //this will include WieraID or reason if failed


                if (bResult == true) {
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    strWieraID = (String) res.get(VALUE);
                    setpolicy = true;
                    return true;
                } else {
                    String reason = (String) res.get(VALUE);
                    System.out.println("[debug] Failed to get local servers list: " + reason);
                    return false;
                }
            }catch(TException e){
                e.printStackTrace();
            }
            return false;
        }else{
            System.out.println("[debug] Not connected to a Wiera central server.");   
            return false;
        }
    }
    public boolean stopPolicy(){
        if(wieraCentralClient == null || strWieraID.equals("")){
            System.out.println("[debug] ");
            return false;
        }else{
        //Create instance here with global Policy.
            try{
                JSONObject jsonReq = new JSONObject();
                jsonReq.put(ID, strWieraID);
                String strReq = jsonReq.toString();
                String strResult = wieraCentralClient.stopInstances(strReq);
                JSONObject res = new JSONObject(strResult);
                boolean bResult = (boolean) res.get(RESULT);

                if (bResult == true) {
                    String wieraId = (String) res.get(VALUE);
                    System.out.println("[debug] Stop " + wieraId +" instances successfully.");
                    setpolicy = false;
                    return true;
                } else {
                    String reason = (String) res.get(VALUE);
                    System.out.println("[debug] Failed to stop instance reason: " + reason);
                    return false;
                }
            }catch(TException e){
                e.printStackTrace();
            }
        }
        return false;
    }
    public boolean isSetpolicy(){
        return setpolicy;
    }


}