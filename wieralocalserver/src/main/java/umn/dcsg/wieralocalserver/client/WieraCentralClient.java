
package umn.dcsg.wieralocalserver.client;


import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import org.json.JSONArray;
import org.json.JSONObject;
import umn.dcsg.wieralocalserver.PolicyGenerator;

import umn.dcsg.wieralocalserver.thriftinterfaces.ApplicationToWieraIface;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * WieraCentralClient is used to create, configure and manage the Wiera Local Storage instances.
 *
 *
 * <p>How to use.</p>
 * <ol>
 *  <li><code>getLocalStorageServers</code> Checks the available local servers. And prepare your policy.</li>
 *  <li><code>startPolicy(policyPath)</code> Launches this policy and corresponding local instances.</li>
 *  <li><code>getLocalStorageInstances()</code> Checks these launched instances, and find their IPs and Ports.</li>
 *  <li><code>getLocalInstance(strIPAddress, nPort)</code> Obtains a instance's client.</li>
 *  </ol>
 *  @see WieraLocalInstanceClient
 * */

public class WieraCentralClient {


    protected ApplicationToWieraIface.Client wieraCentralClient = null;
    protected String strWieraID = "";
    protected JSONArray wieraLocalInstancesList = null;
    protected JSONArray wieraLocalServersList = null;
    protected boolean setpolicy = false;

    /**
     * Creates a client object that is used to create, configure and manage the Wiera Local Storage instances.
     *
     * @param strIPAddress The IP address of the Wiera central server.
     * @param nPort The port number of the Wiera central server service.
     *
     * */
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
    /**
     * Gets the registered available local storage servers' info.
     *
     * The format of each entry in the array is defined as ["hostname", "IP-address"].
     * @return An array of local storage servers' info.
     * */

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
    /**
     * Prints all the available servers' info.
     *
     *
     * */
    public void printLocalServers(){
        if(wieraLocalServersList == null){
            getLocalStorageServers();
        }
        if(wieraLocalServersList != null){
            System.out.println("------------------------  Local Server List  ------------------------");
            int len = wieraLocalServersList.length();
            for (int i = 0; i < len; i++) {
                JSONArray server = (JSONArray) wieraLocalServersList.get(i);
                System.out.format("Hostname: %s, IP: %s\n", server.get(0), server.get(1));
            }

        }else{
            System.out.println("[debug] ");
        }
    }
    /**
     * Gets the information of the launched local storage instances from local servers.
     *
     * The format of each entry in the array is defined as ["hostname", "IP-address", nPort].
     * @return An array of launched local storage instances' info; null if the policy is not setup.
     * @see WieraCentralClient#startPolicy(String)
     * */
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
    /**
     * Gets a default local storage instances.
     *
     * @return An array that stores the info about the default instance; null if the policy is not setup.
     * @see WieraCentralClient#startPolicy(String)
     *
     * */

    public JSONArray getDefaultLocalStorageInstance(){
        if(wieraLocalInstancesList != null){
            return (JSONArray) wieraLocalInstancesList.get(0);
        }else{

        }
        return null;
    }

    /**
     * Gets a local storage instances according to the supplied hostname.
     *
     * @param hostname The desired instance's hostname.
     * @return An array that stores the info about this instance; null if the instance is not found.
     *
     * */
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
    /**
     * Gets a client of the local storage instance.
     *
     * Creates a concrete local instance's client, applications can use this client to store and retrieve data according to
     * the set policy.
     * @param strIPAddress The IP address of the local server that runs this storage instance.
     * @param nPort The port number where the storage instance listens a client.
     * @return A client of the local storage instance.
     * @see WieraLocalInstanceClient
     * */

    public WieraLocalInstanceClient getLocalInstance(String strIPAddress, int nPort){
        return new WieraLocalInstanceClient(strIPAddress, nPort);
    }

    /**
     * Prints all the launched storage instance' info.
     *
     * Usually call before creating the local instance's client.
     * @see WieraCentralClient#getLocalInstance(String, int)
     *
     * */
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
    /**
     * Start a <i href="">policy file</i> and launch corresponding local instances.
     *
     * @param strPolicyPath The location of the policy file.
     * @return True if successful, otherwise false.
     * */

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
    /**
     * Start the launched policy.
     *
     *
     * @return True if successful, otherwise false.
     * */
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

    /**
     * Checks whether a policy is started.
     *
     * @return True if already started a policy, otherwise false.
     * */
    public boolean isSetpolicy(){
        return setpolicy;
    }


}