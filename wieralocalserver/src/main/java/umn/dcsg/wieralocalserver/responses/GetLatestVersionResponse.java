

package umn.dcsg.wieralocalserver.responses;

import org.apache.thrift.TException;
import org.json.JSONObject;
import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.LocalServer;
import umn.dcsg.wieralocalserver.MetaObjectInfo;
import umn.dcsg.wieralocalserver.thriftinterfaces.LocalInstanceToPeerIface.Client;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static umn.dcsg.wieralocalserver.Constants.*;


/**
 * initPara
 * in-para:
 *  Key:Hostname_List
 *
 * out-para:
 *  RESULT[:VERSION:REASON]
 *
 *
 */
public class GetLatestVersionResponse extends Response {
    protected String m_selfHostname;

    protected ConcurrentHashMap<String, JSONObject> mainResult;
    protected JSONObject returnResult;
    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(KEY);
        m_lstRequiredParams.add(HOSTNAME_LIST);
    }

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {

    }


    public GetLatestVersionResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
        m_selfHostname = LocalServer.getHostName();
    }


    @Override
    public boolean respond(Map<String, Object> responseParams) {
        mainResult = new ConcurrentHashMap<>();
        List<String> hostnames;
        String strKey;
        boolean includeSelf = false;
        if(responseParams.containsKey(HOSTNAME_LIST)){
            hostnames = (List<String>) responseParams.get(HOSTNAME_LIST);
        }else {
            hostnames = (List<String>) m_initParams.get(HOSTNAME_LIST);
        }
        if(responseParams.containsKey(KEY)){
            strKey = (String) responseParams.get(KEY);
        }else {
            strKey = (String) m_initParams.get(KEY);
        }

        List<Thread> workers = new LinkedList<>();
        int latestVersion = MetaObjectInfo.NO_SUCH_KEY;
        Thread worker;
        Runnable run;
        /////////////////Get latest version /////////////////////
        for(int i = 0; i < hostnames.size() ; i++){
            final int no = i;
            if(hostnames.get(no).equals(m_selfHostname)){
                //From self
                includeSelf = true;
            }else{
                run = ()->{ getVersion(strKey, hostnames.get(no)); };
                worker = new Thread( run );
                worker.start();
                workers.add(worker);
            }
        }
        if(includeSelf){
            JSONObject verJ = new JSONObject();
            int  selfVer =  m_localInstance.getLatestVersion(strKey);
            verJ.put(RESULT, selfVer != MetaObjectInfo.NO_SUCH_KEY);
            verJ.put(VALUE, selfVer);
            mainResult.put(LocalServer.getHostName(), verJ);
        }
        try {
            for(int num=0; num < workers.size(); num++)
            {
                workers.get(num).join();
            }
        }catch(InterruptedException e){
            e.printStackTrace();
        }
        for(Map.Entry<String, JSONObject> entry: mainResult.entrySet()){
            System.out.println(entry.toString());
            if((boolean)entry.getValue().get(RESULT)){
                if(latestVersion < entry.getValue().getInt(VALUE)){
                    latestVersion = entry.getValue().getInt(VALUE);
                }
            }
        }
        if(latestVersion == MetaObjectInfo.NO_SUCH_KEY){
            // fail
            responseParams.put(RESULT, false);
            responseParams.put(REASON,"No such key");
        }else{
            // succeed
            responseParams.put(RESULT, true);
            responseParams.put(VERSION, latestVersion);
        }
        return true;
    }
    protected void getVersion(String strKey, String hostname){

        JSONObject lockReq = new JSONObject();
        lockReq.put(KEY, strKey);
        lockReq.put(IS_WRITE, true);
        Client peerClient = m_localInstance.m_peerInstanceManager.getPeerClient(hostname);
        try{
            // peerClient.getClusterLock(lockReq.toString());
            System.out.println(strKey);
            String strRS = peerClient.getLatestVersion(strKey);
            JSONObject jRS = new JSONObject(strRS);
            mainResult.put(hostname, jRS);
        }catch(TException e){
            e.printStackTrace();
        }
    }
}

