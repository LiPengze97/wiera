package umn.dcsg.wieralocalserver.responses;

import org.apache.thrift.TException;
import org.json.JSONObject;
import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.LocalServer;
import umn.dcsg.wieralocalserver.MetaObjectInfo;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static umn.dcsg.wieralocalserver.Constants.*;

import umn.dcsg.wieralocalserver.thriftinterfaces.LocalInstanceToPeerIface.Client;
import umn.dcsg.wieralocalserver.utils.Utils;

public class GetLatestDataResponse extends Response{
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


    public GetLatestDataResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
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

        Thread worker;
        Runnable run;
        /////////////////Get latest version /////////////////////
        for(int i = 0; i < hostnames.size() ; i++){
            final int no = i;
            if(hostnames.get(no).equals(m_selfHostname)){
                //From self
                includeSelf = true;
            }else{
                run = ()->{ getLatestData(strKey, hostnames.get(no)); };
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
            mainResult.put(m_selfHostname, verJ);
        }
        try {
            for(int num=0; num < workers.size(); num++)
            {
                workers.get(num).join();
            }
        }catch(InterruptedException e){
            e.printStackTrace();
        }
        boolean result = true;
        int nVer = MetaObjectInfo.NO_SUCH_KEY;
        byte [] databytes = null;
        for (Map.Entry<String, JSONObject > entry : mainResult.entrySet()) {
            JSONObject value = entry.getValue();
            if((boolean)value.get(RESULT)){
                int tempVer = (int)value.get(VERSION);

                if(nVer  < tempVer ){
                    nVer = tempVer;

                    databytes = Utils.decodeBytes((String)value.get(VALUE));

                }
            }else{
                result = false;
                break;
            }
        }

        if(nVer == MetaObjectInfo.NO_SUCH_KEY || !result){
            // fail
            responseParams.put(RESULT, false);
            responseParams.put(REASON,"No such key");
        }else{
            // succeed
            responseParams.put(RESULT, true);
            responseParams.put(VERSION, nVer);
            responseParams.put(VALUE, databytes);
        }
        return true;
    }
    protected boolean getLatestData(String strKey, String hostname){

        JSONObject req = new JSONObject();
        JSONObject resultC = new JSONObject();
        boolean rs = false;
        req.put(KEY, strKey);
        req.put(FROM, hostname);

        Client peerClient = m_localInstance.m_peerInstanceManager.getPeerClient(hostname);
        try {
            String result = peerClient.get(req.toString());
            JSONObject data = new JSONObject(result);
            mainResult.put(hostname, data);
        }catch(TException e){
            rs = false;
        }


        return rs;

    }
}