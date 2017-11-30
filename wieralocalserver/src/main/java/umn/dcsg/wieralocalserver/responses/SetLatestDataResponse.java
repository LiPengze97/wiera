package umn.dcsg.wieralocalserver.responses;

import org.apache.thrift.TException;
import org.json.JSONObject;
import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.LocalServer;
import umn.dcsg.wieralocalserver.MetaObjectInfo;
import umn.dcsg.wieralocalserver.thriftinterfaces.LocalInstanceToPeerIface;
import umn.dcsg.wieralocalserver.utils.Utils;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static umn.dcsg.wieralocalserver.Constants.*;


public class SetLatestDataResponse extends Response{
    protected String m_selfHostname;

    protected ConcurrentHashMap<String, JSONObject> mainResult;
    protected JSONObject returnResult;
    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(KEY);
        m_lstRequiredParams.add(HOSTNAME_LIST);
        m_lstRequiredParams.add(VALUE);
        m_lstRequiredParams.add(VERSION);
    }

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {

    }


    public SetLatestDataResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
        m_selfHostname = LocalServer.getHostName();
    }


    @Override
    public boolean respond(Map<String, Object> responseParams) {
        mainResult = new ConcurrentHashMap<>();
        List<String> hostnames;
        String strKey;
        int nVer = MetaObjectInfo.NO_SUCH_KEY;
        byte [] data;
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
        if(responseParams.containsKey(VALUE)){
            data = (byte[]) responseParams.get(VALUE);
        }else {
            data = (byte []) m_initParams.get(VALUE);
        }
        if(responseParams.containsKey(VERSION)){
            nVer = (int) responseParams.get(VERSION);
        }else {
            nVer = (int) m_initParams.get(VERSION);
        }
        List<Thread> workers = new LinkedList<>();

        Thread worker;
        Runnable run;
        final int latestVersion = nVer;
        /////////////////Get latest version /////////////////////
        for(int i = 0; i < hostnames.size() ; i++){
            final int no = i;
            if(hostnames.get(no).equals(m_selfHostname)){
                //From self
                includeSelf = true;
            }else{
                run = ()->{ setLatestData(strKey, latestVersion, data, hostnames.get(no) ); };
                worker = new Thread( run );
                worker.start();
                workers.add(worker);
            }
        }
        if(includeSelf){
            //
            m_localInstance.put(strKey, latestVersion, data);
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


        for(Map.Entry<String, JSONObject> entry: mainResult.entrySet()){
            if((boolean)entry.getValue().get(RESULT)){
                    //successful
            }else{
                result = false;
                break;
            }
        }



        if(nVer == MetaObjectInfo.NO_SUCH_KEY || !result){
            // fail
            responseParams.put(RESULT, false);
            responseParams.put(REASON,"No such key");
            responseParams.put(KEY, strKey);
        }else{
            // succeed
            responseParams.put(RESULT, true);
            responseParams.put(VERSION, nVer);
            responseParams.put(KEY, strKey);
            responseParams.put(VALUE, data);
        }
        return true;
    }
    protected void setLatestData(String strKey, int nVer, byte [] value, String hostname){
        JSONObject req = new JSONObject();
        req.put(KEY, strKey);
        req.put(VALUE, Utils.encodeBytes(value));
        req.put(VERSION, nVer);


        LocalInstanceToPeerIface.Client peerClient = m_localInstance.m_peerInstanceManager.getPeerClient(hostname);

        try {

            System.out.println("qw");
            String strRS = peerClient.put(req.toString());
            System.out.println("qw1");
            JSONObject jRS = new JSONObject(strRS);

            mainResult.put(HOSTNAME, jRS);

        }catch(TException e){
            e.printStackTrace();
        }


    }
}