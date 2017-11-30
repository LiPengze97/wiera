package umn.dcsg.wieralocalserver.responses.consistency;

import org.apache.thrift.TException;
import org.json.JSONArray;
import org.json.JSONObject;

import org.mortbay.util.ajax.JSON;
import org.omg.PortableInterceptor.SUCCESSFUL;
import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.responses.Response;
import umn.dcsg.wieralocalserver.thriftinterfaces.LocalInstanceToPeerIface.Client;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Lock;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static umn.dcsg.wieralocalserver.Constants.*;


/**
 * Created by Kwangsung on 11/28/2016.
 * This is an example for strong consistency using Quorum.
 * Nan:
 *  initParam
 *  in-Param:Key:[Value:Version]
 *
 *  Out-param:Key:Value:To:Target_locale:Version:last_modified_time:reason:result[:tag]
 * {
 * "response_type": "Store",
 * "response_parameters": {
 *  "read_quorum": 3
 }
 }
 *
 */
public class QuorumConsistencyResponse2 extends Response {
    protected int m_nWriteQuorum = -1;
    protected int m_nReadQuorum = -1;
    protected int type = 0; // 1 read, -1 write, cann
    protected final Lock lock;
    protected final Condition enough;
    protected List<String> peersList;


    protected ConcurrentHashMap<String, JSONObject> mainResult;
    protected JSONObject returnResult;


    public QuorumConsistencyResponse2(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
        m_strRelatedEventType = strEventName;
        mainResult = new ConcurrentHashMap<>();
        returnResult = new JSONObject();


        if(m_initParams.containsKey(READ_QUORUM) == true) {
            m_nReadQuorum = (int)(double) m_initParams.get(READ_QUORUM);
            type = 1;
        }
        if(m_initParams.containsKey(WRITE_QUORUM) == true){
            m_nWriteQuorum = (int)(double) m_initParams.get(WRITE_QUORUM);
            type = -1;
        }
        if(m_nWriteQuorum < 0 && m_nReadQuorum < 0){
            System.out.println("[Error] Neither write nor read quorum is correct.");
            System.exit(1);
        }
        lock = new ReentrantLock();
        enough = lock.newCondition();

    }

    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(KEY);

        //Only for PUT operation
        if (m_initParams.containsKey(WRITE_QUORUM) == true) {
            //m_lstRequiredParams.add(TARGET_LOCALES);
            m_lstRequiredParams.add(VALUE);
            m_lstRequiredParams.add(VERSION);
            //m_lstRequiredParams.add(TIER_NAME);
            //m_lstRequiredParams.add(TAG);
            //m_lstRequiredParams.add(ONLY_META_INFO);
        }
    }

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {

    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {

        boolean bRet;
        String strReason;
        String strKey = (String) responseParams.get(KEY);


        if((boolean)responseParams.get(RESULT) == false){
            //How to handle last response's error? Here, we ignore it.
            //return false;
        }
        if(type == 1){
            // read
            JSONObject rs = getLatestData(strKey);
            if((boolean)rs.get(RESULT) == true) {
                responseParams.put(VALUE, rs.get(VALUE));
                responseParams.put(KEY, strKey);
                responseParams.put(VERSION, rs.get(VERSION));
                responseParams.put(RESULT, true);
            }else{
                responseParams.put(RESULT, false);
            }
        }else if(type == -1){
            // write
            JSONObject rs = setData((String)responseParams.get(KEY), (byte []) responseParams.get(VALUE));
        }else{
            // for future extension
            System.exit(1);
        }
        return (boolean) responseParams.get(RESULT);
    }

    private JSONObject getLatestData(String strKey){
        peersList = m_localInstance.m_peerInstanceManager.getRandomPeers(m_nReadQuorum);
        List<Thread> workers = new LinkedList<Thread>();
        Runnable workerf;
        Thread worker;
        boolean rs = false;
        for(int num=0; num < peersList.size(); num++)
        {
            final int i = num;
            workerf = ()-> { getLatestData(strKey, peersList.get(i)); };
            worker = new Thread(workerf);
            worker.start();
            workers.add(worker);
        }


        try {
            while(mainResult.size() < m_nReadQuorum) {
                enough.await();
                if(mainResult.size() < m_nReadQuorum){
                    // wake up earlier than we expect. This can happen if any worker return false
                    rs = false;
                    break;
                }else{
                    rs = true;
                }

            }

        }catch(InterruptedException e){
            e.printStackTrace();
        }

        int nVer = -10;
        byte [] databytes = null;

        if(rs == true){
            for (Map.Entry<String, JSONObject > entry : mainResult.entrySet()) {
                JSONObject value = entry.getValue();
                int tempVer = (int)value.get(VERSION);
                if(nVer  < tempVer ){
                    nVer = tempVer;
                    databytes = (byte [])value.get(VALUE);
                }
            }
            returnResult.put(KEY, strKey);
            returnResult.put(VERSION, nVer);
            returnResult.put(VALUE, databytes);
            returnResult.put(RESULT, true);
        }else {
            returnResult.put(RESULT, false);
            returnResult.put(REASON, "Worker error");
        }

        return returnResult;
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
            if( (boolean)data.get(RESULT) == true){
                int nVer = (int) data.get(VERSION);
                byte [] dat = (byte[]) data.get(VALUE);
                resultC.put(VERSION, nVer);
                resultC.put(VALUE, dat);
                mainResult.put(hostname, resultC);
                if(mainResult.size() >= m_nReadQuorum){
                    enough.signal();
                }
                rs = true;
            }else{
                enough.signal();
                rs = false;
            }
        }catch(TException e){
            rs = false;
        }

        lock.unlock();
        return rs;

    }

    /**
     * Get lock is ayschonized, but we release them together.
     *
     * */
    protected JSONObject setData(String strKey, byte [] data){

        peersList = m_localInstance.m_peerInstanceManager.getRandomPeers(m_nWriteQuorum);
        List<Thread> workers = new LinkedList<Thread>();
        int latestVersion = -10;
        Thread worker;
        Runnable run;
        boolean rs = false;
        for(int i = 0; i < m_nWriteQuorum; i++){
            final int no = i;
            run = ()->{ getVersion(strKey, peersList.get(no)); };
            worker = new Thread( run );
            worker.start();
            workers.add(worker);
        }
        try{
            while(mainResult.size() < m_nWriteQuorum){
                enough.await();
                if(mainResult.size() < m_nWriteQuorum){
                    rs = false;
                    break;
                }else{
                    rs = true;

                }
            }
            if(rs == true){
                for(Map.Entry<String, JSONObject> entry: mainResult.entrySet()){
                    JSONObject obj = entry.getValue();
                    if(latestVersion < obj.getInt(VERSION)){
                        latestVersion = obj.getInt(VERSION);
                    }
                }
            }else{
                System.out.println("failed to get version");
                returnResult.put(RESULT, false);
                returnResult.put(REASON, "Worker error");
                // release peers
                releasePeers(strKey);
                return returnResult;
            }


        }catch(InterruptedException e){
            e.printStackTrace();

        }

        workers.clear();
        final int ver = latestVersion;
        mainResult.clear();

        for(int i = 0; i < m_nWriteQuorum; i++){
            final int no = i;
            run = ()->{ setData(strKey,ver, data, peersList.get(no)); };
            worker = new Thread( run );
            worker.start();
            workers.add(worker);
        }
        try{
            while(mainResult.size() < m_nWriteQuorum){
                enough.await();
                if(mainResult.size() < m_nWriteQuorum){
                    rs = false;
                }else{
                    rs = true;
                }
            }
            if(rs  == true){
                returnResult.put(RESULT, true);
                returnResult.put(REASON, "succeed");
            }else{
                System.out.println("failed to get version");
                returnResult.put(RESULT, false);
                returnResult.put(REASON, "Worker error");
                // release peers
                releasePeers(strKey);
                return returnResult;
            }


        }catch(InterruptedException e){
            e.printStackTrace();


        }
        releasePeers(strKey);

        return returnResult;
    }
    protected boolean getVersion(String strKey, String hostname){
        int version = -1;
        boolean rsB = true;
        JSONObject lockReq = new JSONObject();
        lockReq.put(KEY, strKey);
        lockReq.put(IS_WRITE, true);
        Client peerClient = m_localInstance.m_peerInstanceManager.getPeerClient(hostname);
        try{
            peerClient.getClusterLock(lockReq.toString());
            String strRS = peerClient.getLatestVersion(strKey);
            JSONObject jRS = new JSONObject(strRS);
            if((boolean)jRS.get(RESULT) == true){
                mainResult.put(hostname, jRS);
                if(mainResult.size() >= m_nWriteQuorum){
                    enough.signal();
                }
            }else{
                enough.signal();
            }

        }catch(TException e){
            e.printStackTrace();
        }
        lock.unlock();
        return rsB;
    }

    protected boolean setData(String strKey, int nVer, byte [] value, String hostname){
        JSONObject req = new JSONObject();
        req.put(KEY, strKey);
        req.put(VALUE, value);
        req.put(VERSION, nVer);
        boolean result = false;

        Client peerClient = m_localInstance.m_peerInstanceManager.getPeerClient(hostname);
        try {
            //KEY:VALUE:[TAG:VERSION:TIER_NAME:CONFLICT_CHECK]
            String strRS = peerClient.put(req.toString());
            JSONObject jRS = new JSONObject(strRS);
            if((boolean)jRS.get(RESULT) == true){
                result = true;
                mainResult.put(HOSTNAME, jRS);
                if(mainResult.size() >= m_nWriteQuorum){
                    enough.signal();
                }
            }else{
                enough.signal();
            }

        }catch(TException e){
            e.printStackTrace();
        }

        return result;
    }

    private boolean releasePeers(String strKey) {
        JSONObject lockReq = new JSONObject();
        lockReq.put(KEY, strKey);
        lockReq.put(IS_WRITE, true);
        boolean rs = true;
        for(int i = 0; i < peersList.size(); i++){
            Client peerClient = m_localInstance.m_peerInstanceManager.getPeerClient(peersList.get(i));
            try{
                peerClient.releaseClusterLock(lockReq.toString());
            }catch (TException e){
                e.printStackTrace();
                rs = false;
            }
        }
        return rs;
    }



    /*

    protected boolean lockGlobalRead(String strKey){
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(KEY, strKey);
        Response rs = Response.createResponse(m_localInstance, LOCK_GLOBAL_READ_RESPONSE, m_strRelatedEventType ,m_initParams);
        return Response.respondAtRuntimeWithInstance(m_localInstance, rs, parameters);
    }
    protected boolean lockGlobalWrite(String strKey){
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(KEY, strKey);
        Response rs = Response.createResponse(m_localInstance, LOCK_GLOBAL_WRITE_RESPONSE, m_strRelatedEventType ,m_initParams);
        return Response.respondAtRuntimeWithInstance(m_localInstance, rs, parameters);
    }
    protected boolean unlockGlobal(String strKey){
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(KEY, strKey);
        Response rs = Response.createResponse(m_localInstance, UNLOCK_RESPONSE, m_strRelatedEventType ,m_initParams);
        return Response.respondAtRuntimeWithInstance(m_localInstance, rs, parameters);
    }*/

}