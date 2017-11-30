

package umn.dcsg.wieralocalserver.responses.consistency;

import org.apache.thrift.TException;
import org.json.JSONArray;
import org.json.JSONObject;

import org.mortbay.util.ajax.JSON;
import org.omg.PortableInterceptor.SUCCESSFUL;
import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.LocalServer;
import umn.dcsg.wieralocalserver.MetaObjectInfo;
import umn.dcsg.wieralocalserver.responses.Response;
import umn.dcsg.wieralocalserver.thriftinterfaces.LocalInstanceToPeerIface.Client;
import umn.dcsg.wieralocalserver.utils.Utils;

import javax.rmi.CORBA.Util;
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
public class QuorumConsistencyResponse extends Response {
    protected int m_nWriteQuorum = -1;
    protected int m_nReadQuorum = -1;
    protected int type = 0; // 1 read, -1 write, cann

    protected List<String> peersList;


    protected ConcurrentHashMap<String, JSONObject> mainResult;
    protected JSONObject returnResult;


    public QuorumConsistencyResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
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
        mainResult.clear();

        boolean bRet;
        String strReason;
        String strKey = (String) responseParams.get(KEY);

/*
        if((boolean)responseParams.get(RESULT) == false){
            //How to handle last response's error? Here, we ignore it.
            //return false;
        }*/
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
            int nVer = -10;
            System.out.println("Checkp1");
            if(responseParams.containsKey(VERSION)){
                nVer = (int)responseParams.get(VERSION);
            }
            JSONObject rs = setData((String)responseParams.get(KEY), (byte []) responseParams.get(VALUE), nVer);
        }else{
            // for future extension
            System.exit(1);
        }
        responseParams.put(RESULT, true);
        return (boolean) responseParams.get(RESULT);
    }

    private JSONObject getLatestData(String strKey){
        peersList = m_localInstance.m_peerInstanceManager.getRandomPeers(m_nReadQuorum - 1);
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
        JSONObject data = new JSONObject();
        data.put(VALUE, Utils.encodeBytes(m_localInstance.get(strKey)));

        mainResult.put(LocalServer.getHostName(),data);
        try {
            for(int num=0; num < workers.size(); num++)
            {
                workers.get(num).join();
            }
        }catch(InterruptedException e){
            e.printStackTrace();
        }
        if(mainResult.size() < m_nReadQuorum){
            // wake up earlier than we expect. This can happen if any worker return false
            rs = false;
        }else{
            rs = true;
        }


        int nVer = -10;
        byte [] databytes = null;

        if(rs == true){
            for (Map.Entry<String, JSONObject > entry : mainResult.entrySet()) {
                JSONObject value = entry.getValue();
                if((boolean)value.get(RESULT)){
                    int tempVer = (int)value.get(VERSION);

                    if(nVer  < tempVer ){
                        nVer = tempVer;

                        databytes = Utils.decodeBytes((String)value.get(VALUE));

                    }
                }else{
                    rs = false;
                    break;
                }
            }
        }
        if(rs == true){
            returnResult.put(KEY, strKey);
            returnResult.put(VERSION, nVer);
            returnResult.put(VALUE, databytes);
            returnResult.put(RESULT, true);
        }else{
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
            mainResult.put(hostname, data);
        }catch(TException e){
            rs = false;
        }


        return rs;

    }

    /**
     * Get lock is ayschonized, but we release them together.
     *
     * */
    protected JSONObject setData(String strKey, byte [] data, int nVer){
        mainResult.clear();
        peersList = m_localInstance.m_peerInstanceManager.getRandomPeers(m_nWriteQuorum -1);
        List<Thread> workers = new LinkedList<>();
        int latestVersion = nVer;
        Thread worker;
        Runnable run;
        boolean rs = false;
        /////////////////Get latest version /////////////////////
        for(int i = 0; i < peersList.size() ; i++){
            final int no = i;
            run = ()->{ getVersion(strKey, peersList.get(no)); };
            worker = new Thread( run );
            worker.start();
            workers.add(worker);
        }
        JSONObject verJ = new JSONObject();
        int  selfVer =  m_localInstance.getLatestVersion(strKey);
        verJ.put(RESULT, selfVer != MetaObjectInfo.NO_SUCH_KEY);
        verJ.put(VALUE, selfVer);
        mainResult.put(LocalServer.getHostName(), verJ);
        try {
            for(int num=0; num < workers.size(); num++)
            {
                workers.get(num).join();
            }
        }catch(InterruptedException e){
            e.printStackTrace();
        }
        if(mainResult.size() < m_nReadQuorum){
            // wake up earlier than we expect. This can happen if any worker return false

            rs = false;
        }else{
            rs = true;
        }

        if(rs == true){
            for(Map.Entry<String, JSONObject> entry: mainResult.entrySet()){
                System.out.println(entry.toString());
                if((boolean)entry.getValue().get(RESULT)){
                    if(latestVersion < entry.getValue().getInt(VALUE)){
                        latestVersion = entry.getValue().getInt(VALUE);
                    }
                }else{
                    // can be false. (false is allowed)
                    // because the peer may not have this key-value.
                }
            }
        }
        if(rs == false){
            System.out.println("failed to get version");
            returnResult.put(RESULT, false);
            returnResult.put(REASON, "Worker error");
            // release peers
            //releasePeersLock(strKey);
            return returnResult;
        }

        System.out.println("Checkp3");
        workers = new LinkedList<>();
        final int ver = latestVersion;
        mainResult.clear();

        for(int i = 0; i < peersList.size(); i++){
            final int no = i;
            run = ()->{ setData(strKey,ver, data, peersList.get(no)); };
            worker = new Thread( run );
            worker.start();
            workers.add(worker);
        }
        m_localInstance.put(strKey, latestVersion, data);
        JSONObject valueJ = new JSONObject();
        valueJ.put(RESULT, true);
        mainResult.put(LocalServer.getHostName(), valueJ);
        /*try {
            for(int num=0; num < workers.size(); num++)
            {
                //workers.get(num).join();
            }
        }catch(InterruptedException e){
            e.printStackTrace();
        }*/
       // releasePeersLock(strKey);
        System.out.println("Checkp");
        if(mainResult.size() < m_nWriteQuorum){
            rs = false;
        }else{
            rs = true;
        }

        if(rs == true){
            for(Map.Entry<String, JSONObject> entry: mainResult.entrySet()){
                if((boolean)entry.getValue().get(RESULT)){
                    //successful
                }else{
                    rs = false;
                    break;
                }
            }
        }

        if(rs  == true){
            returnResult.put(RESULT, true);
            returnResult.put(REASON, "succeed");
        }
        else{
            System.out.println("failed to get version");
            returnResult.put(RESULT, false);
            returnResult.put(REASON, "Worker error");
            // release peers

            return returnResult;
        }



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
           // peerClient.getClusterLock(lockReq.toString());
            System.out.println(strKey);
            String strRS = peerClient.getLatestVersion(strKey);
            JSONObject jRS = new JSONObject(strRS);
            mainResult.put(hostname, jRS);
        }catch(TException e){
            e.printStackTrace();
        }

        return true;
    }

    protected void setData(String strKey, int nVer, byte [] value, String hostname){
        JSONObject req = new JSONObject();
        req.put(KEY, strKey);
        req.put(VALUE, Utils.encodeBytes(value));
        req.put(VERSION, nVer);


        Client peerClient = m_localInstance.m_peerInstanceManager.getPeerClient(hostname);

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
    /*
    private boolean releasePeersLock(String strKey) {
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
*/



}

