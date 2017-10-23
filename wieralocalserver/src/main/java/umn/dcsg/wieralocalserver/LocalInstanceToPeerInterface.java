package umn.dcsg.wieralocalserver;

import com.google.gson.Gson;
import umn.dcsg.wieralocalserver.info.Latency;
import org.apache.commons.codec.binary.Base64;
import org.apache.thrift.TException;
import org.json.JSONObject;
import umn.dcsg.wieralocalserver.thriftinterfaces.LocalInstanceToPeerIface;
import umn.dcsg.wieralocalserver.utils.Utils;

import java.nio.ByteBuffer;
import java.util.HashMap;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by Kwangsung on 12/28/2015.
 */
public class LocalInstanceToPeerInterface implements LocalInstanceToPeerIface.Iface {
    LocalInstance m_localInstance = null;
    //protected ReentrantReadWriteLock  = null;

    public LocalInstanceToPeerInterface(LocalInstance instance) {
        this.m_localInstance = instance;
        //m_dataDistributionLock = new ReentrantReadWriteLock();
    }

    @Override
    public String ping() throws TException {
        String strReason = NOT_HANDLED;
        Latency latencyForRetrieving = new Latency();
        latencyForRetrieving.start();

        JSONObject response = new JSONObject();
        response.put(TYPE, PING);

        try {
            //Need to configure storage information to sent with return
            //Now return storage info and average_dcs_latency from each instance for leader election
            HashMap<String, HashMap<String, Double>> storageInfo = m_localInstance.m_localInfo.getStorageInfo(true);
            HashMap<String, Double> networkInfo = m_localInstance.m_localInfo.getDCsLatencyInfo();
            HashMap<String, Long> accessInfo = m_localInstance.m_localInfo.getLatestLocalAccessInfo();
            latencyForRetrieving.stop();

            Gson gson = new Gson();

            response.put(RESULT, true);
            response.put(OP_TIME, latencyForRetrieving.getLatencyInMills());    //This is special case as this call will be used for checking latency between DCs
            response.put(VALUE, gson.toJson(storageInfo));
            response.put(VALUE2, gson.toJson(networkInfo));
            response.put(VALUE3, gson.toJson(accessInfo));
        } catch (Exception e) {
            e.printStackTrace();
            response.put(RESULT, false);
            response.put(OP_TIME, latencyForRetrieving.getLatencyInMills());    //This is special case as this call will be used for checking latency between DCs
            response.put(REASON, e.getMessage());
        }

        return response.toString();
    }

    @Override
    public String forwardPutRequest(String strPutReq){
        String strReason = NOT_HANDLED;
        boolean bRet;
        int nVer;
        long lLastModifiedTime;

        JSONObject response = new JSONObject();

        JSONObject obj = new JSONObject(strPutReq);
        String strKey = (String) obj.get(KEY);
        byte[] value = Base64.decodeBase64((String) obj.get(VALUE));
        String strFrom = (String) obj.get(FROM);
        String strTag;

        if(obj.has(TAG) == true) {
            strTag = (String) obj.get(TAG);
        }

        //Handling when peer instance does not know about the tiername in this instance.
        //Find and use the fastest storage tier in this instance
        if (m_localInstance.m_applicationToLocalInstanceInterface.put(strKey, ByteBuffer.wrap(value)) == true) {
            m_localInstance.m_localInfo.incrementalForwardedRequestCnt(strFrom, PUT);

            MetaObjectInfo meta = m_localInstance.getMetadata(strKey);
            nVer = meta.getLatestVersion();
            lLastModifiedTime = meta.getLastModifiedTime();

            strReason = OK;
            bRet = true;
        } else {
            strReason = "Failed to putObject the key-value forwarded into Primary Instance";
            bRet = false;

            nVer = MetaObjectInfo.NO_SUCH_VERSION;
            lLastModifiedTime = 0;
        }

        response.put(TYPE, "forwardPutRequest");
        response.put(RESULT, bRet);
        response.put(VALUE, strReason);
        response.put(REASON, strReason);
        response.put(VERSION, nVer);
        response.put(LAST_MODIFIED_TIME, lLastModifiedTime);

        //System.out.println("Forwarding: " + strReason);
        return response.toString();
    }

    //Nan: call by peer, forwardGET
    //input KEY:FROM:[VERSION:TIER_NAME]
    //output RESULT:REASON:TYPE:VALUE
    @Override
    public String get(String strReq) throws TException {
        //This is forwarded get operation from other peers
        JSONObject response = new JSONObject();

        try {
            JSONObject obj = new JSONObject(strReq);

            String strKey = (String) obj.get(KEY);
            String strFrom = (String) obj.get(FROM);
            String strTierName = m_localInstance.m_strDefaultTierName;
            int nVer = m_localInstance.getLatestVersion(strKey);
            if (obj.has(VERSION) == true) {
                nVer = (int) obj.get(VERSION);
            }

            if (obj.has(TIER_NAME) == true) {
                strTierName = (String) obj.get(TIER_NAME);
            }

            MetaObjectInfo meta = m_localInstance.getMetadata(strKey);
            if(meta != null){
                if(meta.hasLocale(strTierName)){

                }else if(meta.hasLocale( m_localInstance.m_strDefaultTierName)){
                    strTierName = m_localInstance.m_strDefaultTierName;
                }else{
                    strTierName = meta.getFastTier(nVer);
                }
                if(strTierName != null){
                    byte[] bytes = m_localInstance.get(strKey, nVer, strTierName);
                    //Increase forwarded get operation
                    m_localInstance.m_localInfo.incrementalForwardedRequestCnt(strFrom, GET);
                    if (bytes != null) {
                        response.put(RESULT, true);
                        response.put(VALUE, Base64.encodeBase64String(bytes));

                    }
                }
            }else{
                response.put(RESULT, false);
                response.put(REASON, "Failed to find a value associated with the key");
            }
        } catch (Exception e) {
            e.printStackTrace();
            response.put(RESULT, false);
            response.put(REASON, e.getMessage());
        }
        response.put(TYPE, GET);
        return response.toString();
    }
    // Nan
    // output RESULT:REASON:TYPE:VALUE
    @Override
    public String getLatestVersion(String strKey) throws TException {
        String strReason = NOT_HANDLED;
        boolean bRet = false;
        JSONObject response = new JSONObject();

        int nVer = m_localInstance.getLatestVersion(strKey);

        if (nVer >= 0) {
            strReason = OK;
            bRet = true;
        } else {
            strReason = "Failed to find the latest version";
        }

        response.put(RESULT, bRet);
        response.put(VALUE, nVer);
        response.put(VERSION, nVer);
        response.put(REASON, strReason);
        response.put(TYPE, "getLatestVersion");

        return response.toString();
    }
    //Nan:
    //input KEY:VALUE:[TAG:VERSION:TIER_NAME:CONFLICT_CHECK]
    //output TYPE:RESULT:VALUE:REASON
    @Override //This function is only used for update broadcasting
    public String put(String strReq) {
        JSONObject req = new JSONObject(strReq);
        String strReason = NOT_HANDLED;
        boolean bRet;
        String strTag = "";
        int nVer = 0;
        String strTiername = "";
        boolean bConflictCheck = false;
        String strKey = (String) req.get(KEY);
        byte[] value = Base64.decodeBase64((String) req.get(VALUE));
        MetaObjectInfo obj = m_localInstance.getMetadata(strKey);
        if(req.has(TAG) == true) {
            strTag = (String) req.get(TAG);
        }
        if(req.has(VERSION) == true) {
            nVer = (int) req.get(VERSION);
        }else if(obj != null){
            nVer = obj.getLatestVersion() + 1;
        }
        if(req.has(TIER_NAME) == true) {
            strTiername = (String) req.get(TIER_NAME);
        }else{
            strTiername = m_localInstance.m_strDefaultTierName;
        }
        if(req.has(CONFLICT_CHECK) == true) {
            bConflictCheck = (boolean) req.get(CONFLICT_CHECK);
        }
        //Just Write to local without checking version conflict
        if (bConflictCheck == false ||obj == null || obj.getLatestVersion() != nVer) {
            if (m_localInstance.put(strKey, nVer, value, strTiername) != null) {
                strReason = OK;
                bRet = true;
            } else {
                strReason = "Failed to create a new key: " + strKey;
                bRet = false;
            }
        }else {

            bRet = false;
            strReason = "There was a conflicts (same version is available). Update was not done";
        }
        //Result
        JSONObject response = new JSONObject();
        response.put(TYPE, PUT);
        response.put(RESULT, bRet);
        response.put(VALUE, strReason);
        response.put(REASON, strReason);
        return response.toString();
    }

    @Override
    public String getClusterLock(String strLockReq) throws TException {

        JSONObject lockReq = new JSONObject(strLockReq);
        String strKey = (String) lockReq.get(KEY);
        boolean bWrite = (boolean) lockReq.get(IS_WRITE);

        //For ret
        JSONObject response = new JSONObject();
        //DataDistributionUtil dataDistribution = m_localInstance.m_peerInstanceManager.getDataDistribution();
        String strResponse = "";



        response.put(VALUE, strResponse);
        System.out.println(strResponse);

        return response.toString();
    }

    @Override
    public String releaseClusterLock(String strLockReq) throws org.apache.thrift.TException {
        //Extract req
        JSONObject lockReq = new JSONObject(strLockReq);
        String strKey = (String) lockReq.get(KEY);
        boolean bWrite = (boolean) lockReq.get(IS_WRITE);

        JSONObject response = new JSONObject();
        String strResponse = NOT_HANDLED;


        response.put(VALUE, strResponse);
        System.out.println(strResponse);

        return response.toString();
    }

    @Override
    public String setLeader(String strLeaderHostNameReq) throws org.apache.thrift.TException {
        JSONObject response = new JSONObject();
        String strResponse = NOT_HANDLED;
        boolean bRet = false;

        try {

            JSONObject obj = new JSONObject(strLeaderHostNameReq);
            String strLeaderHostName = (String) obj.get(LEADER_HOSTNAME);


        } catch (Exception e) {
            e.printStackTrace();
        }

        response.put(RESULT, bRet);
        response.put(VALUE, strResponse);
        return response.toString();
    }
}