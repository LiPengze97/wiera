package umn.dcsg.wieralocalserver.datadistribution;

import org.apache.commons.codec.binary.Base64;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.json.JSONObject;
import umn.dcsg.wieralocalserver.Constants;
import umn.dcsg.wieralocalserver.LocalServer;
import umn.dcsg.wieralocalserver.PeerInstancesManager;
import umn.dcsg.wieralocalserver.thriftinterfaces.LocalInstanceToPeerIface.Client;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by Kwangsung on 7/23/2017.
 */
public class ParallelPeerRequest implements Runnable {
    String m_strRequestType;
    String m_strTargetHostName;
    String m_strKey;
    int m_nVer;
    byte[] m_value;
    long m_lSize;
    String m_strTierName;
    String m_strTag;
    long m_lLastModifiedTime;
    JSONObject m_result = new JSONObject();
    boolean m_bOnlyMetaInfo = false;
    PeerInstancesManager m_peersManager;    // for releasing client after done

    public ParallelPeerRequest(PeerInstancesManager peersManager,
                               String strRequestType,
                               String strTargetHostName,
                               String strKey, int nVer,
                               long lSize, byte[] value, String strTierName, String strTag,
                               long lLastModifiedTime, boolean bOnlyMetaInfo) {
        m_strRequestType = strRequestType;
        m_strTargetHostName = strTargetHostName;
        m_strKey = strKey;
        m_nVer = nVer;
        m_lSize = lSize;
        m_value = value;
        m_strTierName = strTierName;
        m_strTag = strTag;
        m_lLastModifiedTime = lLastModifiedTime;
        m_bOnlyMetaInfo = bOnlyMetaInfo;
        m_peersManager = peersManager;
    }

    @Override
    public void run() {
        //System.out.println("[debug] Sender send update to peer in thread");
        _sendRequest();
    }

    public void _sendRequest() {
        //This is thrift client to send update
        Client peerClient = m_peersManager.getPeerClient(m_strTargetHostName);

        try {
            if (peerClient != null) {
                switch (m_strRequestType) {
                    case PUT_PEER:
                        sendPutRequest(peerClient);
                        break;
                    case GET_LASTEST_VERSION_PEER:
                        sendGetLatestVersionRequest(peerClient);
                        break;
                    default:
                        m_result.put(RESULT, false);
                        m_result.put(REASON, m_strRequestType + " is not supported yet.");
                        return;
                }
            }
        } finally{
            m_peersManager.releasePeerClient(m_strTargetHostName, peerClient);
        }
    }

    public void sendPutRequest(Client peerClient) {
        //Create JSONObject which will be sent to all peers.
        JSONObject req = new JSONObject();
        req.put(Constants.VERSION, m_nVer);
        req.put(Constants.KEY, m_strKey);
        req.put(Constants.SIZE, m_lSize);
        req.put(Constants.VALUE, Base64.encodeBase64String(m_value)); //Todo need to make value be shared.
        req.put(Constants.TIER_NAME, m_strTierName);
        req.put(Constants.TAG, m_strTag);
        req.put(Constants.LAST_MODIFIED_TIME, m_lLastModifiedTime);
        req.put(Constants.HOSTNAME, LocalServer.getHostName());
        req.put(Constants.ONLY_META_INFO, m_bOnlyMetaInfo);
        String strReq = req.toString();

        try {
            //Send a put request
            m_result = new JSONObject(peerClient.put(strReq));
        } catch (TTransportException e) {
            m_result.put(RESULT, false);
            m_result.put(REASON, e.getMessage());
        } catch (TException e) {
            m_result.put(RESULT, false);
            m_result.put(REASON, e.getMessage());
        }
    }

    public void sendGetLatestVersionRequest(Client peerClient) {
        //Create JSONObject which will be sent to all peers.
        JSONObject req = new JSONObject();
        req.put(Constants.KEY, m_strKey);
        String strReq = req.toString();

        try {
            //Send a request to target
            m_result = new JSONObject(peerClient.getLatestVersion(strReq));
        } catch (TTransportException e) {
            m_result.put(RESULT, false);
            m_result.put(REASON, e.getMessage());
        } catch (TException e) {
            m_result.put(RESULT, false);
            m_result.put(REASON, e.getMessage());
        }
    }

    public boolean getResult() { return m_result.getBoolean(RESULT); }

    public String getReason() {
        return m_result.getString(REASON);
    }

    public String getValue() {
        return m_result.getString(VALUE);
    }

    public int getVersion() { return m_result.getInt(VERSION); }

    public String getTargetHostname() {
        return m_strTargetHostName;
    }
}