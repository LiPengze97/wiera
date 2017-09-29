package umn.dcsg.wieralocalserver.datadistribution;

import org.apache.commons.codec.binary.Base64;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.json.JSONObject;
import umn.dcsg.wieralocalserver.Constants;
import umn.dcsg.wieralocalserver.LocalServer;
import umn.dcsg.wieralocalserver.PeerInstancesManager;
import umn.dcsg.wieralocalserver.thriftinterfaces.LocalInstanceToPeerIface.Client;

/**
 * Created by Kwangsung on 7/23/2017.
 */
public class Updater implements Runnable {
    String m_strTargetHostName;
    String m_strKey;
    long m_version;
    byte[] m_value;
    long m_lSize;
    String m_strTierName;
    String m_strTag;
    long m_lLastModifiedTime;
    String m_strReason = null;
    boolean m_bOnlyMetaInfo = false;
    PeerInstancesManager m_peersManager;

    public Updater(PeerInstancesManager peersManager, String strTargetHostName, String strKey, long lVersion, long lSize, byte[] value, String strTierName, String strTag, long lLastModifiedTime, boolean bOnlyMetaInfo) {
        m_strTargetHostName = strTargetHostName;
        m_strKey = strKey;
        m_version = lVersion;
        m_lSize = lSize;
        m_value = value;
        m_strTierName = strTierName;
        m_strTag = strTag;
        m_lLastModifiedTime = lLastModifiedTime;
        m_strReason = null;
        m_bOnlyMetaInfo = bOnlyMetaInfo;
        m_peersManager = peersManager;
    }

    @Override
    public void run() {
        //System.out.println("[debug] Sender send update to peer in thread");
        m_strReason =  _sendUpdate();
    }

    public String _sendUpdate() {
        //Create JSONObject which will be sent to all peers.
        JSONObject req = new JSONObject();
        req.put(Constants.VERSION, m_version);
        req.put(Constants.KEY, m_strKey);
        req.put(Constants.SIZE, m_lSize);
        req.put(Constants.VALUE, Base64.encodeBase64String(m_value)); //Todo need to make value be shared.
        req.put(Constants.TIER_NAME, m_strTierName);
        req.put(Constants.TAG, m_strTag);
        req.put(Constants.LAST_MODIFIED_TIME, m_lLastModifiedTime);
        req.put(Constants.HOSTNAME, LocalServer.getHostName());
        req.put(Constants.ONLY_META_INFO, m_bOnlyMetaInfo);
        //req.putObject("fqdn", SocketComm.getExternalDNS());

        String strReason = null;
        String strReq = req.toString();

        //This is thrift client to send update
        Client peerClient = m_peersManager.getPeerClient(m_strTargetHostName);

        if (peerClient != null) {
            try {
/*
                Latency remoteWriteLatency = new Latency();
                remoteWriteLatency.start();
                remoteWriteLatency.stop();
                m_localInstance.m_localInfo.addRemoteLatency(m_strTargetHostName, remoteWriteLatency);
*/
                //Send a update
                String strResponse = peerClient.put(strReq);

                if (strResponse.length() > 0) {
                    JSONObject response = new JSONObject(strResponse);
                    boolean bResult = (boolean) response.get("result");

                    if (bResult == false) {
                        strReason = (String) response.get(Constants.VALUE);
                        System.out.println("Send update failed. Reason: " + strReason);
                    }
                } else {
                    strReason = "This should not happen as this line is not reachable. Target Hostname: " + m_strTargetHostName;
                }
            } catch (TTransportException e) {
                strReason = e.getMessage();
            } catch (TException e) {
                strReason = e.getMessage();
            } finally {
                m_peersManager.releasePeerClient(m_strTargetHostName, peerClient);
            }
        } else {
            System.out.println("Should not happen. Fail to get thrift client for peer");
        }

        return strReason;
    }

    public String getReason() {return m_strReason;}
    public String getTargetHostName() {return m_strTargetHostName;}

}