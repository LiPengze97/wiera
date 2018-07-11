package umn.dcsg.wieralocalserver.clients;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.json.JSONArray;
import org.json.JSONObject;
import umn.dcsg.wieralocalserver.Constants;
import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.thriftinterfaces.ApplicationToLocalInstanceIface;
import umn.dcsg.wieralocalserver.thriftinterfaces.ApplicationToWieraIface;
import umn.dcsg.wieralocalserver.utils.Utils;

import java.nio.file.Path;

import static umn.dcsg.wieralocalserver.Constants.ID;
import static umn.dcsg.wieralocalserver.Constants.RESULT;
import static umn.dcsg.wieralocalserver.Constants.VALUE;

public class WieraInstanceClient {
    String m_strIPAddress;
    int m_nPort;
    String m_strWieraID;
    boolean m_bConnected = false;
    JSONArray m_localInstanceList = null;
    ApplicationToWieraIface.Client m_wieraClient = null;

    public WieraInstanceClient(String strIPAddress, int nPort){
        m_strIPAddress = strIPAddress;
        m_nPort = nPort;
        connect(m_strIPAddress, m_nPort);
    }

    public boolean connect() {
        return connect(m_strIPAddress, m_nPort);
    }

    public boolean connect(String strIPAddress, int nPort) {
        if(isConnected() == false) {
            TTransport transport;

            transport = new TSocket(strIPAddress, nPort);
            TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
            ApplicationToWieraIface.Client client = new ApplicationToWieraIface.Client(protocol);

            try {
                transport.open();
                m_bConnected = true;
                m_wieraClient = client;
            } catch (TTransportException e) {
                e.printStackTrace();
            }
        }

        return m_bConnected;
    }

    public boolean startInstance(String strPolicyPath) {
        return startInstance(Utils.loadJSONObject(strPolicyPath.toString()));
    }

    public boolean startInstance(JSONObject strPolicy) {
        if(isConnected() == true) {
            try {
                String strResult = m_wieraClient.startWieraInstance(strPolicy.toString());
                JSONObject res = new JSONObject(strResult);
                boolean bResult = (boolean) res.get(RESULT);

                //this will include WieraID or rls -aeason if failed
                String strValue = (String) res.get(VALUE);

                if (bResult == true) {
                    m_strWieraID = strValue;
                    Thread.sleep(3000);
                } else {
                    System.out.println("Failed to start instance reason: " + strValue);
                }

                //Update instance List
                updateInstanceClient(m_strWieraID);
                return m_localInstanceList != null;
            } catch (TException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            System.out.printf("Not connected to Wiera yet");
        }

        return false;
    }

    public boolean stopInstance(String strWieraID) {
        boolean bResult = false;
        //Create instance here with global Policy.
        if (isConnected() == true) {
            JSONObject jsonReq = new JSONObject();
            jsonReq.put(ID, strWieraID);
            String strReq = jsonReq.toString();

            String strResult = null;
            try {
                strResult = m_wieraClient.stopWieraInstance(strReq);
            } catch (TException e) {
                e.printStackTrace();
            }
            JSONObject res = new JSONObject(strResult);
            bResult = (boolean) res.get(RESULT);

            //this will include WieraID or reason if failed
            String strValue = (String) res.get(VALUE);

            if (bResult == true) {
                System.out.println("Stop instances successfully.");
            } else {
                System.out.println("Failed to stop instance reason: " + strValue);
            }
        } else {
            System.out.printf("Wiera Thrift is not running");
        }

        return bResult;
    }

    public boolean isConnected() {
        return m_bConnected;
    }

    public ApplicationToWieraIface.Client getWieraClient(String strIPAddress, int nPort) {
        return m_wieraClient;
    }

    private JSONArray getLocalInstanceList(String strWieraID) {
        String strResult;
        String strValue;
        JSONArray instanceList;

        try {
            strResult = m_wieraClient.getWieraInstance(strWieraID);

            //This will contain list of LocalInstance instance as a JsonObject
            JSONObject res = new JSONObject(strResult);

            //getObject list of local instance.
            boolean bResult = (boolean) res.get(Constants.RESULT);

            if (bResult == true) {
                //Get instance List
                instanceList = (JSONArray) res.get(Constants.VALUE);
                return instanceList;
            } else {
                strValue = (String) res.get(Constants.VALUE);
                System.out.println("Failed to getObject instance list reason: " + strValue);
            }
        } catch (TException e) {
            e.printStackTrace();
        }

        return null;
    }

    public LocalInstanceClient getLocalInstanceClient() {
        return getLocalInstanceClient(0);
    }

    public void updateInstanceClient(String strWieraID) {
        m_localInstanceList = getLocalInstanceList(strWieraID);
    }

    public LocalInstanceClient getLocalInstanceClient(int nOrder) {
        if(isConnected() == true && m_localInstanceList != null && m_localInstanceList.length() > 0) {
            int nSelected = Math.min(nOrder, m_localInstanceList.length()-1);
            JSONArray selectedInstance = (JSONArray) m_localInstanceList.get(nSelected);
            return new LocalInstanceClient((String) selectedInstance.get(0), (String) selectedInstance.get(1),(int) selectedInstance.get(2));
        }

        return null;
    }

    public JSONArray getLocalInstanceList() {
        return m_localInstanceList;
    }
}