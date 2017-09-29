package umn.dcsg.wieralocalserver;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.json.JSONArray;
import org.json.JSONObject;
import umn.dcsg.wieralocalserver.thriftinterfaces.ApplicationToLocalInstanceIface;
import umn.dcsg.wieralocalserver.thriftinterfaces.ApplicationToWieraIface;

/**
 * Created by Kwangsung on 1/30/2017.
 */
public class LocalInstanceClient {
	String m_strWieraIP;
	int m_nWieraPort;
	ApplicationToWieraIface.Client m_wieraClient;
	ApplicationToLocalInstanceIface.Client m_localInstanceClient;

	LocalInstanceClient(String strIPAddress, int nPort, String strWieraID) {
		m_strWieraIP = strIPAddress;
		m_nWieraPort = nPort;
		m_wieraClient = getWieraClient();

		if (m_wieraClient != null) {
			JSONArray instanceList = getInstanceList(m_wieraClient, "test");

			if (instanceList != null) {
				int nIndex = 0;

				for (int i = 0; i < instanceList.length(); i++) {
					JSONArray instanceInfo = (JSONArray) instanceList.get(i);

					if (LocalServer.getHostName().compareTo((String) instanceInfo.get(0)) == 0) ;
					{
						//This is it!
						m_localInstanceClient = getNewConnection((String) instanceInfo.get(1), (int) instanceInfo.get(2));
					}
				}
			} else {
				System.out.println("Failed to getObject LocalInstance Server List.");
			}
		}
	}

	public ApplicationToWieraIface.Client getWieraClient() {
		TTransport transport;

		transport = new TSocket(m_strWieraIP, m_nWieraPort);
		TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
		ApplicationToWieraIface.Client client = new ApplicationToWieraIface.Client(protocol);

		try {
			transport.open();
		} catch (TException x) {
			System.out.println(x.getMessage());
			return null;
		}

		return client;
	}

	private static JSONArray getInstanceList(ApplicationToWieraIface.Client wieraClient, String strWieraID) {
		String strResult;
		String strValue;
		JSONArray instanceList;

		try {
			strResult = wieraClient.getInstances(strWieraID);

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

	public ApplicationToLocalInstanceIface.Client getNewConnection(String strIPAddress, int nPort) {
		TTransport transport;

		transport = new TSocket(strIPAddress, nPort);
		TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport, 1048576 * 200));
		ApplicationToLocalInstanceIface.Client client;

		try {
			transport.open();
			client = new ApplicationToLocalInstanceIface.Client(protocol);
		} catch (TException x) {
			x.printStackTrace();
			return null;
		}

		return client;
	}
}