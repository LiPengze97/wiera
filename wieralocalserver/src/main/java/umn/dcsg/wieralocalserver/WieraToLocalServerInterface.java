package umn.dcsg.wieralocalserver;

import org.apache.thrift.TException;
import org.json.JSONObject;
import umn.dcsg.wieralocalserver.thriftinterfaces.WieraToLocalServerIface;

/**
 * Created by Kwangsung on 12/28/2015.
 * This is the core class that provides remote function call for the Wiera central server from a Local server.
 * In other words, the Wiera central server uses this class to create, destroy and check wiera local instances.
 *
 */
public class WieraToLocalServerInterface implements WieraToLocalServerIface.Iface {
	LocalServer m_localServer = null;

	WieraToLocalServerInterface(LocalServer localServer) {
		m_localServer = localServer;
	}

	public String ping() throws TException {
		return m_localServer.pingFromWiera();
	}

	public String startInstance(String strPolicy) throws TException {
		JSONObject policy = new JSONObject(strPolicy);
		return m_localServer.startInstance(policy);
	}

	public String stopInstance(String strPolicy) throws TException {
		JSONObject policy = new JSONObject(strPolicy);
		return m_localServer.stopInstance(policy);
	}
}