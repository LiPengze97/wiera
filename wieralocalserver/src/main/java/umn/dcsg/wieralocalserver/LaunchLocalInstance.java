package umn.dcsg.wieralocalserver;

import org.json.JSONObject;

import java.io.IOException;

//This class is used only for Wiera Mode
class LaunchLocalInstance implements Runnable {
	JSONObject m_policy;
	LocalInstance m_localInstance;
	Boolean m_bStandAlone;

	public LaunchLocalInstance(JSONObject policy, Boolean bStandAlone) {
		m_policy = policy;
		m_bStandAlone = bStandAlone;
	}

	public LaunchLocalInstance() {
		m_policy = null;
	}

	@Override
	public void run() {
		try {
			//LocalInstance localInstance = new LocalInstance(filename);
			m_localInstance = new LocalInstance("config.txt", m_policy, m_bStandAlone);
			m_localInstance.runForever(m_policy);
		} catch (NoSuchFieldException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}