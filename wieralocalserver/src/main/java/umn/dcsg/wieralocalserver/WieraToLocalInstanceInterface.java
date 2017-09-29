package umn.dcsg.wieralocalserver;

import umn.dcsg.wieralocalserver.responses.Response;
import org.apache.thrift.TException;
import org.json.JSONArray;
import org.json.JSONObject;
import umn.dcsg.wieralocalserver.thriftinterfaces.WieraToLocalInstanceIface;

import java.util.LinkedList;
import java.util.List;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by Kwangsung on 12/30/2015.
 */
public class WieraToLocalInstanceInterface implements WieraToLocalInstanceIface.Iface {
	LocalInstance m_instance = null;

	WieraToLocalInstanceInterface(LocalInstance instance) {
		m_instance = instance;
	}

	@Override
	public String peersInfo(String strPolicy) throws TException {
		JSONObject policy = new JSONObject(strPolicy);
		JSONArray peersList = (JSONArray) policy.get(VALUE);

		//Return to Wiera
		JSONObject response = new JSONObject();
		response.put(TYPE, "peersInfo");

		if (peersList != null) {
			if(m_instance.isStandAloneMode() == false && m_instance.m_peerInstanceManager != null) {
				m_instance.m_peerInstanceManager.updatePeersInfo(peersList);
				response.put(VALUE, "Done to update peers info");
			} else {
				response.put(VALUE, "Local instance is running stand alone");
			}

			response.put(RESULT, true);

		} else {
			response.put(RESULT, false);
		}

		return response.toString();
	}

	@Override
	public String ping() throws TException {
		//Return to Wiera
		JSONObject response = new JSONObject();
		response.put(TYPE, PING);
		response.put(RESULT, true);
		return response.toString();
	}

	@Override
	public String policyChange(String strPolicy) throws TException {
		JSONObject req = new JSONObject(strPolicy);
		String strReturn = null;

		if (req.has(EVENTS) == true && req.has(RESPONSES) == true) {
			// Changes Response of events.
			List<String> lstEvent = new LinkedList<>((List) req.get(EVENTS));
			List<Response> lstResponse = new LinkedList((List) req.get(RESPONSES));

			//Todo need to implement change responses of specific event using lock.
			//Need to think about how to avoid any issues (sync and lock so on)
		} else if (req.has(PRIMARY) == true) {
			//Todo simply change the primary instance name in peerManager
			//Need to consider lock!
			String strPrimary = req.getString(PRIMARY);
		}

		//Return to Wiera
		JSONObject response = new JSONObject();
		response.put(TYPE, "policyChange");

		if (strReturn != null) {
			response.put(RESULT, false);
			response.put(VALUE, strReturn);
		} else {
			response.put(RESULT, true);
			response.put(VALUE, "Set data distribution successfully");
		}

		return response.toString();
	}

	@Override
	public String dataPlacementChange(String strDataPlacement) {
		JSONObject response = new JSONObject();

		try {
			JSONObject dataPlacement = new JSONObject(strDataPlacement);
			response.put(TYPE, "dataPlacementChange");

			/*if (m_localInstance.m_peerInstanceManager.getDataDistribution().getDistributionType() == DataDistributionUtil.DATA_DISTRIBUTION_TYPE.TRIPS) {
				((TripS) m_localInstance.m_peerInstanceManager.getDataDistribution()).applyNewDataPlacement(dataPlacement);

				//Return to Wiera
				response.put(RESULT, true);
				response.put(VALUE, "Done to change placement policy");
			} else {
				response.put(RESULT, false);
				response.put(VALUE, "Data placement changes only supported in TripS mode.");
			}*/
		} catch (Exception e) {
			System.out.println("Failed in dataPlacementChange : received: " + strDataPlacement);
			e.printStackTrace();
		}

		return response.toString();
	}
}