package umn.dcsg.wieralocalserver.responses;

import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.responses.instoragecomputing.InStorageComputing;

import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.KEY;

/**
 * Created by Kwangsung on 2/4/2016.
 */
public class InStorageComputeResponse extends Response {
	protected InStorageComputing m_inStorageComputing;
	protected String m_strStorageTierName;
	protected String m_strTag;

	@Override
	protected void InitRequiredParams() {
		m_lstRequiredParams.add(KEY);
	}

	public InStorageComputeResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
		super(instance, strEventName, params); //Not supported yet
	}

	InStorageComputeResponse(LocalInstance Instance, String strEventName, InStorageComputing computing, String strStorageTierName, String strTag) {
		super(Instance, strEventName, null);
		m_inStorageComputing = computing;
		m_strStorageTierName = strStorageTierName;
		m_strTag = strTag;
	}

	@Override
	public boolean respond(Map<String, Object> responseParams) {
		//String strKey = (String)responseParams.get(KEY);

		//Need to changed to the since this will store intermediate data
		//return m_inStorageComputing.doComputing(responseParams);

		return false;
	}

	@Override
	public void doPrepareResponseParams(Map<String, Object> responseParams) {

	}
}