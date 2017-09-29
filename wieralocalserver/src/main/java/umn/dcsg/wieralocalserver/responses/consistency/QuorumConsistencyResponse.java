package umn.dcsg.wieralocalserver.responses.consistency;

import org.apache.thrift.TException;
import org.json.JSONObject;
import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.responses.Response;
import umn.dcsg.wieralocalserver.thriftinterfaces.LocalInstanceToPeerIface.Client;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.*;
import static umn.dcsg.wieralocalserver.MetaObjectInfo.NO_SUCH_VERSION;

/**
 * Created by Kwangsung on 11/28/2016.
 * This is an example for strong consistency using Quorum.
 */
public class QuorumConsistencyResponse extends Response {
    protected int m_nWriteQuorum;
    protected int m_nReadQuorum;

    public QuorumConsistencyResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
        m_strRelatedEventType = strEventName;
        m_nReadQuorum = (int) m_initParams.get(READ_QUORUM);
        m_nWriteQuorum = (int) m_initParams.get(WRITE_QUORUM);
    }

    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(KEY);

        //Only for PUT operation
        if (m_strRelatedEventType.equals(ACTION_PUT_EVENT) == true) {
            m_lstRequiredParams.add(TARGET_LOCALES);
            m_lstRequiredParams.add(VALUE);
            m_lstRequiredParams.add(VERSION);
            m_lstRequiredParams.add(TIER_NAME);
            m_lstRequiredParams.add(TAG);
            m_lstRequiredParams.add(ONLY_META_INFO);
        }
    }

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {

    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        if (isValidQuorum() == false) {
            responseParams.put(RESULT, false);
            responseParams.put(REASON, "Quorum is not valid");
            return false;
        }

        boolean bRet;
        String strReason;

        String strKey = (String) responseParams.get(KEY);

        //Read should be done first
        int nPeerCnt = m_nReadQuorum;
        int nLatestVer = NO_SUCH_VERSION;
        int nVer;
        List<String> peersList = m_localInstance.m_peerInstanceManager.getRandomPeers(nPeerCnt);
        Client peerClient;

        try {
            for (String strHostname : peersList) {
                peerClient = m_localInstance.m_peerInstanceManager.getPeerClient(strHostname);

                String strRet = peerClient.getLatestVersion(strKey);
                JSONObject ret = new JSONObject(strRet);

                if ((boolean) ret.get(RESULT) == true) {
                    nVer = (int) ret.get(VALUE);
                    if (nVer > nLatestVer) {
                        nLatestVer = nVer;
                    }
                }
            }

            //Found the latest version
            if (nLatestVer > NO_SUCH_VERSION) {
                nLatestVer++;
                responseParams.put(VERSION, nLatestVer);
            }
        } catch (TException e) {
            e.printStackTrace();
        }

        List<Class> responseList = new LinkedList<>();

        if (m_strRelatedEventType.equals(ACTION_PUT_EVENT) == true) {
            //Write first
        } else {
        }

        return Response.respondSequentiallyWithClass(m_localInstance, responseList, responseParams);
    }

    private boolean isValidQuorum() {
        int nTotalPeerCnt = m_localInstance.m_peerInstanceManager.getPeersList().size();
        return m_nReadQuorum + m_nWriteQuorum > nTotalPeerCnt && m_nWriteQuorum > nTotalPeerCnt / 2;

    }

	/*public QuorumConsistencyResponse(LocalInstance instance, String strPolicyID, long lR, long lW, ReentrantReadWriteLock lock)
	{
		super(instance, strPolicyID, lock);

		m_dataDistributionType = DATA_DISTRIBUTION_TYPE.QUORUM;
		m_strDistributionTypeName = Constants.QUORUM;

		setReadQuorum(lR);
		setWriteQuorum(lW);
	}

	public void setReadQuorum(long lR)
	{
		m_lReadQuorum = lR;
	}

	public long getReadQuorum()
	{
		return m_lReadQuorum;
	}

	public void setWriteQuorum(long lW)
	{
		m_lWriteQuorum = lW;
	}

	public long getWriteQuorum()
	{
		return m_lWriteQuorum;
	}

	LinkedList<String> getRandomQuorum(long nQuorum)
	{
		LinkedList<String> hostList = new LinkedList(m_peerInstanceManager.getPeersList().keySet());

		//Check available list and quorum
		if (nQuorum > hostList.size())
		{
			System.out.println("QuorumConsistencyResponse size cannot be more than available nodes.");
			return null;
		}

		//Choose randomly or based on access pattern.

		return hostList;
	}

	@Override
	protected MetaObjectInfo putInternal(String strKey, byte[] value, String strTierName, String tag, OperationLatency latency, boolean bFromApplication)
	{
		//Randomly (or based on information for smart selection)
		LinkedList<String> list = getRandomQuorum(m_lWriteQuorum);

		long lVersion = CheckLatestVersionFromQuorum(list, strKey);

		//Need to check how to handle version in this case. Need to write to local first! no!
		//broadcastToAllPeers(list, strKey, lVersion, value.length, value, strTierName, tag, System.currentTimeMillis(), latency);
		return null;
	}

	private long CheckLatestVersionFromQuorum(LinkedList<String> list, String strKey)
	{
		return MetaObjectInfo.NO_SUCH_VERSION;
	}

	@Override
	protected Object getInternal(String strKey, OperationLatency latency)
	{
		//Randomly (or based on information for smart selection) chosen nodes based on quorum
		LinkedList<String> list = getRandomQuorum(m_lReadQuorum);
		long lVersion = CheckLatestVersionFromQuorum(list, strKey);

		//Find closest one from list and retrieve it.
		//And update not update peer in background

		//Retrieve from the list and check version.
		return null;
	}

	@Override
	public boolean peersInformationChagesNotify()
	{
		return true;
	}

	@Override
	public boolean putFromPeerInstance(JSONObject req, String strReturnReason)
	{
		return false;
	}*/
}