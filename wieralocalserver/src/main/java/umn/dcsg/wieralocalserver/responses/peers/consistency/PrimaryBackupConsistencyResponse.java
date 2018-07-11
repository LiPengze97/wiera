package umn.dcsg.wieralocalserver.responses.peers.consistency;

import umn.dcsg.wieralocalserver.LocalServer;
import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.Locale;
import umn.dcsg.wieralocalserver.responses.*;
import umn.dcsg.wieralocalserver.responses.peers.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by Kwangsung on 9/29/2015.
 */
public class PrimaryBackupConsistencyResponse extends PeerResponse {
    public PrimaryBackupConsistencyResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
    }

    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(KEY);
        m_lstRequiredParams.add(PRIMARY);

        //Only for PUT operation
        if (m_strEventName.equals(ACTION_PUT_EVENT) == true) {
            m_lstRequiredParams.add(VALUE);
        }
    }

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {
        if(responseParams.containsKey(PRIMARY) == false) {
            responseParams.put(PRIMARY, m_peerInstanceManager.getPrimaryPeerHostname());
            responseParams.put(TARGET_LOCALE, Locale.getLocalesWithoutTierName(m_peerInstanceManager.getPrimaryPeerHostname()));
        }

        responseParams.put(LAZY_UPDATE, m_localInstance.getPolicyConfBoolean(LAZY_UPDATE));

        if (responseParams.containsKey(TARGET_LOCALE_LIST) == false) {
            if (m_peerHostnameList == null || m_peerHostnameList.size() == 0 ||
                    ((m_peerHostnameList.size() == 1) && (m_peerHostnameList.get(0).equals(ALL) == true))) {
                m_peerHostnameList = getPeersHostnameList();
            }

            responseParams.put(TARGET_LOCALE_LIST, Locale.getLocalesWithoutTierName(m_peerHostnameList));
        }
    }

    @Override
    public boolean doCheckResponseConditions(Map<String, Object> responseParams) {
        return true;
    }

    @Override
    public boolean doCheckPeerResponseConditions(Map<String, Object> responseParams) {
        return true;
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        List<Class> lstResponse = new LinkedList<>();
        String strPrimaryHostname = (String)responseParams.get(PRIMARY);

        if (LocalServer.getHostName().equals(strPrimaryHostname) == true) { //Check primary
            if (m_strEventName.equals(ACTION_PUT_EVENT) == true) {
                lstResponse.add(StoreResponse.class);

                //Distribute update lazily
                //Use queue response for updating in background
                if (responseParams.containsKey(LAZY_UPDATE) == true && (boolean) responseParams.get(LAZY_UPDATE) == true) {
                    lstResponse.add(QueueResponse.class);
                } else {
                    lstResponse.add(BroadcastResponse.class);
                }
                return Response.respondSequentiallyWithClass(m_localInstance, lstResponse, responseParams);
            } else {
                return Response.respondAtRuntimeWithClass(m_localInstance, RetrieveResponse.class, responseParams);
            }
        } else {    //I'm secondary
            //Set primary target
            if (m_strEventName.equals(ACTION_PUT_EVENT) == true) {
                return Response.respondAtRuntimeWithClass(m_localInstance, ForwardPutResponse.class, responseParams);
            } else {
                //Check local first (regraardless
                if(Response.respondAtRuntimeWithClass(m_localInstance, RetrieveResponse.class, responseParams) == false) {
                    return Response.respondAtRuntimeWithClass(m_localInstance, ForwardGetResponse.class, responseParams);
                }

                return true;
            }
        }
    }

	/*String m_strPrimaryHostName;
	boolean m_isPrimary = false;

	public PrimaryBackupConsistencyResponse(String strPrimaryHostName, LocalInstance instance, String strPolicyID, ReentrantReadWriteLock lock)
	{
		super(instance, strPolicyID, lock);
		m_strPrimaryHostName = strPrimaryHostName;

		if(LocalServer.getHostName().equals(m_strPrimaryHostName))
		{
			System.out.println("This node becomes a primary node.");
			m_isPrimary = true;
		}
		else
		{
			System.out.println("This node becomes a secondary node.");
			m_isPrimary = false;
		}

		m_dataDistributionType = DATA_DISTRIBUTION_TYPE.PRIMARY_BACKUP;
		m_strDistributionTypeName = Constants.PRIMARY_BACKUP;
	}

	public String getPrimaryHostName() {return m_strPrimaryHostName;}
	public boolean isPrimary()
	{
		return m_isPrimary;
	}

	public String findInstanceForwardingMore(long lPeriodInSec)
	{
		long lDirectRequestCnt = getDirectPutReqCnt(lPeriodInSec);
		long lDifference = 0;
		long lMaxDifference = 0;
		long lForwardedRequestCnt = 0;
		long lCurTime = System.currentTimeMillis();
		String newPrimaryHostName = null;
		LinkedList<Long> forwardedTimeList;

		Iterator it = m_requestCntInfo.entrySet().iterator();

		while (it.hasNext())
		{
			Map.Entry pair = (Map.Entry)it.next();
			forwardedTimeList = (LinkedList<Long>)pair.getValue();
			lForwardedRequestCnt = getReqCntInPeriod(lCurTime, forwardedTimeList, lPeriodInSec);

			if(lDirectRequestCnt < lForwardedRequestCnt)
			{
				lDifference = lForwardedRequestCnt - lDirectRequestCnt;

				if(lDifference > lMaxDifference)
				{
					lMaxDifference = lDifference;
					newPrimaryHostName = (String)pair.getKey();
				}
			}
		}

		return newPrimaryHostName;
	}

	public long increaseForwardedRequestCnt(String strHostname)
	{
		m_forwardedPutReqList.add(System.currentTimeMillis());

		LinkedList<Long> list;

		if(m_requestCntInfo.containsKey(strHostname) == false)
		{
			list = new LinkedList<>();
			m_requestCntInfo.put(strHostname, list);
		}
		else
		{
			list = m_requestCntInfo.get(strHostname);
		}

		list.add(System.currentTimeMillis());

		return list.size();
	}

	public Object getInternal(String strKey, OperationLatency latencyInfo)
	{
		Object ret = null;
		final String strPeer = "US-EAST";

		try
		{
			//Test timing purpose.
			m_totalGetReqList.put(System.currentTimeMillis(), isPrimary());

			m_dataDistributionLock.readLock().lock();
			//Should use this.
			//ret = m_localInstance.getObject(strKey);

			//For test purpose.
			//Need to be removed.
			//Todo should be removed
			PeerInstanceIface.WieraClient client = m_peerInstanceManager.getPeerClient(strPeer);

			if(client != null)
			{
				JSONObject getReq = new JSONObject();

				try
				{
					getReq.put(Constants.KEY, strKey);
					String strResponse = client.get(getReq.toString());

					JSONObject response = new JSONObject(strResponse);
					boolean bRet = (boolean)response.get(Constants.RESULT);

					if(bRet == true)
					{
						ret = Base64.decodeBase64((String) response.get(Constants.VALUE));
					}
					else
					{
						System.out.println("Fail to retrieve data from peer: " + strPeer);
					}
				}
				catch (TException e)
				{
					e.printStackTrace();
				}
				finally
				{
					m_peerInstanceManager.releasePeerClient(strPeer, client);
				}
			}
			else
			{
				System.out.println("Read local data. LocalInstanceCLI is null peer : " + strPeer);
				ret = m_localInstance.get(strKey);
			}
		}
		finally
		{
			m_dataDistributionLock.readLock().unlock();
		}

		return ret;
	}

	//Do conditional putObject to local instance peers.
	public MetaObjectInfo putInternal(String strKey, byte[] value, String strTierName, String strTag, OperationLatency latencyInfo, boolean bFromApplication)
	{
		MetaObjectInfo resultPut = null;

		//Broadcast needs to be sync.
		ReentrantReadWriteLock broadcastKeyLock = m_broadcastKeyLocker.getLock(strKey);

		try
		{
			//Lock for changing data distribution.
			m_dataDistributionLock.readLock().lock();

			if (m_isPrimary == true)
			{
				broadcastKeyLock.writeLock().lock();

				resultPut = m_localInstance.put(strKey, value, strTierName, strTag);

				if (resultPut == null)
				{
					System.out.println("LocalInstance putObject failed in PrimaryBackupConsistencyResponse. Should not happen");
				}
				else if(m_peerInstanceManager.getPeersList().size() > 0) //Check any peer
				{
					m_totalPutReqList.add(System.currentTimeMillis());

					if(broadcastToAllPeers(strKey, resultPut.getLastestVersion(), value.length, value, strTierName, strTag, 0, latencyInfo) != 0)
					{
						System.out.println("Failed to broadcast the updated in PrimaryBackupConsistencyResponse.");
					}
				}
			}
			else
			{
				//Follow other instance's internal policy for storing data as passing "" for the second parameter
				String strReason = forwardingPut(m_strPrimaryHostName, "", strKey, value, strTag);

				if(strReason != null)
				{
					System.out.println("failed to forward request to primary node in PrimaryBackupConsistencyResponse. Reason: " + strReason);
				}
				else
				{
					m_forwardingPutReqList.add(System.currentTimeMillis());
					//System.out.println("forward request to primary node in PrimaryBackupConsistencyResponse.");
				}
			}
		}
		finally
		{
			if(m_dataDistributionLock.getReadLockCount() > 0)
			{
				m_dataDistributionLock.readLock().unlock();
			}

			if(m_isPrimary == true)
			{
				broadcastKeyLock.writeLock().unlock();
			}
		}

		return resultPut;
	}

	@Override
	public boolean peersInformationChagesNotify()
	{
		//Not use for now in this policy
		return false;
	}

	@Override
	public boolean putFromPeerInstance(JSONObject req, String strReturnReason)
	{
		String strKey = (String) req.get(Constants.KEY);
		byte[] value = Base64.decodeBase64((String) req.get(Constants.VALUE));
		String strTierName = (String) req.get(Constants.TIER_NAME);
		String strTag = (String) req.get(Constants.TAG);

		boolean bRet;

		//Create new version
		if (m_localInstance.put(strKey, value, strTierName, strTag) != null)
		{
			//System.out.println("new key inserted.");
			strReturnReason = "New key has been created in PrimaryBackupConsistencyResponse Policy.";
			bRet = true;
		}
		else
		{
			strReturnReason = "Failed to crated a new key in PrimaryBackupConsistencyResponse Policy.";
			bRet = false;
		}

		return bRet;
	}*/
}