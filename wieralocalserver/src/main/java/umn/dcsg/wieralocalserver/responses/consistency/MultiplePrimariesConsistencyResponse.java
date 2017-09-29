package umn.dcsg.wieralocalserver.responses.consistency;

import umn.dcsg.wieralocalserver.Locale;
import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.responses.*;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by Kwangsung on 9/29/2015.
 */
public class MultiplePrimariesConsistencyResponse extends Response {
    List m_targetHostnameList = null;
    Map<String, LinkedList<Response>> m_opResponse = new HashMap<>();

    public MultiplePrimariesConsistencyResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);

        if (params.containsKey(TO) == true) {
            m_targetHostnameList = (List) params.get(TO);
        }

        //Set operation instance list
        //Put Operation
        LinkedList opList = new LinkedList<>();
        opList.add(Response.createResponse(instance, LOCK_GLOBAL_WRITE_RESPONSE, MULTIPLE_PRIMARIES_CONSISTENCY, params));

        //Store Response needs local tier-name
        //Let it use default storage-tier
        Map<String, Object> storeParams = new HashMap<>(params);
        storeParams.put(TO, "");

        opList.add(Response.createResponse(instance, STORE_RESPONSE, MULTIPLE_PRIMARIES_CONSISTENCY, storeParams));
        opList.add(Response.createResponse(instance, BROADCAST_RESPONSE, MULTIPLE_PRIMARIES_CONSISTENCY, params));
        opList.add(Response.createResponse(instance, UNLOCK_RESPONSE, MULTIPLE_PRIMARIES_CONSISTENCY, params));
        m_opResponse.put(ACTION_PUT_EVENT, opList);

        //Get Operation
        opList = new LinkedList<>();
        opList.add(Response.createResponse(instance, LOCK_GLOBAL_READ_RESPONSE, MULTIPLE_PRIMARIES_CONSISTENCY, params));
        opList.add(Response.createResponse(instance, RETRIEVE_RESPONSE, EVENTUAL_CONSISTENCY, params));
        opList.add(Response.createResponse(instance, UNLOCK_RESPONSE, MULTIPLE_PRIMARIES_CONSISTENCY, params));
        m_opResponse.put(ACTION_GET_EVENT, opList);
    }

    @Override
    protected void InitRequiredParams() {
        //For all operations type
        m_lstRequiredParams.add(KEY);

        //Only for PUT operation
        if (m_strRelatedEventType.equals(ACTION_PUT_EVENT) == true) {
            m_lstRequiredParams.add(VALUE);
        }
    }

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {

    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        List<Class> responseList = new LinkedList<>();

        if (m_strRelatedEventType.equals(ACTION_PUT_EVENT) == true) {
            //Set target locale
            if(m_targetHostnameList != null) {
                //This means Broadcasting will be done to based on the TO in policy
                //Broadcast to all
                if (m_targetHostnameList == null || ((m_targetHostnameList.size() == 1) && (m_targetHostnameList.get(0).equals(ALL) == true))) {
                    m_targetHostnameList = m_localInstance.m_peerInstanceManager.getPeersHostnameList();
                }
                responseParams.put(TARGET_LOCALES, Locale.getLocalesWithoutTierName(m_targetHostnameList));
            }
        }

        return Response.respondSequentiallyWithInstance(m_localInstance, m_opResponse.get(m_strRelatedEventType), responseParams);
    }
	/*public MultiplePrimariesConsistencyResponse(LocalInstance instance, String strPolicyID, ReentrantReadWriteLock lock)
	{
		super(instance, strPolicyID, lock);
		m_dataDistributionType = DATA_DISTRIBUTION_TYPE.MULTIPLE_MASTERS;
		m_strDistributionTypeName = Constants.MULTIPLE_PRIMARIES_CONSISTENCY;
	}

	public Object getInternal(String strKey, OperationLatency latencyInfo)
	{
		Object ret = null;
		InterProcessMutex gReadLock = null;

		try
		{
			m_dataDistributionLock.readLock().lock();
			gReadLock = getGlobalReadLock(strKey);

			gReadLock.acquire();
			ret = m_localInstance.get(strKey);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			m_dataDistributionLock.readLock().unlock();
		}

		if(gReadLock != null)
		{
			if(gReadLock.isAcquiredInThisProcess() == true)
			{
				try
				{
					gReadLock.release();
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			}
		}

		return ret;
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
			strReturnReason = "New key has been created in MultiplePrimariesConsistencyResponse Policy.";
			bRet = true;
		}
		else
		{
			strReturnReason = "Failed to crated a new key in MultiplePrimariesConsistencyResponse Policy.";
			bRet = false;
		}

		return bRet;
	}

	//Do conditional putObject to local instance peers.
	public MetaObjectInfo putInternal(String strKey, byte[] value, String strTierName, String strTag, OperationLatency latencyInfo, boolean bFromApplication)
	{
		InterProcessMutex gWriteLock = null;
		MetaObjectInfo resultPut = null;

		try
		{
			m_dataDistributionLock.readLock().lock();

			//Lock Timing.d
			latencyInfo.m_acquireGLock.start();
			gWriteLock = getGlobalWriteLock(strKey);
			gWriteLock.acquire();
			latencyInfo.m_acquireGLock.stop();

			resultPut = m_localInstance.put(strKey, value, strTierName, strTag);

			if (resultPut == null)
			{
				System.out.println("LocalInstance putObject failed in MultiplePrimariesConsistencyResponse. Should not happen");
			}
			else if (m_peerInstanceManager.getPeersList().size() > 0)    //Check any peers
			{
				if (broadcastToAllPeers(strKey, resultPut.getLastestVersion(), value, strTierName, strTag, 0, latencyInfo) != 0)
				{
					System.out.println("Failed to broadcast the updated in Multiple Master policy.");
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			m_dataDistributionLock.readLock().unlock();
		}

		if (gWriteLock != null)
		{
			if (gWriteLock.isAcquiredInThisProcess() == true)
			{
				try
				{
					gWriteLock.release();
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
		}

		return resultPut;
	}*/
}