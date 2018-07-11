package umn.dcsg.wieralocalserver.responses.peers.consistency;

import umn.dcsg.wieralocalserver.Locale;
import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.responses.*;
import umn.dcsg.wieralocalserver.responses.peers.PeerResponse;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by Kwangsung on 9/29/2015.
 */
public class EventualConsistencyResponse extends PeerResponse {
    List<Response> m_lstResponse = new LinkedList<>();

    public EventualConsistencyResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);

        //Set operation instance list
        //Put Operation

        if(strEventName.equals(ACTION_PUT_EVENT) == true) {
            //Put Operation
            m_lstResponse.add(Response.createResponse(instance, STORE_RESPONSE, EVENTUAL_CONSISTENCY, params));
            m_lstResponse.add(Response.createResponse(instance, QUEUE_RESPONSE, EVENTUAL_CONSISTENCY, params));

        } else if (strEventName.equals(ACTION_GET_EVENT) == true){
            m_lstResponse.add(Response.createResponse(instance, RETRIEVE_RESPONSE, EVENTUAL_CONSISTENCY, params));
        }
    }

    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(KEY);

        //Only for PUT operation
        if (m_strEventName.equals(ACTION_PUT_EVENT) == true) {
            m_lstRequiredParams.add(VALUE);
        }
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        return Response.respondSequentiallyWithInstance(m_localInstance, m_lstResponse, responseParams);
    }

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {
        if(responseParams.containsKey(TARGET_LOCALE) == false) {
            responseParams.put(TARGET_LOCALE, Locale.defaultLocalLocale);
        }

        if (responseParams.containsKey(TARGET_LOCALE_LIST) == false) {
            if (m_peerHostnameList == null || m_peerHostnameList.size() == 0 ||
                    ((m_peerHostnameList.size() == 1) && (m_peerHostnameList.get(0).equals(ALL) == true))) {
                m_peerHostnameList = getPeersHostnameList();
            }

            responseParams.put(TARGET_LOCALE_LIST, Locale.getLocalesWithoutTierName(m_peerHostnameList));
        }
    }

    @Override
    public boolean doCheckPeerResponseConditions(Map<String, Object> responseParams) {
        return true;
    }

/*	LazyUpdateInBackground m_lazyUpdater = null;
	//Period is set in Millisecond
	public EventualConsistencyResponse(LocalInstance instance, String strPolicyID, long lPeriod, ReentrantReadWriteLock lock)
	{
		super(instance, strPolicyID, lock);
		m_lazyUpdater = new LazyUpdateInBackground(lPeriod, 5);
		m_dataDistributionType = DATA_DISTRIBUTION_TYPE.EVENTUAL_CONSISTENCY;
		m_strDistributionTypeName = Constants.EVENTUAL_CONSISTENCY;
	}

	public void stopRunning()
	{
		m_lazyUpdater.stopRunning();
	}

	public long getPeriod()
	{
		return m_lazyUpdater.getPeriod();
	}

	//Conflict check.
	//Need to send to timer..
	//Internal timer.. OK -KS
	public MetaObjectInfo putInternal(String strKey, byte[] value, String strTierName, String tag, OperationLatency latencyInfo, boolean bFromApplication)
	{
		MetaObjectInfo resultPut = null;

		try
		{
			m_dataDistributionLock.readLock().lock();
			resultPut = m_localInstance.put(strKey, value, strTierName, tag);

			if (resultPut == null)
			{
				System.out.println("LocalInstance putObject failed in EventualConsistencyResponse. Should not happen");
			}
			else if(m_peerInstanceManager.getPeersList().size() > 0)	//Check any peer is available
			{
				if(m_lazyUpdater.putToQueue(null, strKey, resultPut.getLastestVersion(), value, strTierName, tag, false, latencyInfo) == false)
				{
					System.out.println("Failed to putObject update to the local QueueResponse.");
				}
			}
		}
		finally
		{
			m_dataDistributionLock.readLock().unlock();
		}

		return resultPut;
	}

	public Object getInternal(String strKey, OperationLatency latencyInfo)
	{
		Object ret = null;

		try
		{
			m_dataDistributionLock.readLock().lock();
			ret = m_localInstance.get(strKey);
		}
		finally
		{
			m_dataDistributionLock.readLock().unlock();
		}

		return ret;
	}

	@Override
	public boolean peersInformationChagesNotify()
	{
		//Not use for now in this policy
		return false;
	}

	@Override //Only handle update
	public boolean putFromPeerInstance(JSONObject req, String strReturnReason)
	{
		String strKey = (String) req.get(Constants.KEY);
		long nRemoteVer = (int) req.get(Constants.VERSION);
		byte[] value = Base64.decodeBase64((String) req.get(Constants.VALUE));
		String strTierName = (String) req.get(Constants.TIER_NAME);
		String strTag = (String) req.get(Constants.TAG);

		//Local version information.
		MetaObjectInfo dataObj = m_localInstance.getMetadata(strKey);
		boolean bRet;

		//Todo TierName should be changed based on local policy. -KS
		//If data was not created create it in any data distribution
		if (dataObj == null)
		{
			//Create new version
			if (m_localInstance.put(strKey, value, strTierName, strTag) != null)
			{
				//System.out.println("new key inserted.");
				strReturnReason = "New key has been created.";
				bRet = true;
			}
			else
			{
				strReturnReason = "Failed to crated a new key.";
				bRet = false;
			}
		}
		else
		{
			long lLocalVer = dataObj.getLastestVersion();
			long remoteModifiedTime = Utils.convertToLong(req.get(Constants.LAST_MODIFIED_TIME));

			//Conflict same version.
			if (lLocalVer == nRemoteVer)
			{
				long localModifiedTime = dataObj.getLastModifiedTime();

				//Now simply check the time.
				if (remoteModifiedTime > localModifiedTime)
				{
					bRet = true;
					strReturnReason = "There was a conflicts (same version is available) but updated based on modified time.";
				}
				else
				{
					bRet = false;
					strReturnReason = "There was a conflicts (same version is available). Update was not done.";
				}
			}
			else if (lLocalVer > nRemoteVer)
			{
				bRet = false;
				strReturnReason = "Newer version is available on the instance.";
			}
			else
			{
				bRet = true;
				strReturnReason = "Put operation is done in EventualConsistencyResponse";
			}

			if (bRet == true)
			{
				dataObj = m_localInstance.putKeyWithVer(strKey, nRemoteVer, value, strTierName, strTag, remoteModifiedTime, true);

				if (dataObj == null)
				{
					strReturnReason += "- Failed to update the value.";
				}
				else    //Local copy successfully updated
				{
					//Change to original to avoid anything bad
					dataObj.setLastModifiedTime(remoteModifiedTime);
				}
			}
		}

		return bRet;
	}*/
}