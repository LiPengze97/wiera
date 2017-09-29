package umn.dcsg.wieralocalserver.datadistribution;

/**
 * Created by Kwangsung on 12/31/2016.
 */
public class TripS
{
/*	public final static String FASTEST = "fastest";
	public final static String CHEAPEST = "cheapest";

	HashMap<String, TierInfo.TIER_TYPE> m_targetLocaleList; //this can include myself with cost information
	HashMap<String, TierInfo.TIER_TYPE> m_dataPlacement;

	long m_lastTimeDataPlacementUpdated;

//	String m_strPreferStorageType;
	LazyUpdateManager m_lazyUpdater;
	DATA_DISTRIBUTION_TYPE m_dataDistributionInPlace;
	String m_dataDistributionInPlaceType;
	boolean m_isPrimary = false;

	public TripS(LocalInstance instance, String strPolicyID, String strPreferStorage, DATA_DISTRIBUTION_TYPE type, ReentrantReadWriteLock lock, String strPrimaryHostName)
	{
		super(instance, strPolicyID, lock);

		m_dataDistributionType = DATA_DISTRIBUTION_TYPE.TRIPS;
		m_strDistributionTypeName = Constants.TRIPS;
		m_dataDistributionInPlace = type;

		if(LocalServer.getHostName().compareTo(strPrimaryHostName) == 0)
		{
			//Only this node will moniotring the access pattern.
			//Can be generalized with leader election for later. (or can be chosen dynamically with distance from the Wiera
			m_isPrimary = true;
		}

		//For put operation.
		//Should be updated by applyNewDataPlacement
		m_targetLocaleList = new HashMap<String, TierInfo.TIER_TYPE>();

		//For get operation.
		m_dataPlacement = new HashMap<String, TierInfo.TIER_TYPE>();

//		m_strPreferStorageType = strPreferStorage;

		if(type == DATA_DISTRIBUTION_TYPE.EVENTUAL_CONSISTENCY)
		{
			m_lazyUpdater = new LazyUpdateManager(100, 16);
			m_dataDistributionInPlaceType = Constants.EVENTUAL_CONSISTENCY;
		}
		else
		{
			m_lazyUpdater = null;
			m_dataDistributionInPlaceType = Constants.MULTIPLE_PRIMARIES_CONSISTENCY;
		}

		m_lastTimeDataPlacementUpdated = System.currentTimeMillis();
	}

	public long getLastTimeDataPlacementUpdated()
	{
		return m_lastTimeDataPlacementUpdated;
	}

	public void updateAccessRule(JSONObject jsonAccessRue)
	{
		Gson gson = new Gson();

		//Convert to Map
		Type type = new TypeToken<HashMap<String, HashMap<String, HashMap<String, Double>>>>(){}.getTierType();
		JSONObject access = (JSONObject) jsonAccessRue.get(LocalServer.getHostName());

		//Get my target peer for requests
		HashMap<String, HashMap<String, HashMap<String, Double>>> accessRule = gson.fromJson(access.toString(), type);
		TierInfo.TIER_TYPE tierType;

		m_targetLocaleList.clear();

		//Set accessRule like meta.
		for(String strHostName: accessRule.keySet())
		{
			for (String strTierName : accessRule.get(strHostName).keySet())
			{
				if(strHostName.compareTo(LocalServer.getHostName()) == 0)
				{
					tierType = m_localInstance.getLocalStorageTierType(strTierName);
					m_targetLocaleList.put(strTierName, tierType);
				}
				else
				{
					m_targetLocaleList.put(strHostName + ":" + strTierName, TierInfo.TIER_TYPE.REMOTE_TIER);
				}
			}
		}
	}

	public void  updateDataPlacement(JSONObject jsonDataPlacement)
	{
		Gson gson = new Gson();

		//Convert to Map
		Type type = new TypeToken<HashMap<String, String>>(){}.getTierType();
		TierInfo.TIER_TYPE tierType;

		//Get my target peer for requests
		HashMap<String, String> dataPlacement = gson.fromJson(jsonDataPlacement.toString(), type);
		m_dataPlacement.clear();

		//Set accessRule like meta.
		for (String strHostName : dataPlacement.keySet())
		{
			String strTierName = dataPlacement.get(strHostName);

			if(strHostName.compareTo(LocalServer.getHostName()) == 0)
			{
				tierType = m_localInstance.getLocalStorageTierType(strTierName);
				m_dataPlacement.put(strTierName, tierType);
			}
			else
			{
				m_dataPlacement.put(strHostName + ":" + strTierName, TierInfo.TIER_TYPE.REMOTE_TIER);
			}
		}
	}

	public String applyNewDataPlacement(JSONObject dataPlacement)
	{
		JSONObject ret = new JSONObject();

		try
		{
			if(dataPlacement.has(Constants.ACCESS_RULE) == true)
			{
				JSONObject jsonAccessRue;
				jsonAccessRue = (JSONObject) dataPlacement.get(Constants.ACCESS_RULE);

				if (jsonAccessRue.has(LocalServer.getHostName()) == true)
				{
					updateAccessRule(jsonAccessRue);
				}
			}
			else
			{
				System.out.println("Failed to find ACCESS_RULE in TripS distribution.");
				ret.put("result", false);
				ret.put("value", "Failed to find ACCESS_RULE in TripS distribution");
			}

			//Where data will be stored
			if(dataPlacement.has(Constants.DATA_PLACEMENT) == true)
			{
				JSONObject jsonDataPlacement;
				jsonDataPlacement = (JSONObject) dataPlacement.get(Constants.DATA_PLACEMENT);

				updateDataPlacement(jsonDataPlacement);
			}
			else
			{
				ret.put("result", false);
				ret.put("value", "Failed to find DATA_PLACEMENT in TripS distribution");
				System.out.println("Failed to find DATA_PLACEMENT in TripS distribution.");
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

		//Debug
		System.out.println("------------------------------");
		System.out.println("- New Data Placement ");

		for (String strTierName: m_dataPlacement.keySet())
		{
			System.out.println(strTierName);
		}

		System.out.println("------------------------------");
		System.out.println("- New Access Rule ");

		for (String strTierName: m_targetLocaleList.keySet())
		{
			System.out.println(strTierName);
		}

		//For check speific time from this point to later data placement
		m_lastTimeDataPlacementUpdated = System.currentTimeMillis();

		return ret.toString();
	}

	@Override //Need to handle circular when violations happen multi locations.
	protected MetaObjectInfo putInternal(String strKey, byte[] value, String strTargetTierName, String strTag, OperationLatency latencyInfo, boolean bFromApplication)
	{
		MetaObjectInfo ret = null;
		String strHostName = null;
		String strTierName = null;
		String strFound = null;
		try
		{
			if(strTargetTierName.compareTo(Constants.CHEAPEST) == 0)
			{
				if(m_targetLocaleList != null)
				{
					strFound = findCheapestStorage(m_targetLocaleList, Constants.PUT_LATENCY, m_dataPlacement, m_dataDistributionInPlace);

					if (strFound != null)
					{
						strHostName = strFound.split(":")[0];
						strTierName = strFound.split(":")[1];
					}
					else
					{
						System.out.println("Should not happen. Failed to find cheapest storage");
					}
				}
				else
				{
					System.out.println("Failed to find access rule in this instance.");
				}
			}
			else
			{
				strTierName = strTargetTierName;
				strHostName = LocalServer.getHostName();
			}

			if (strTierName != null && strHostName != null)
			{
				//Distributing
				//Update tier name in case it was "cheapest"
				if(strHostName.compareTo(LocalServer.getHostName()) == 0)
				{
					latencyInfo.updateTierName(strTierName);
				}
				else
				{
					latencyInfo.updateTierName(strFound);
				}

				//Need to distribute here to all peers including myself
				if (strHostName.compareTo(LocalServer.getHostName()) == 0)
				{
					ret = distributeData(strKey, value, strTierName, strTag, latencyInfo, bFromApplication);

					if(ret != null)
					{
						//For experiment 3
						//Add access rule
						for(String strHostTierName: m_targetLocaleList.keySet())
						{
							if(m_localInstance.getLocalStorageTierType(strHostTierName) == TierInfo.TIER_TYPE.REMOTE_TIER)
							{
								//meta update
								ret.addLocale(strHostTierName, TierInfo.TIER_TYPE.REMOTE_TIER);
								m_localInstance.m_metadataStore.putObject(ret);
								m_localInstance.commitMeta(ret);
							}
						}
					}
				}
				else
				{
					String strReason = forwardingPut(strHostName, strTierName, strKey, value, strTag);

					///////////////////////////////////////////////////////////////////
					//For experiment 2
					//Add put operation for forwarding
					m_localInstance.m_localInfo.incrementalPutForCost(strHostName + ":" + strTierName);

					if (strReason != null)
					{
						System.out.println("Failed to forward request in TripS. Reason: " + strReason);
					}
					else
					{
						ret = m_localInstance.getMetadata(strKey);

						if(ret == null)
						{
							//This is dummy for forwarding
							ret = new MetaObjectInfo();
						}
					}
				}
			}
			else
			{
				System.out.printf("Failed to find hostname: %s, storage tier name: %s \n", strHostName, strTargetTierName);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

		return ret;
	}

	//In this function strLocalTierName is real Tier Name to put data into local.
	//This function is called only when local instance is in the access rule.
	private MetaObjectInfo distributeData(String strKey, byte[] value, String strLocalTierName, String strTag, OperationLatency latencyInfo, boolean bFromApplication)
	{
		MetaObjectInfo ret = null;

		//This is for remove my self for broadcasting
		HashMap<String, String> targetRemoteList = new HashMap<String, String>();

		for(String strTierName: m_dataPlacement.keySet())
		{
			TierInfo.TIER_TYPE type = m_localInstance.getLocalStorageTierType(strTierName);

			//Set to send data into remote peers
			if(type == TierInfo.TIER_TYPE.REMOTE_TIER)
			{
				targetRemoteList.put(strTierName.split(":")[0],  strTierName.split(":")[1]);
			}
		}

		//Check consistency model required.
		if(m_dataDistributionInPlace == DATA_DISTRIBUTION_TYPE.EVENTUAL_CONSISTENCY)
		{
			//Put to myself
			ret = m_localInstance.put(strKey, value, strLocalTierName, strTag);

			if(ret != null)
			{
				//Todo should be removed for Retwis case
				//experiment 4.2 TripS
				//To avoid broadcast session ID
				if(strKey.length() != "MYiSTFaH".length() || strKey.compareTo("user:uid") == 0)
				{
					m_lazyUpdater.putToQueue(targetRemoteList, strKey, ret.getLastestVersion(), value, strLocalTierName, strTag, true, latencyInfo);
				}
			}
		}
		else if(m_dataDistributionInPlace == DATA_DISTRIBUTION_TYPE.MULTIPLE_MASTERS)
		{
			InterProcessMutex gWriteLock = null;

			try
			{
				latencyInfo.m_acquireGLock.start();
				gWriteLock = getGlobalWriteLock(strKey);
				gWriteLock.acquire();
				latencyInfo.m_acquireGLock.stop();

				//Put from myself -> This should be included in broadcasting
				//For now leave it as is
				ret = m_localInstance.put(strKey, value, strLocalTierName, strTag);

				if(ret != null)
				{
					//Here need to broadcast to other peers in data placement except me.
					long lFailed = broadcastToTargetPeers(targetRemoteList, strKey, ret.getLastestVersion(), value.length, value, strTag, ret.getLastModifiedTime(), true, latencyInfo);
					//Only to peers selected by TripS
					if(lFailed > 0)
					{
						System.out.printf("%d instances failed to update metadata.\n", lFailed);
					}
				}
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
			finally
			{
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
			}
		}
		else
		{
			System.out.println("TripS only supports EventualConsistencyResponse and MultiplePrimaries for now.");
			return null;
		}

		/////////////////////////////////////////////////////////////
		//For experiment 2
		//Increase number of put for cost
		//This requests has been forwared and the cost for put is already calucated in the instance sent this request
		if(bFromApplication == true)
		{
			m_localInstance.m_localInfo.incrementalPutForCost(LocalServer.getHostName()+":"+strLocalTierName);
		}

		for (String strHostName: targetRemoteList.keySet())
		{
			m_localInstance.m_localInfo.incrementalPutForCost(strHostName+":"+targetRemoteList.get(strHostName));
		}

		return ret;
	}

	@Override
	protected Object getInternal(String strKey, OperationLatency latencyInfo)
	{
		Object ret = null;

		if(m_targetLocaleList != null)
		{
			MetaObjectInfo dbObject = m_localInstance.getMetadata(strKey);
			String strFound = null;
			String strHostName = null;
			String strTierName = null;
			boolean bLocal = false;

			if(dbObject != null)
			{
				HashMap<String, TierInfo.TIER_TYPE> tierList = dbObject.getLocaleList();
				//System.out.printf("Get Tier List cnt in meta: %d\n", tierList.size());
				strFound = findCheapestStorage(tierList, Constants.GET_LATENCY, null);
			}

			if(strFound == null)
			{
				System.out.println("Failed to find storage tier for the key: " + strKey);
				return null;
			}

			if(strFound != null)
			{
				strHostName = strFound.split(":")[0];
				strTierName = strFound.split(":")[1];
			}

			if(strTierName != null && strHostName != null)
			{
				//Distributing
				//Update tier name in case it was "cheapest"
				if(strHostName.compareTo(LocalServer.getHostName()) == 0)
				{
					latencyInfo.updateTierName(strTierName);
				}
				else
				{
					latencyInfo.updateTierName(strFound);
				}

				if(m_dataDistributionInPlace == DATA_DISTRIBUTION_TYPE.MULTIPLE_MASTERS)
				{
					InterProcessMutex gReadLock = null;

					try
					{
						gReadLock = getGlobalReadLock(strKey);
						gReadLock.acquire();

						if (strHostName.compareTo(LocalServer.getHostName()) == 0)
						{
							//Retrieve from myself
							ret = m_localInstance.get(strKey, MetaObjectInfo.LATEST_VERSION, strTierName);
							bLocal = true;
						}
						else
						{
							//Key can be locked here and remote peer don't need to check the lock
							//System.out.println("Get is forwarding");
							ret = forwardingGet(strKey, strHostName, strTierName);

						}
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}
					finally
					{
						if (gReadLock != null)
						{
							if (gReadLock.isAcquiredInThisProcess() == true)
							{
								try
								{
									gReadLock.release();
								}
								catch (Exception e)
								{
									e.printStackTrace();
								}
							}
						}
					}
				}
				else
				{
					if (strHostName.compareTo(LocalServer.getHostName()) == 0)
					{
						//Retrieve from myself
						ret = m_localInstance.get(strKey, MetaObjectInfo.LATEST_VERSION, strTierName);
						bLocal = true;
					}
					else
					{
						//Get forwarding does not contain value so not including for the cost.
						ret = forwardingGet(strKey, strHostName, strTierName);
					}
				}

				//If data retrieve from local but not it the data placement which includes this instance let's move.
				if(bLocal == true && ret != null)
				{
					if(m_dataPlacement.containsKey(strTierName) == false)
					{
						String strNewLocalTier = findLocalTierFromDataPlacement();

						//Need to move data to another local tier in this instance.
						//If there is only remote tier than do nothing
						if(strNewLocalTier != null)
						{
							m_localInstance.putInternal(strKey, (byte[])ret, strNewLocalTier);

							dbObject.addLocale(strNewLocalTier, m_localInstance.getLocalStorageTierType(strNewLocalTier));
							dbObject.removeLocale(strTierName);

							//This means data placement chagned.
							m_localInstance.deleteInternal(strKey, strTierName);
							//System.out.println("!In get operation, delete key :" + strKey + " stored in " + strTierName + "new tiername:" + strNewLocalTier);

							m_localInstance.m_metadataStore.putObject(dbObject);
							m_localInstance.commitMeta(dbObject);

							//System.out.println("Object has tier name now: " + dbObject.getLocale());
						}
					}
				}

				/////////////////////////////////////////////////////////////
				//For experiment 2
				m_localInstance.m_localInfo.incrementalGetForCost(strHostName+":"+strTierName);
			}
		}

		return ret;
	}

	@Override
	public boolean peersInformationChagesNotify()
	{
		return true;
	}

	//Cost for write here don't need to be calculated as it was done in origin location.
	@Override
	public boolean putFromPeerInstance(JSONObject req, String strReturnReason)
	{
		boolean bRet = false;
		MetaObjectInfo dataObj = null;

		try
		{
			boolean bOnlyMetaInfo = (boolean) req.get(Constants.ONLY_META_INFO);
			String strKey = (String) req.get(Constants.KEY);
			String strTierName = (String) req.get(Constants.TIER_NAME);
			String strTag = (String) req.get(Constants.TAG);
			byte[] value = Base64.decodeBase64((String) req.get(Constants.VALUE));

			dataObj = m_localInstance.getMetadata(strKey);

			if (bOnlyMetaInfo == true)
			{
				//Local version information.
				//Create new version
				//TierName is not important as I already know data placement and access set
				//For experiment 3
				//Add access rule
				for(String strHostTierName: m_targetLocaleList.keySet())
				{
					if(dataObj == null)
					{
						dataObj = m_localInstance.updateMetaDataOnly(strKey, strHostTierName, strTag);
					}
					else
					{
						dataObj.addLocale(strHostTierName, TierInfo.TIER_TYPE.REMOTE_TIER);
					}
				}

				bRet = true;
				strReturnReason = String.format("Metadata for the key: %s updated successfully.", strKey);
			}
			else
			{
				if (m_dataDistributionInPlace == DATA_DISTRIBUTION_TYPE.EVENTUAL_CONSISTENCY)
				{
					//Local version information.
					//If data was not created create it in any data distribution
					if (dataObj == null)
					{
						dataObj = m_localInstance.put(strKey, value, strTierName, strTag);
						if (dataObj != null)
						{
							//System.out.println("new key inserted.");
							strReturnReason = "New key has been created.";
							bRet = true;

							//For experiment 3
							//Add access rule
							for (String strHostTierName : m_targetLocaleList.keySet())
							{
								if (m_localInstance.getLocalStorageTierType(strHostTierName) == TierInfo.TIER_TYPE.REMOTE_TIER)
								{
									dataObj.addLocale(strHostTierName, TierInfo.TIER_TYPE.REMOTE_TIER);
								}
							}
						}
						else
						{
							strReturnReason = "Failed to crated a new key.";
							bRet = false;
						}
					}
					else
					{
						long remoteModifiedTime = Utils.convertToLong(req.get(Constants.LAST_MODIFIED_TIME));
						long lRemoteVer = Utils.convertToLong(req.get(Constants.VERSION));

						if (m_localInstance.isVersionSupported() == true)
						{
							long lLocalVer = dataObj.getLastestVersion();

							//Conflict same version.
							if (lLocalVer == lRemoteVer)
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
							else if (lLocalVer > lRemoteVer)
							{
								bRet = false;
								strReturnReason = "Newer version is available on the instance.";
							}
							else
							{
								bRet = true;
								strReturnReason = "Put operation is done in TripS EventualConsistencyResponse";
							}
						}
						else
						{
							bRet = true;
						}

						//Version need to be updated for previously exist metadata
						if (bRet == true)
						{
							dataObj = m_localInstance.updateVersion(strKey, lRemoteVer, value, strTierName, strTag, remoteModifiedTime, true);

							if (dataObj == null)
							{
								bRet = false;
								strReturnReason += "- Failed to update the value.";
							}
							else    //Local copy successfully updated
							{
								//Change to original to avoid anything bad
								dataObj.setLastModifiedTime(remoteModifiedTime);

								//For experiment 3
								//Add access rule
								for (String strHostTierName : m_targetLocaleList.keySet())
								{
									if (m_localInstance.getLocalStorageTierType(strHostTierName) == TierInfo.TIER_TYPE.REMOTE_TIER)
									{
										dataObj.addLocale(strHostTierName, TierInfo.TIER_TYPE.REMOTE_TIER);
									}
								}
							}
						}
					}
				}
				else if (m_dataDistributionInPlace == DATA_DISTRIBUTION_TYPE.MULTIPLE_MASTERS)
				{
					//Create new version
					dataObj = m_localInstance.put(strKey, value, strTierName, strTag);
					if (dataObj != null)
					{
						//System.out.println("new key inserted.");
						strReturnReason = "New key has been created in TripS MultiplePrimariesConsistencyResponse Policy.";
						bRet = true;

						//For experiment 3
						//Add access rule
						for (String strHostTierName : m_targetLocaleList.keySet())
						{
							if (m_localInstance.getLocalStorageTierType(strHostTierName) == TierInfo.TIER_TYPE.REMOTE_TIER)
							{
								dataObj.addLocale(strHostTierName, TierInfo.TIER_TYPE.REMOTE_TIER);
							}
						}
					}
					else
					{
						strReturnReason = "Failed to crated a new key in TripS MultiplePrimariesConsistencyResponse Policy.";
						bRet = false;
					}
				}
				else
				{
					bRet = false;
					strReturnReason = "TripS only supports EventualConsistencyResponse and MultiplePrimaries Consistency for now";
				}
			}

			if(bRet == true && dataObj != null)
			{
				//Comit meta data information
				m_localInstance.m_metadataStore.putObject(dataObj);
				m_localInstance.commitMeta(dataObj);

				//System.out.println(dataObj.getLocaleList().toString());
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

		return bRet;
	}

	//Primary has a responsibility to report the Wiera instance
	public boolean isPrimary()
	{
		return m_isPrimary;
	}

	public String getMostPopularInstance()
	{
		return getMostPopularInstance(m_localInstance.m_localInfo.m_lCheckPeriod);
	}

	//Currently Put operation only is considered.
	public String getMostPopularInstance(long lPeriod)
	{
		long lPutCnt = 0;
		long lGetCnt = 0;

		long lCur = System.currentTimeMillis();

		long lGetRequestCnt = m_localInstance.m_localInfo.getGetReqCntInPeriod(lCur, lPeriod);
		long lPutRequestCnt = m_localInstance.m_localInfo.getPutReqCntInPeriod(lCur, lPeriod);

		String strMostPopularHostName = LocalServer.getHostName();

		for(String strHostName: m_peersManager.getPeersList().keySet())
		{
			lGetCnt = Utils.convertToLong(m_localInstance.m_localInfo.getRemoteAccessInfo(strHostName, Constants.GET_ACCESS_CNT));
			lPutCnt = Utils.convertToLong(m_localInstance.m_localInfo.getRemoteAccessInfo(strHostName, Constants.PUT_ACCESS_CNT));

			//For now we consider put operation only as it is more expensive than get operation.
			if (lPutCnt > lPutRequestCnt)
			{
				lPutRequestCnt = lPutCnt;
				strMostPopularHostName = strHostName;
			}
		}

		return strMostPopularHostName;
	}

	public boolean checkForwaredMore()
	{
		long lCur = System.currentTimeMillis();

		long lGetRequestCnt = m_localInstance.m_localInfo.getGetReqCntInPeriod(lCur, m_localInstance.m_localInfo.m_lCheckPeriod);
		long lPutRequestCnt = m_localInstance.m_localInfo.getPutReqCntInPeriod(lCur, m_localInstance.m_localInfo.m_lCheckPeriod);

		long lGetForwaredCnt = m_localInstance.m_localInfo.getForwardedGetReqCntInPeriod(lCur, m_localInstance.m_localInfo.m_lCheckPeriod);
		long lPutForwaredCnt = m_localInstance.m_localInfo.getForwardedPutReqCntInPeriod(lCur, m_localInstance.m_localInfo.m_lCheckPeriod);

		return lGetForwaredCnt > lGetRequestCnt || lPutForwaredCnt > lPutRequestCnt;

	}

	public String findLocalTierFromDataPlacement()
	{
		TierInfo.TIER_TYPE type;

		for(String strTierName: m_dataPlacement.keySet())
		{
			type = m_dataPlacement.get(strTierName);

			if (type != TierInfo.TIER_TYPE.REMOTE_TIER)
			{
				return strTierName;
			}
		}

		return null;
	}

	public DATA_DISTRIBUTION_TYPE getDistributionInPlaceType()
	{
		return m_dataDistributionInPlace;
	}
	public String getDistributionInPlaceTypeToString()
	{
		return m_dataDistributionInPlaceType;
	}*/
}