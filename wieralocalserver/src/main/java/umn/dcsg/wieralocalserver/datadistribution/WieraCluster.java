package umn.dcsg.wieralocalserver.datadistribution;

/**
 * Created by Kwangsung on 11/2/2016.
 * This class will make multiple local instance as a single local cluster (possibly can be used for multiple node on
 * a single data center)
 */
public class WieraCluster
{/*
	class LeaderClient extends LeaderSelectorListenerAdapter implements Closeable
	{
		boolean m_bLeader;
		String m_strLeaderHostName;

		LeaderSelector m_leaderSelector = null;

		LeaderClient(CuratorFramework client, String path)
		{
			m_leaderSelector = new LeaderSelector(client, path, this);

			// for most cases you will want your instance to re-QueueResponse when it relinquishes leadership
			m_leaderSelector.autoRequeue();
			m_leaderSelector.start();
		}

		@Override
		public void close() throws IOException
		{
			m_leaderSelector.close();
		}

		@Override
		public void takeLeadership(CuratorFramework client) throws Exception
		{
			// we are now the leader. This method should not return until we want to relinquish leadership
			System.out.println(LocalServer.getHostName() + " is now the leader for policy:" + m_strPolicyID);

			//Brodcast now I'm the leader
			m_bLeader = true;
			m_strLeaderHostName = LocalServer.getHostName();

			while(true)
			{
				//There is another instance who has lower average latency and thus more appropriate to be a leader than me
				//I will resign the leader but will keep in the leader pool
				//Parameter means distribution allowed.
		//		if(m_localInstance.m_localInfo.findLowerAveDCsLatency(5) == true)
		//		{
		//			m_bLeader = false;
		//			break;
		//		}

				//Every 5 seconds for now
				Thread.sleep(5000);
			}
		}
	}

	public final static String FASTEST = "fastest";
	public final static String CHEAPEST = "cheapest";
	public final static String WORKLOAD_AWARE = "workload_aware";
	final List<String> m_predeterminedStorage = new LinkedList<String>(Arrays.asList(FASTEST, CHEAPEST, WORKLOAD_AWARE));

	LeaderClient m_leaderClient;

	public WieraCluster(LocalInstance instance, String strPolicyID, ReentrantReadWriteLock lock)
	{
		super(instance, strPolicyID, lock);
		m_dataDistributionType = DATA_DISTRIBUTION_TYPE.WIERA_CLUSTER;
		m_strDistributionTypeName = "WIERA_CLUSTER";

		//This leader will be used only for local Cluster lock
		//Zookeeper may run locally separated from the global one
		String strLeaderElection = "/wiera/" + m_strPolicyID + "/" + "leaderSelector";
		m_leaderClient = new LeaderClient(m_zkClient, strLeaderElection);
	}

	public Object getInternal(String strKey, OperationLatency latencyInfo)
	{
		Object ret = false;
		MetaObjectInfo obj = m_localInstance.getMetadata(strKey);

		if(obj != null)
		{
			String strTierName = obj.getLocale();

			if(m_localInstance.isLocalStorageTier(strTierName) == true)
			{
				//Retrieve from local
				ret = m_localInstance.get(strKey);
			}
			else
			{
				//Retrieve from remote tier
				String strRemoteHostName = strTierName.split(":")[0];
				String strRemoteTierName = strTierName.split(":")[1];

				PeerInstanceIface.Client client = m_peersManager.getPeerClient(strRemoteHostName);

				if(client != null)
				{
					JSONObject req = new JSONObject();
					req.put(Constants.KEY, strKey);
					req.put(Constants.TIER_NAME, strRemoteTierName);
					req.put(Constants.VERSION, obj.getLastestVersion());

					String strResponse = null;
					try
					{
						strResponse = client.get(req.toString());

						JSONObject response = new JSONObject(strResponse);
						boolean bRet = (boolean)response.get(Constants.RESULT);

						if(bRet == true)
						{
							ret = Base64.decodeBase64((String) response.get(Constants.VALUE));
						}
						else
						{
							System.out.println("Fail to retrieve data from peer: " + strRemoteHostName);
						}
					}
					catch (TException e)
					{
						e.printStackTrace();
					}
					finally
					{
						m_peersManager.releasePeerClient(strRemoteHostName, client);
					}
				}
			}
		}
		else
		{
			System.out.println("Failed to find metadata for the key: " + strKey);
		}

		return ret;
	}

	//Do conditional putObject to local instance peers.
	public MetaObjectInfo putInternal(String strKey, byte[] value, String strTierName, String strTag, OperationLatency latencyInfo, boolean bFromApplication)
	{
		MetaObjectInfo resultPut = null;

		//Check the storage is local (specified storage). if not, it should be forwarded.
		//Need to check pre-determined storage
		if(m_localInstance.isLocalStorageTier(strTierName) == true)
		{
			//Here can be reached by user put or forward put from another peer
			//write to local and update meta to other
			resultPut = m_localInstance.put(strKey, value, strTierName, strTag);

			if(resultPut != null)
			{
				//Only metadata need to be updated.
				//For now, meta data will be propagated using synchronously.
				long lFailed = broadcastToAllPeers(strKey, resultPut.getLastestVersion(), value.length, "".getBytes(), strTierName, strTag, resultPut.getLastModifiedTime(), latencyInfo);

				if(lFailed != 0)
				{
					System.out.printf("%d instances failed to update metadata.\n", lFailed);
				}
			}
		}
		else
		{
			//Find a storage tier to forward putObject operation
			//This means each instance wihtin wiera cluster should know all storage info of other tiers
			String strHostAndStorageTier = findStorageTier(strTierName,Constants.PUT_LATENCY, value.length);

			System.out.printf("Found Storage: %s\n", strHostAndStorageTier);

			if(strHostAndStorageTier != null)
			{
				String strHostName = strHostAndStorageTier.split(":")[0];
				String strFoundTierName = strHostAndStorageTier.split(":")[1];

				//Find desired storage from local instance
				if(strHostName.compareTo(LocalServer.getHostName()) == 0)
				{
					//Store to local storage tier
					resultPut = m_localInstance.put(strKey, value, strFoundTierName, strTag);

					if (resultPut == null)
					{
						System.out.println("LocalInstance putObject failed in cluster mode. Should not happen");
					}
				}
				else
				{
					System.out.println("[Debug]Tier is not local. Forward to host: " + strHostName);
					//Send to peer to store to remote storage tier
					forwardingPut(strHostName, strFoundTierName, strKey, value, strTag);
				}
			}
			else
			{
				System.out.printf("Failed to find the storage tier: %s\n", strTierName);
			}
		}

		return resultPut;
	}

	//This is the most important function for now (Finding appropriate host with pre-defined storage alias)
	protected String findStorageTier(String strTierName, String strOPType, long IDataSize)
	{
		//Supported pre-determined storage
		if(m_predeterminedStorage.contains(strTierName) == true)
		{
			if(strTierName.compareTo(CHEAPEST) == 0)
			{
				System.out.println("OK let's find the cheapest storage.");
				return findCheapestStorage();
			}
			else if(strTierName.compareTo(FASTEST) == 0)
			{
				System.out.println("OK let's find the fastest storage.");
				return findFastestStorage(strOPType, IDataSize);
			}
			else if(strTierName.compareTo(WORKLOAD_AWARE) == 0)
			{
				System.out.println("OK let's find the best storage for the current workload.");
				return findWorkdloadAwareStorage();
			}
			else if(strTierName.startsWith("query:") == true) // -> Can be a query to find a storage.
			{
				return queryStorage(strTierName);
			}

			else
			{
				//Not supported yet.
				System.out.printf("Not supported storage name yet: %s\n", strTierName);
			}
		}

		return null;
	}

	@Override
	public boolean peersInformationChagesNotify()
	{
		if(m_leaderClient.m_bLeader == true)
		{
			PeerInstanceIface.LocalInstanceClient client = m_localInstance.m_peersManager.getPeerClient(strHostName);

			//Need to change to json object.
			//Create JSONObject which will be sent to all peers.
			JSONObject req = new JSONObject();
			req.put(Constants.LEADER_HOSTNAME, m_leaderClient.m_strLeaderHostName);

			try
			{
				String strRet = client.setLeader(req.toString());
				JSONObject ret = new JSONObject(strRet);

				if((boolean)ret.get(Constants.RESULT) == false)
				{
					System.out.printf("Failed to send leader information to %s: Reason: %s", strHostName, ret.get(Constants.VALUE));
				}
				else
				{
					return true;
				}
			}
			catch (TException e)
			{
				System.out.printf("Exception while sending leader information to %s: Reason: %s", strHostName, e.getMessage());
				//e.printStackTrace();
			}
		}

		return false;
	}

	@Override
	public boolean putFromPeerInstance(JSONObject req, String strReturnReason)
	{
		String strKey = "";
		//In here only metadata needs to be update with information of location where data is stored
		try
		{
			strKey = (String) req.get(Constants.KEY);
			long lVersion = Utils.convertToLong(req.get(Constants.VERSION));
			long lSize = Utils.convertToLong(req.get(Constants.SIZE));
			String strTierName = (String) req.get(Constants.TIER_NAME);
			String strTag = (String) req.get(Constants.TAG);
			long lLastModifiedTime = Utils.convertToLong(req.get(Constants.LAST_MODIFIED_TIME));
			String strHostName = (String) req.get(Constants.HOSTNAME);

			//Update meta info
			MetaObjectInfo dataObj = m_localInstance.updateRemoteMetadata(strKey, lVersion, lSize, strHostName+":"+strTierName, strTag, lLastModifiedTime);

			if(dataObj != null)
			{
				strReturnReason = "Update meta data for the key: " + strKey;
				return true;
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

		strReturnReason = "Failed to update meta data for the key: " + strKey;
		return false;
	}

	public boolean checkLeader()
	{
		return m_leaderClient.m_bLeader;
	}

	public void setLeaderName(String strLeaderHostName)
	{
		m_leaderClient.m_strLeaderHostName = strLeaderHostName;
	}

	private String findFastestStorage(String strOPType, long lSize)
	{
		double dbLowest = 9999999;
		double dbLatency = 0;
		String strFastestTierName = null;
		String strFastestTierHostName = null;

		//Search local storage tier first.a

		for (String strTierName: m_localInstance.m_localInfo.getLocalTiers().keySet())
		{
			dbLatency = m_localInstance.m_localInfo.getLocalTierLatency(strTierName, strOPType);

			//Check enough size for the storage tier
			if(dbLatency > 0 && dbLatency < dbLowest && m_localInstance.m_localInfo.getLocalTierFreeSpace(strTierName) >= lSize)
			{
				dbLowest = dbLatency;
				strFastestTierName = strTierName;
				strFastestTierHostName = LocalServer.getHostName();
			}
		}

		for(String strHostName: m_localInstance.m_localInfo.getRemoteTiers().keySet())
		{
			for(String strTierName: m_localInstance.m_localInfo.getRemoteTiers().get(strHostName).keySet())
			{
				dbLatency = m_localInstance.m_localInfo.getRemoteTierAveLatency(strHostName, strTierName, strOPType);

				//Check remote tier has enough storage
				if(dbLatency > 0 && dbLatency <= dbLowest && m_localInstance.m_localInfo.getRemoteTierFreeSpace(strHostName, strTierName) >= lSize)
				{
					dbLowest = dbLatency;
					strFastestTierName = strTierName;
					strFastestTierHostName = strHostName;
				}
			}
		}

		return String.format("%s:%s", strFastestTierHostName, strFastestTierName);
	}

	private String findWorkdloadAwareStorage()
	{
		return null;
	}

	private String findCheapestStorage()
	{
		return null;
	}

	//It might be available
	private String queryStorage(String strQuery)
	{
		return null;
	}*/
}