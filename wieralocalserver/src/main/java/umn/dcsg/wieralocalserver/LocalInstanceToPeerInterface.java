package umn.dcsg.wieralocalserver;

import com.google.gson.Gson;
import umn.dcsg.wieralocalserver.info.Latency;
import org.apache.commons.codec.binary.Base64;
import org.apache.thrift.TException;
import org.json.JSONObject;
import umn.dcsg.wieralocalserver.thriftinterfaces.LocalInstanceToPeerIface;
import umn.dcsg.wieralocalserver.utils.Utils;

import java.nio.ByteBuffer;
import java.util.HashMap;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by Kwangsung on 12/28/2015.
 */
public class LocalInstanceToPeerInterface implements LocalInstanceToPeerIface.Iface {
    LocalInstance m_localInstance = null;
    //protected ReentrantReadWriteLock  = null;

    public LocalInstanceToPeerInterface(LocalInstance instance) {
        this.m_localInstance = instance;
        //m_dataDistributionLock = new ReentrantReadWriteLock();
    }

    @Override
    public String ping() throws TException {
        String strReason = NOT_HANDLED;
        Latency latencyForRetrieving = new Latency();
        latencyForRetrieving.start();

        JSONObject response = new JSONObject();
        response.put(TYPE, PING);

        try {
            //Need to configure storage information to sent with return
            //Now return storage info and average_dcs_latency from each instance for leader election
            HashMap<String, HashMap<String, Double>> storageInfo = m_localInstance.m_localInfo.getStorageInfo(true);
            HashMap<String, Double> networkInfo = m_localInstance.m_localInfo.getDCsLatencyInfo();
            HashMap<String, Long> accessInfo = m_localInstance.m_localInfo.getLatestLocalAccessInfo();
            latencyForRetrieving.stop();

            Gson gson = new Gson();

            response.put(RESULT, true);
            response.put(OP_TIME, latencyForRetrieving.getLatencyInMills());    //This is special case as this call will be used for checking latency between DCs
            response.put(VALUE, gson.toJson(storageInfo));
            response.put(VALUE2, gson.toJson(networkInfo));
            response.put(VALUE3, gson.toJson(accessInfo));
        } catch (Exception e) {
            e.printStackTrace();
            response.put(RESULT, false);
            response.put(OP_TIME, latencyForRetrieving.getLatencyInMills());    //This is special case as this call will be used for checking latency between DCs
            response.put(REASON, e.getMessage());
        }

        return response.toString();
    }

    @Override
    public String forwardPutRequest(String strPutReq){
        String strReason = NOT_HANDLED;
        boolean bRet;
        int nVer;
        long lLastModifiedTime;

        JSONObject response = new JSONObject();

        JSONObject obj = new JSONObject(strPutReq);
        String strKey = (String) obj.get(KEY);
        byte[] value = Base64.decodeBase64((String) obj.get(VALUE));
        String strFrom = (String) obj.get(FROM);
        String strTag;

        if(obj.has(TAG) == true) {
            strTag = (String) obj.get(TAG);
        }

        //Handling when peer instance does not know about the tiername in this instance.
        //Find and use the fastest storage tier in this instance
        if (m_localInstance.m_applicationToLocalInstanceInterface.put(strKey, ByteBuffer.wrap(value)) == true) {
            m_localInstance.m_localInfo.incrementalForwardedRequestCnt(strFrom, PUT);

            MetaObjectInfo meta = m_localInstance.getMetadata(strKey);
            nVer = meta.getLastestVersion();
            lLastModifiedTime = meta.getLastModifiedTime();

            strReason = OK;
            bRet = true;
        } else {
            strReason = "Failed to putObject the key-value forwarded into Primary Instance";
            bRet = false;

            nVer = MetaObjectInfo.NO_SUCH_VERSION;
            lLastModifiedTime = 0;
        }

        response.put(TYPE, "forwardPutRequest");
        response.put(RESULT, bRet);
        response.put(VALUE, strReason);
        response.put(REASON, strReason);
        response.put(VERSION, nVer);
        response.put(LAST_MODIFIED_TIME, lLastModifiedTime);

        //System.out.println("Forwarding: " + strReason);
        return response.toString();
    }

    @Override
    public String get(String strReq) throws TException {
        //This is forwarded get operation from other peers
        JSONObject response = new JSONObject();

        try {
            JSONObject obj = new JSONObject(strReq);
            int nVer = MetaObjectInfo.LATEST_VERSION;
            String strKey = (String) obj.get(KEY);
            String strFrom = (String) obj.get(FROM);
            String strTierName = m_localInstance.m_strDefaultTierName;

            //check if version is specified
            if (obj.has(VERSION) == true) {
                nVer = (int) obj.get(VERSION);
            }

            if (obj.has(TIER_NAME) == true) {
                strTierName = (String) obj.get(TIER_NAME);
            }
            //Handling when peer instance does not know about the tiername in this instance.
            //Find and use the fastest storage tier in this instance
            MetaObjectInfo meta = m_localInstance.getMetadata(strKey);

            if (meta != null) {
                //Found as requested
                //I don't think this will be true for now as forwared request does not need to know the non-local storage tiername
                //This may happen in TripS mode in which not all instances have replicas
                Locale targetLocale = meta.getLocale(true);

                if (strTierName.equals(targetLocale.getTierName()) != true) {
                    System.out.println("[debug] Retrieving the object requested from different Tier: " + targetLocale.getTierName());
                    strTierName = targetLocale.getTierName();
                }
            }

            //bUpdateMeta is true as the object is accessed by peer
            byte[] bytes = m_localInstance.get(strKey, nVer, strTierName, true);

            //Increase forwarded get operation
            m_localInstance.m_localInfo.incrementalForwardedRequestCnt(strFrom, GET);

            if (bytes == null) {
                response.put(RESULT, false);
                response.put(REASON, "Failed to find a value associated with the key");
            } else {
                response.put(RESULT, true);
                response.put(VALUE, Base64.encodeBase64String(bytes));
            }
        } catch (Exception e) {
            e.printStackTrace();
            response.put(RESULT, false);
            response.put(REASON, e.getMessage());
        }

        response.put(TYPE, GET);
        return response.toString();
    }

    @Override
    public String getLatestVersion(String strKey) throws TException {
        String strReason = NOT_HANDLED;
        boolean bRet = false;
        JSONObject response = new JSONObject();

        int nVer = m_localInstance.getLatestVersion(strKey);

        if (nVer >= 0) {
            strReason = OK;
            bRet = true;
        } else {
            strReason = "Failed to find the latest version";
        }

        response.put(RESULT, bRet);
        response.put(VALUE, nVer);
        response.put(VERSION, nVer);
        response.put(REASON, strReason);
        response.put(TYPE, "getLatestVersion");

        return response.toString();
    }

    @Override //This function is only used for update broadcasting
    public String put(String strReq) {
        JSONObject req = new JSONObject(strReq);
        String strReason = NOT_HANDLED;
        boolean bRet;

        String strKey = (String) req.get(KEY);
        byte[] value = Base64.decodeBase64((String) req.get(VALUE));
        String strTag = (String) req.get(TAG);
        MetaObjectInfo meta = m_localInstance.getMetadata(strKey);

        String strTierName;
        if(req.has(TIER_NAME) == true && m_localInstance.isLocalStorageTier(req.getString(TIER_NAME)) == true) {
            strTierName = (String) req.get(TIER_NAME);
        } else {
            strTierName = m_localInstance.m_strDefaultTierName;
        }

        //System.out.println("[debug] I will write to :" + strTierName);

        boolean bConflictCheck = false;
        if(req.has(CONFLICT_CHECK) == true) {
            bConflictCheck = (boolean) req.get(CONFLICT_CHECK);
        }

        //Just Write to local without checking version conflict
        if (bConflictCheck == false || meta == null) {
            //Create new version
            if (m_localInstance.put(strKey, value, strTierName, strTag, true) != null) {
                //System.out.println("new key inserted.");
                strReason = OK;
                bRet = true;
            } else {
                strReason = "Failed to create a new key: " + strKey;
                bRet = false;
            }
        } else {
            int nRemoteVer = (int) req.get(VERSION);
            int nLocalVer = meta.getLastestVersion();
            long remoteModifiedTime = Utils.convertToLong(req.get(LAST_MODIFIED_TIME));

            //Conflict same version.
            if (nLocalVer == nRemoteVer) {
                long localModifiedTime = meta.getLastModifiedTime();

                //Now simply check the time.
                if (remoteModifiedTime > localModifiedTime) {
                    bRet = true;
                    strReason = "There was a conflicts (same version is available) but updated based on modified time";
                } else {
                    bRet = false;
                    strReason = "There was a conflicts (same version is available). Update was not done";
                }
            } else if (nLocalVer > nRemoteVer) {
                bRet = false;
                strReason = "Newer version is available on the instance";
            } else {
                bRet = true;
                strReason = OK;
            }

            if (bRet == true) {
                meta = m_localInstance.updateVersion(strKey, nRemoteVer, value, strTierName, strTag, remoteModifiedTime, true);

                if (meta == null) {
                    strReason += "- Failed to update the value";
                } else    //Local copy successfully updated
                {
                    //Change to original to avoid anything bad
                    meta.setLastModifiedTime(remoteModifiedTime);
                }
            }
        }

        if (bRet == false) {
            System.out.println("[debug] Failed In putfrompeerinstance: reason: " + strReason);
        }

        //Result
        JSONObject response = new JSONObject();
        response.put(TYPE, PUT);
        response.put(RESULT, bRet);
        response.put(VALUE, strReason);
        response.put(REASON, strReason);
        return response.toString();
    }

    @Override
    public String getClusterLock(String strLockReq) throws TException {
        //this function mainly will be used for distributing meta data if strong dataDistribution is required.
        //Extract req
        JSONObject lockReq = new JSONObject(strLockReq);
        String strKey = (String) lockReq.get(KEY);
        boolean bWrite = (boolean) lockReq.get(IS_WRITE);

        //For ret
        JSONObject response = new JSONObject();
        //DataDistributionUtil dataDistribution = m_localInstance.m_peerInstanceManager.getDataDistribution();
        String strResponse = "";

		/*if(dataDistribution.getDistributionType() == DataDistributionUtil.DATA_DISTRIBUTION_TYPE.WIERA_CLUSTER)
		{
			if(((WieraCluster)dataDistribution).checkLeader() == false)
			{
				strResponse = "I'm not the leader for the cluster:" + dataDistribution.getPolicyID();
				response.put(RESULT, false);
			}
			else
			{
				ReentrantReadWriteLock lock = dataDistribution.m_broadcastKeyLocker.getLock(strKey);

				if(bWrite == true)
				{
					lock.writeLock().lock();
					strResponse = "Write lock for key: " + strKey + " acquired";
					response.put(RESULT, true);
				}
				else
				{
					lock.readLock().lock();
					strResponse = "Read lock for key: " + strKey + " acquired";
					response.put(RESULT, true);
				}
			}
		}
		else
		{
			strResponse = "Cluster lock is only available in Cluster mode.";
			response.put(RESULT, false);
		}*/

        response.put(VALUE, strResponse);
        System.out.println(strResponse);

        return response.toString();
    }

    @Override
    public String releaseClusterLock(String strLockReq) throws org.apache.thrift.TException {
        //Extract req
        JSONObject lockReq = new JSONObject(strLockReq);
        String strKey = (String) lockReq.get(KEY);
        boolean bWrite = (boolean) lockReq.get(IS_WRITE);

        JSONObject response = new JSONObject();
        String strResponse = NOT_HANDLED;
		/*DataDistributionUtil dataDistribution = m_localInstance.m_peerInstanceManager.getDataDistribution();


		if(dataDistribution.getDistributionType() == DataDistributionUtil.DATA_DISTRIBUTION_TYPE.WIERA_CLUSTER)
		{
			if(((WieraCluster)dataDistribution).checkLeader() == false)
			{
				strResponse = "I'm not the leader for the cluster:" + dataDistribution.getPolicyID();
				response.put(RESULT, false);
			}
			else
			{
				ReentrantReadWriteLock lock = dataDistribution.m_broadcastKeyLocker.getLock(strKey);

				if(bWrite == true)
				{
					if(lock.isWriteLocked() == true)
					{
						lock.writeLock().unlock();
						strResponse = "Lock for key: " + strKey + " released";
						response.put(RESULT, true);
					}
					else
					{
						strResponse = "There is no write lock for the key: " + strKey;
						response.put(RESULT, false);
					}
				}
				else
				{
					lock.readLock().unlock();
					strResponse = "Lock for key: " + strKey + " released";
					response.put(RESULT, true);
				}
			}
		}
		else
		{
			strResponse = "Peer Local lock is only available in Cluster mode.";
			response.put(RESULT, false);
		}*/

        response.put(VALUE, strResponse);
        System.out.println(strResponse);

        return response.toString();
    }

    @Override
    public String setLeader(String strLeaderHostNameReq) throws org.apache.thrift.TException {
        JSONObject response = new JSONObject();
        String strResponse = NOT_HANDLED;
        boolean bRet = false;

        try {
            //This function will be called by current leader which will resign soon
            //but let's assume that their will be no chance for now.
            JSONObject obj = new JSONObject(strLeaderHostNameReq);
            String strLeaderHostName = (String) obj.get(LEADER_HOSTNAME);

			/*DataDistributionUtil dataDistribution = m_localInstance.m_peerInstanceManager.getDataDistribution();

			if(dataDistribution.getDistributionType() == DataDistributionUtil.DATA_DISTRIBUTION_TYPE.WIERA_CLUSTER)
			{
				((WieraCluster)dataDistribution).setLeaderName(strLeaderHostName);
				strResponse = "Leader name has been changed";
				bRet = true;
			}
			else
			{
				strResponse = "Peer Local lock is only available in Cluster mode.";
			}*/
        } catch (Exception e) {
            e.printStackTrace();
        }

        response.put(RESULT, bRet);
        response.put(VALUE, strResponse);
        return response.toString();
    }
}