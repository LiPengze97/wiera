package umn.dcsg.wieralocalserver;

import org.json.JSONObject;
import umn.dcsg.wieralocalserver.storageinterfaces.*;

import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by Kwangsung on 10/8/2015.
 */
public class Tier {
    //This will be shared with other instances
    //public static int MAX_TIERS = 4; 	//0: Memory	1:Disk (hdd or ssd) 2: Storage Service (S3) 3. Archival (gracier)
    TierInfo m_tierInfo;

    int m_nConnCnt;
    int m_nCurConnectionPos;

    ArrayList m_tierConnection;
    ReentrantLock m_lock;

    Tier(JSONObject storageInfo, int nMaxConn) {
        //System.out.println(obj.toString());
        m_tierInfo = new TierInfo(storageInfo);

        m_nConnCnt = nMaxConn;
        m_nCurConnectionPos = 0;
        m_tierConnection = new ArrayList();
        m_lock = new ReentrantLock();

        switch (m_tierInfo.getTierType()) {
            case MEMORY:
                String strServerLIst = (String) storageInfo.get(TIER_LOCATION);

                if (strServerLIst != null) {
                    addMemoryTier(strServerLIst);
                }
                break;
            case SSD:
            case HDD:
                String diskFolder = (String) storageInfo.get(TIER_LOCATION);

                if (diskFolder != null) {
                    addDiskTier(diskFolder);
                }
                break;
            case CLOUD_STORAGE:
                TierInfo.STORAGE_PROVIDER type = TierInfo.STORAGE_PROVIDER.values()[(int) storageInfo.get(STORAGE_PROVIDER)];
                String strParam1 = (String) storageInfo.get(STORAGE_ARG1);
                String strParam2 = (String) storageInfo.get(STORAGE_ARG2);
                String strKey = (String) storageInfo.get(STORAGE_ID1);
                String strSecret = (String) storageInfo.get(STORAGE_ID2);

                if (strParam1 != null && strParam2 != null) {
                    //System.out.println("I'mhere in Cloud Storage TierName = " + m_strLocaleID);
                    addStorageTier(type, strKey, strSecret, strParam1, strParam2);
                }
                break;
            case CLOUD_ARCHIVAL:
                break;
            case WIERA_INSTANCE:
                String strWieraID = (String) storageInfo.get(WIERA_ID);
                String strWieraIP = (String) storageInfo.get(WIERA_IPADDRESS);
                int nWieraPort = (int) storageInfo.get(WIERA_PORT);

                if (strWieraIP != null) {
                    addWieraTier(strWieraIP, nWieraPort, strWieraID);
                }
                break;
        }
    }

    public String getTierName() {
        return m_tierInfo.getTierName();
    }

    public TierInfo.TIER_TYPE getTierType() {
        return m_tierInfo.getTierType();
    }

    public boolean addMemoryTier(String strServerList) {
        //MemcachedInterface mcInterface;
        RedisInterface redisInterface;

        boolean bRet = true;

        for (int i = 0; i < m_nConnCnt; i++) {
            redisInterface = new RedisInterface(strServerList);
            if (!m_tierConnection.add(redisInterface)) {
                bRet = false;
            }
		/*	strServerList += ":11211";
			mcInterface = new MemcachedInterface(strServerList);
			m_tierConnection.add(mcInterface);*/
        }

        return bRet;
    }

    public boolean addDiskTier(String strLocalPath) {
        LocalDiskInterface localDiskInterface;

        for (int i = 0; i < m_nConnCnt; i++) {
            //Using direct IO to avoid simulated delay
            localDiskInterface = new LocalDiskInterface(strLocalPath, false);

            //Seems not work
            //localDiskInterface = new LocalDiskInterface(strLocalPath, 4096);
            return m_tierConnection.add(localDiskInterface);
        }

        return true;
    }

    //For now not used connection cnt for simplicity
    public boolean addStorageTier(TierInfo.STORAGE_PROVIDER type, String strID1, String strID2, String strArg1, String strArg2) {
        switch (type) {
            case S3:         //Amazone S3
                S3Interface s3;
                for (int i = 0; i < m_nConnCnt; i++) {
                    s3 = new S3Interface(strID1, strID2, strArg1, strArg2);
                    return m_tierConnection.add(s3);
                }
                break;
            case AS:        //Azure Storage
                AzureStorageInterface azureStorage;
                for (int i = 0; i < m_nConnCnt; i++) {
                    azureStorage = new AzureStorageInterface(strID1, strID2, strArg1);
                    return m_tierConnection.add(azureStorage);
                }
                break;
            case GS:        //Google Storage
                GoogleCloudStorageInterface gcs;
                for (int i = 0; i < m_nConnCnt; i++) {
                    try {
                        gcs = new GoogleCloudStorageInterface(strID1, strID2, strArg1);
                    } catch (Exception e) {
                        e.printStackTrace();
                        return false;
                    }

                    return m_tierConnection.add(gcs);
                }
                break;
        }
        return false;
    }

    public boolean addArchivalTier(String strID1, String strID2) {
        return false;
    }

    public boolean addWieraTier(String strWieraIP, int nWieraPort, String strWieraID) {
        WieraInstanceTierInterface wieraTierInterface = new WieraInstanceTierInterface(strWieraIP, nWieraPort, strWieraID);
        return m_tierConnection.add(wieraTierInterface);
    }

    public StorageInterface getInterface() {
        StorageInterface iface = null;

        try {
            m_lock.lock();
            iface = (StorageInterface) m_tierConnection.get(m_nCurConnectionPos);
            m_nCurConnectionPos = (m_nCurConnectionPos + 1) % m_nConnCnt;    //Circular.
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            m_lock.unlock();
        }

        return iface;
    }

    public double getUsedRate() {
        return m_tierInfo.getUsedRate();
    }

    public double getUsedRateInPercent() {
        return m_tierInfo.getUsedRate() * 100;
    }

    public boolean checkFreeSpace(long lRequiredSize) {
        return getFreeSpace() >= lRequiredSize;
    }

    public long getFreeSpace() {
        return m_tierInfo.getFreeSpace();
    }

    public void useSpace(long lSize) {
        m_tierInfo.useSpace(lSize);
    }

    public void freeSpace(long lSize) {
        m_tierInfo.freeSpace(lSize);
    }

    public long growSpaceByPercent(int nByPercent){
        return m_tierInfo.growTierByPercent(nByPercent);
    }

    public long growSpaceBySize(int nBySize){
        return m_tierInfo.growTierByPercent(nBySize);
    }

    public long getExpectedLatency() {
        return m_tierInfo.getExpectedLatency();
    }
}