package umn.dcsg.wieralocalserver;

import java.util.*;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

/**
 * Created with IntelliJ IDEA. User: ajay Date: 29/03/13 Time: 12:42 PM To
 * change this template use File | Settings | File Templates.
 * 
 * This tracks a single object created and moved around LocalInstance.
 */
//Store Key and data location with version. -- ks

@Entity
public class MetaObjectInfo {
    @PrimaryKey
    String m_key = null;

    @SecondaryKey(relate = Relationship.MANY_TO_MANY)
    Set<String> tags = null;

    @SecondaryKey(relate = Relationship.MANY_TO_MANY)
    Set<Long> accessTime = null;

    public static final int LATEST_VERSION = 99999999;
    public static final int NO_SUCH_VERSION = -1;
    public static final int NO_VERSIONING_SUPPORT = -2;

    MetaVerInfo m_latestMetaInfo;
    boolean m_pin = false;
    int m_nLatestVer = NO_SUCH_VERSION;
    boolean m_bSupportVersioning = false;
    HashMap<Integer, MetaVerInfo> m_versions;

    public MetaObjectInfo() {
        this("", "", "", TierInfo.TIER_TYPE.UNKNOWN, System.currentTimeMillis(), 0, "", false, false);
    }

    //Local locale meta information
    MetaObjectInfo(String key, String strTierName, TierInfo.TIER_TYPE initTierType, long size, String strTag, boolean bSupportVersioning) {
        this(key, LocalServer.getHostName(), strTierName, initTierType, System.currentTimeMillis(), size, strTag, false, bSupportVersioning);
    }

    MetaObjectInfo(String key, String strHostName, String strTierName, TierInfo.TIER_TYPE initTierType, long size, String strTag, boolean bSupportVersioning) {
        this(key, strHostName, strTierName, initTierType, System.currentTimeMillis(), size, strTag, false, bSupportVersioning);
    }

    MetaObjectInfo(String Key, String strHostName, String strTierName, TierInfo.TIER_TYPE initTierType, long startTime, long size, String strTag, boolean Pin, boolean bSupportVersioning) {
        //For each key.
        m_key = Key;

        //Object Info
        m_latestMetaInfo = new MetaVerInfo(0, strHostName, strTierName, initTierType, System.currentTimeMillis(), size);
        m_latestMetaInfo.m_lAccessCnt = 0;
        m_latestMetaInfo.m_lStartTime = startTime;
        m_latestMetaInfo.m_lLastAccessTime = startTime;
        m_latestMetaInfo.m_lLastModifiedTime = 0;
        m_latestMetaInfo.m_bDirty = false;
        m_latestMetaInfo.m_bPin = Pin;
        m_latestMetaInfo.m_lSize = size;

        //Todo Need to be improved
        m_bSupportVersioning = bSupportVersioning;

        if (m_bSupportVersioning == true) {
            //Init version
            m_nLatestVer = NO_SUCH_VERSION;
            m_versions = new HashMap<>();
        } else {
            m_nLatestVer = NO_VERSIONING_SUPPORT;
        }

        tags = new HashSet<String>();
        tags.add(strTag);

        accessTime = new HashSet<Long>();
        accessTime.add(startTime);
    }

    //Insert new version. always increased.
    synchronized public void addNewVersion(String strHostName, String strTierName, TierInfo.TIER_TYPE initTierType, long startTime, long size) {
        if (m_bSupportVersioning == false) {
            //Version is not supported
            return;
        }

        m_nLatestVer++;
        MetaVerInfo obj = new MetaVerInfo(m_nLatestVer, strHostName, strTierName, initTierType, startTime, size);
        m_versions.put(m_nLatestVer, obj);
        m_latestMetaInfo.m_lLastModifiedTime = startTime;
        m_latestMetaInfo.m_lSize = size;
    }

    //Update version if it is updated by other peers, or newly added,
    synchronized public void updateVersion(int nVersion, String strHostName, String strTierName, TierInfo.TIER_TYPE initTierType, long modifiedTime, long size) {
        if (m_bSupportVersioning == false) {
            //Version is not supported
            return;
        }

        MetaVerInfo verInfo = m_versions.get(nVersion);

        if (verInfo == null) {
            //Need to be synced?
            if (m_nLatestVer < nVersion) {
                m_nLatestVer = nVersion;
            }

            verInfo = new MetaVerInfo(nVersion, strHostName, strTierName, initTierType, modifiedTime, size);
            m_versions.put(m_nLatestVer, verInfo);
        } else {
            verInfo.setLastModifedTime(modifiedTime);
        }
    }

    public int getLastestVersion() {
        return m_nLatestVer;
    }

    public String getKey() {
        return m_key;
    }

    public String getVersionedKey() {
        return getVersionedKey(m_nLatestVer);
    }

    public String getVersionedKey(int nVer) {
        if (m_bSupportVersioning == true) {
            //Thread.dumpStack();
            return m_key + String.format("_ver_%d", nVer);
        } else {
            return m_key;
        }
    }

    public void fillVersionGap(long version) {
        long nGap = version - m_nLatestVer;

        for (int i = 0; i < nGap; i++) {
            m_nLatestVer++;
            m_versions.put(m_nLatestVer, null);
        }
    }

    public HashMap<Integer, MetaVerInfo> getVersionList() {
        if (m_bSupportVersioning == true) {
            return m_versions;
        } else {
            HashMap<Integer, MetaVerInfo> ver = new HashMap<Integer, MetaVerInfo>();
            ver.put(0, m_latestMetaInfo);
            return ver;
        }
    }

    /*public boolean addLocalLocale(long lVer, String strTierName, TierInfo.TIER_TYPE tierType) {
        return addLocale(lVer, LocalServer.getHostName(), strTierName, tierType);
    }*/

    /*public boolean addLocalLocale(String strTierName, TierInfo.TIER_TYPE tierType) {
        return addLocale(m_nLatestVer, LocalServer.getHostName(), strTierName, tierType);
    }*/

    public boolean addLocale(int nVer, Locale locale) {
        return addLocale(nVer, locale.getHostName(), locale.getTierName(), locale.getTierType());
    }

    public boolean addLocale(String strHostName, String strTierName, TierInfo.TIER_TYPE tierType) {
        return addLocale(m_nLatestVer, strHostName, strTierName, tierType);
    }

    //Only when called for internal usage like move or replicate
    //Tier Name is unique.
    public boolean addLocale(int nVer, String strHostName, String strTierName, TierInfo.TIER_TYPE tierType) {
        MetaVerInfo ver;

        if (m_bSupportVersioning == true) {
            ver = m_versions.get(nVer); //findByVer(nVer);
        } else {
            ver = m_latestMetaInfo;
        }

        if (ver != null) {
            return ver.addLocale(strHostName, strTierName, tierType);
        } else {
            return false;
        }
    }

    public synchronized boolean setSize(long lSize) {
        return setSize(LATEST_VERSION, lSize);
    }

    public synchronized boolean setSize(long nVer, long lSize) {
        MetaVerInfo info;

        if (m_bSupportVersioning == true) {
            info = m_versions.get(nVer); //findByVer(nVer);
        } else {
            info = m_latestMetaInfo;
        }

        if (info != null) {
            info.setSize(lSize);
            return true;
        }

        return false;
    }

    public synchronized long getSize() {
        return getSize(LATEST_VERSION);
    }

    public synchronized long getSize(long nVer) {
        MetaVerInfo info;

        if (m_bSupportVersioning == true) {
            info = m_versions.get(nVer); //findByVer(nVer);
        } else {
            info = m_latestMetaInfo;
        }

        if (info != null) {
            return info.getSize();
        }

        return NO_SUCH_VERSION;
    }

    public synchronized boolean removeLocale(String strHostName, String strTierName) {
        return removeLocale(m_nLatestVer, strHostName, strTierName);
    }

    public synchronized boolean removeLocale(long nVer, Locale targetLocale) {
        return removeLocale(nVer, targetLocale.getHostName(), targetLocale.getTierName());
    }

    public synchronized boolean removeLocale(long nVer, String strHostName, String strTierName) {
        MetaVerInfo info;

        if (m_bSupportVersioning == true) {
            info = m_versions.get(nVer); //findByVer(nVer);
        } else {
            info = m_latestMetaInfo;
        }

        if (info != null) {
            return info.removeLocale(strHostName, strTierName);
        } else {
            return false;
        }
    }

    // set last access time to current time
    public synchronized void setLAT() {
        setLAT(System.currentTimeMillis());
    }

    // set the last access time
    public synchronized void setLAT(long lLAT) {
        //Last access time has been updated
        //Update db also
        accessTime.remove(m_latestMetaInfo.m_lLastAccessTime);
        m_latestMetaInfo.m_lLastAccessTime = lLAT;
        accessTime.add(m_latestMetaInfo.m_lLastAccessTime);
    }

    // set the modifeid time
    public synchronized void setLastModifiedTime(long time) {
        m_latestMetaInfo.m_lLastModifiedTime = time;
    }

    public synchronized long getLastModifiedTime() {
        return m_latestMetaInfo.m_lLastModifiedTime;
    }

    // set the access count
    public synchronized void set_accessCount(int count) {
        m_latestMetaInfo.m_lAccessCnt = count;
    }

    public synchronized long getLAT() {
        return m_latestMetaInfo.m_lLastAccessTime;
    }

    // increment the access count
    public synchronized void countInc() {
        m_latestMetaInfo.m_lAccessCnt++;
    }

    public synchronized long get_accessCount() {
        return m_latestMetaInfo.m_lAccessCnt;
    }

    public synchronized void setPin() {
        m_pin = true;
    }

    public synchronized void unsetPin() {
        m_pin = false;
    }

    public synchronized boolean isPinned() {
        return m_pin;
    }

    public synchronized Locale getLocale(boolean bOnlyLocal) {
        return getLocale(m_nLatestVer, bOnlyLocal);
    }

    public synchronized Locale getLocale(long lVer, boolean bOnlyLocal) {
        MetaVerInfo info;

        if (m_bSupportVersioning == true) {
            if (lVer < 0) {
                lVer = m_nLatestVer;
            }

            info = m_versions.get(lVer);//findByVer(lVer);
        } else {
            info = m_latestMetaInfo;
        }

        return info.getFastestLocale(bOnlyLocal);
    }

    public synchronized Locale getLocale(long lVer, String strLocaleID){
        String[] tokens = strLocaleID.split(":");
        return getLocale(lVer, tokens[0], tokens[1]);
    }

    public synchronized Locale getLocale(long lVer, String strHostName, String strTierName){
        MetaVerInfo info;
        if(m_bSupportVersioning == true){
            info = m_versions.get(lVer);
        }else{
            info = m_latestMetaInfo;
        }
        if(info == null){
            return null;
        }
        return info.getLocale(strHostName, strTierName);
    }

    public synchronized boolean hasLocale(String strLocaleID) {
        String[] tokens = strLocaleID.split(":");
        return hasLocale(m_nLatestVer, tokens[0], tokens[1]);
    }

    public synchronized boolean hasLocale(long lVer, String strLocaleID) {
        String[] tokens = strLocaleID.split(":");
        return hasLocale(lVer, tokens[0], tokens[1]);
    }

    public synchronized boolean hasLocale(long lVer, String strHostName, String strTierName) {
        MetaVerInfo info;

        if (strHostName == null) {
            strHostName = LocalServer.getHostName();
        }

        if (m_bSupportVersioning == true) {
            if (lVer < 0) {
                lVer = m_nLatestVer;
            }

            info = m_versions.get(lVer);//findByVer(nVer);
        } else {
            info = m_latestMetaInfo;
        }

        if (info != null) {
            return info.hasLocale(strHostName, strTierName);
        }

        return false;
    }

    public synchronized Map<String, Locale> getLocaleList() {
        return getLocaleList(m_nLatestVer);
    }

    public synchronized Map<String, Locale> getLocaleList(long lVer) {
        MetaVerInfo info;

        if (m_bSupportVersioning == true) {
            if (lVer < 0) {
                lVer = m_nLatestVer;
            }

            info = m_versions.get(lVer);//findByVer(lVer);
        } else {
            info = m_latestMetaInfo;
        }

        return info.getLocaleList();
    }

    public synchronized void setDirty() {
        m_latestMetaInfo.m_bDirty = true;
    }

    public synchronized void clearDirty() {
        m_latestMetaInfo.m_bDirty = false;
    }

    public synchronized boolean isDirty() {
        return m_latestMetaInfo.m_bDirty;
    }
}