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
    public static final int NO_SUCH_KEY = -3;
    public static final int NO_VERSIONING_SUPPORT = -2;

    MetaVerInfo m_latestMetaInfo;
    boolean m_pin = false;
    int m_nLatestVer = NO_SUCH_VERSION;
    boolean m_bSupportVersioning = false;
    String m_strHostname;
    HashMap<Integer, MetaVerInfo> m_versions;

    MetaObjectInfo(){
        //this("", "", "", 0, System.currentTimeMillis(), 0, 0, false);
    }

    // convenient
    MetaObjectInfo(String Key, String strHostName, String strTierName, TierInfo.TIER_TYPE initTierType,  long size, String strTag,  boolean bSupportVersioning){
        this(0 ,Key, strHostName, strTierName, initTierType, System.currentTimeMillis(), size, strTag, bSupportVersioning) ;

    }

    MetaObjectInfo(int nVer, String strKey, String strHostName, String strTierName, TierInfo.TIER_TYPE initTierType, long startTime, long size, String strTag,  boolean bSupportVersioning) {
        m_key = strKey;
        m_bSupportVersioning = bSupportVersioning;
        m_strHostname = strHostName;


        if(m_bSupportVersioning == true){
            m_nLatestVer = NO_VERSIONING_SUPPORT;
            m_latestMetaInfo = new MetaVerInfo(m_nLatestVer, strHostName,  startTime, size);
        }else{
            m_latestMetaInfo = new MetaVerInfo(nVer, strHostName,  startTime, size);
            m_nLatestVer = nVer;
            m_versions = new HashMap<>();
            m_versions.put(nVer, m_latestMetaInfo);
        }



        addLocale(m_nLatestVer, strHostName, strTierName, initTierType);
        tags = new HashSet<String>();
        tags.add(strTag);

        accessTime = new HashSet<Long>();
        accessTime.add(startTime);
    }


    /**
     * Nan
     * Insert new version by increasing the old version by 1
     */
    synchronized public void addNewVersion(String strHostName, String strTierName, TierInfo.TIER_TYPE initTierType, long startTime, long size) {
        if (m_bSupportVersioning == false) {
            //Version is not supported
            return;
        }

        m_nLatestVer++;
        MetaVerInfo obj = new  MetaVerInfo(m_nLatestVer, strHostName, startTime, size);
        m_versions.put(m_nLatestVer, obj);
        addLocale(m_nLatestVer, strHostName, strTierName, initTierType);
        m_latestMetaInfo = obj;
        accessTime.add(startTime);
    }
    /**
     * Nan:
     *  manually set a new specified version
     *  if the given version is older than the existed latest version, then this action is ignored.
     */
    synchronized public boolean updateVersion(int nVersion, String strHostName, String strTierName, TierInfo.TIER_TYPE initTierType, long modifiedTime, long size) {
        accessTime.add(modifiedTime);
        if (m_bSupportVersioning == false) {
            m_latestMetaInfo.setSize(size);
            m_latestMetaInfo.setLastModifedTime(modifiedTime);
            //Nan : ... future deal time issue, now ignore
            addLocale(m_nLatestVer, strHostName, strTierName, initTierType);
            return true;
        }else{
            MetaVerInfo verInfo;
            if (m_nLatestVer >= nVersion) {
                System.out.println("[Error] Provided an older version.");
                return false;
            }
            m_nLatestVer = nVersion;
            verInfo = new MetaVerInfo(nVersion, strHostName,  modifiedTime, size);
            m_versions.put(m_nLatestVer, verInfo);
            addLocale(m_nLatestVer, strHostName, strTierName, initTierType);

            return true;
        }
    }

    /*public int getLastestVersion() {
        return m_nLatestVer;
    }*/
    public int getLatestVersion(){
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
            return m_key + String.format("_ver_%d", nVer);
        } else {
            return m_key;
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


    //Nan
    public boolean addLocale(int nVer, Locale locale) {
        return addLocale(nVer, locale.getHostName(), locale.getTierName(), locale.getTierType());
    }
    //Nan
    public boolean addLocale(String strHostName, String strTierName, TierInfo.TIER_TYPE tierType) {
        return addLocale(m_nLatestVer, strHostName, strTierName, tierType);
    }
    /**
     * Nan:Base
     * First create this version's MetaVerInfo before call it.
     * */
    public boolean addLocale(int nVer, String strHostName, String strTierName, TierInfo.TIER_TYPE tierType) {
        MetaVerInfo ver;

        if (m_bSupportVersioning == true) {
            ver = m_versions.get(nVer);
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
    //Nan
    public synchronized long getSize() {
        return getSize(LATEST_VERSION);
    }
    //Nan
    public synchronized long getSize(long nVer) {
        MetaVerInfo info;
        if (m_bSupportVersioning == true) {
            info = m_versions.get(nVer);
        } else {
            info = m_latestMetaInfo;
        }
        if (info != null) {
            return info.getSize();
        }
        return -1;
    }

    //Nan
    public synchronized boolean removeLocale(long nVer, String strTiername){
        return removeLocale(nVer, m_strHostname, strTiername);
    }
    //Nan
    public synchronized boolean removeLocale(String strHostName, String strTierName) {
        return removeLocale(m_nLatestVer, strHostName, strTierName);
    }
    //Nan
    public synchronized boolean removeLocale(long nVer, Locale targetLocale) {
        return removeLocale(nVer, targetLocale.getHostName(), targetLocale.getTierName());
    }
    //Nan
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

    //Nan:
    public synchronized boolean hasLocale(String strTierName){
        return hasLocale(m_nLatestVer, strTierName);
    }
    //Nan:
    public synchronized boolean hasLocale(long lVer, String strTierName){
        return hasLocale(lVer,m_strHostname, strTierName);
    }
    //Nan:
    public synchronized boolean hasLocale(long lVer, String strHostName, String strTierName) {
        MetaVerInfo info;
        boolean rs = false;
        if (m_bSupportVersioning == true) {
            if (lVer < 0) {
               return rs;
            }else{
                info = m_versions.get(lVer);
            }
        } else {
            info = m_latestMetaInfo;
        }
        if (info != null) {
            rs = info.hasLocale(strHostName, strTierName);
        }
        return rs;
    }
    public synchronized String getFastTier(){
        return getFastTier(m_nLatestVer);
    }
    //Nan
    public synchronized String getFastTier(int nVer){
        MetaVerInfo verinfo;
        if(m_bSupportVersioning == true){
             verinfo = m_versions.get(nVer);
        }else{
            verinfo = m_latestMetaInfo;
        }
        if(verinfo == null){
            return null;
        }
        Locale loc = verinfo.getFastestLocale(true);
        return loc.getTierName();
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