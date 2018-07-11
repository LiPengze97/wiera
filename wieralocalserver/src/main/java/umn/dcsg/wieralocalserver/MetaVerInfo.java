package umn.dcsg.wieralocalserver;

import com.sleepycat.persist.model.Persistent;
import java.util.HashMap;
import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.*;
import static umn.dcsg.wieralocalserver.TierInfo.TIER_TYPE.*;

/**
 * Created by Kwangsung on 10/22/2015.
 */

@Persistent
public class MetaVerInfo {
    //For each version
    int m_nVer = 0;
    long m_lAccessCnt = 0;
    long m_lCreatedTime = 0;
    long m_lLastAccessTime = 0;
    long m_lLastModifiedTime = 0;
    long m_lSize = 0;
    boolean m_bDirty = false;
    boolean m_bPin = false;

    //String: HostName:TierName -> Only for hash value for eash searching
    //Locale: all information about Locale
    Map<String, Locale> m_LocaleList;

    MetaVerInfo() {
        this(0, System.currentTimeMillis(), 0);
    }

    String getLocaleID(String strHostName, String strTierName) {
        return strHostName + ':' + strTierName;
    }

    MetaVerInfo(int nVer, long lCreatedTime, long lSize) {
        m_nVer = nVer;
        m_lAccessCnt = 0;
        m_lCreatedTime = lCreatedTime;
        m_lLastAccessTime = lCreatedTime;
        m_lLastModifiedTime = lCreatedTime;
        m_lSize = lSize;

        m_LocaleList = new HashMap<>();
    }

    void setLastModifedTime(long lastmodifiedTime) {
        m_lLastModifiedTime = lastmodifiedTime;
    }

    long getLastModifiedTime() {
        return m_lLastModifiedTime;
    }

    void setSize(long lSize) {
        m_lSize = lSize;
    }

    long getSize() {
        return m_lSize;
    }

    synchronized boolean addLocale(String strHostName, String strTierName, TierInfo.TIER_TYPE tierType) {
        try {
            //Ignore same tiername. Tiername should be unique.
            if (m_LocaleList.containsKey(getLocaleID(strHostName, strTierName))) {
                return false;
            } else {
                m_LocaleList.put(getLocaleID(strHostName, strTierName), new Locale(strHostName, strTierName, tierType));
                //System.out.printf("[Debug] I have added %s - total %d tiers now.", strTierName, m_LocaleList.size());
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    synchronized boolean removeLocale(String strHostName, String strTierName) {
        String strLocaleID = getLocaleID(strHostName, strTierName);
        try {
            if (m_LocaleList.containsKey(strLocaleID)) {
                m_LocaleList.remove(strLocaleID);
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    public boolean hasLocale(String strHostName, String strTierName) {
/*
        System.out.println("[debug] check if exist : " + strHostName + ":" + strTierName);
        System.out.println("[debug] -------Available Tiers------");

        for (String strTier: m_LocaleList.keySet()){
            System.out.println("- " + strTier);
        }
*/
        return m_LocaleList.containsKey(getLocaleID(strHostName, strTierName));
    }

    public Locale getLocale(String strHostName, String strTierName){
        if(hasLocale(strHostName, strTierName) == true) {
            return m_LocaleList.get(getLocaleID(strHostName, strTierName));
        }

        return null;
    }

    TierInfo.TIER_TYPE getTierType(String strHostName, String strTierName) {
        if (hasLocale(strHostName, strTierName) == true) {
            return m_LocaleList.get(getLocaleID(strHostName, strTierName)).getTierType();
        }

        return null;
    }

    //Get Local Fastest one.
    public Locale getFastestLocale(boolean bOnlyLocal) {
        Locale potentialLocale = null;

        //Find local (fastest) tier first and remote
        for (Locale locale : m_LocaleList.values()) {
            if (locale.isLocalLocale() == true) {
                switch (locale.getTierType()) {
                    case MEMORY:
                        return locale;
                    case SSD:
                        if (potentialLocale == null || SSD.getTierType() < potentialLocale.getTierType().getTierType()) {
                            potentialLocale = locale;
                        }
                    case HDD:
                        if (potentialLocale == null || HDD.getTierType() < potentialLocale.getTierType().getTierType()) {
                            potentialLocale = locale;
                        }
                        break;
                    case CLOUD_STORAGE: //Assume that it is local dc cloud storage //Find local first
                        if ((potentialLocale == null) || CLOUD_STORAGE.getTierType() < potentialLocale.getTierType().getTierType()) {
                            potentialLocale = locale;
                        }
                        break;
                    case REMOTE_TIER:
                        if (bOnlyLocal == false && potentialLocale == null) {
                            potentialLocale = locale;
                        }
                        break;
                    default:
                        break;
                }
            }
        }

        return potentialLocale;
    }

    public Map<String, Locale> getLocaleList() {
        return m_LocaleList;
    }

    public int getVersion() {
        return m_nVer;
    }

    public Map<String, Object> getMetaInfo() {
        Map <String, Object> meta = new HashMap<String, Object> ();
        meta.put(VERSION, m_nVer);
        meta.put(ACCESS_CNT, m_lAccessCnt);
        meta.put(CREATED_TIME, m_lCreatedTime);
        meta.put(LAST_ACCESSED_TIME, m_lLastAccessTime);
        meta.put(LAST_MODIFIED_TIME, m_lLastModifiedTime);
        meta.put(SIZE, m_lSize);
        meta.put(DIRTY, m_bDirty);
        meta.put(PIN, m_bPin);
        meta.put(LOCALE_LIST, m_LocaleList);

        return meta;
    }
}