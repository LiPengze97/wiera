package umn.dcsg.wieralocalserver;

import com.sleepycat.persist.model.Persistent;
import java.util.HashMap;
import java.util.Map;

import static umn.dcsg.wieralocalserver.TierInfo.TIER_TYPE.*;

/**
 * Created by Kwangsung on 10/22/2015.
 */

@Persistent
public class MetaVerInfo {
    //For each version
    long m_lVer = 0;
    long m_lAccessCnt = 0;
    long m_lStartTime = 0;
    long m_lLastAccessTime = 0;
    long m_lLastModifiedTime = 0;
    long m_lSize = 0;
    boolean m_bDirty = false;

    // Nan : add, but not useful
    String m_hostname = "";


    //String: HostName:TierName -> Only for hash value for eash searching
    //Locale: all information about Locale
    Map<String, Locale> m_storedTiers;

    MetaVerInfo(){

    }

    String getLocaleID(String strHostName, String strTierName) {
        return strHostName + ':' + strTierName;
    }

    public MetaVerInfo(long ver, String strHostName, long startTime, long size) {
        m_lVer = ver;
        m_lAccessCnt = 0;
        m_lStartTime = startTime;
        m_lLastAccessTime = startTime;
        m_lLastModifiedTime = startTime;
        m_lSize = size;
        m_hostname = strHostName;
        m_storedTiers = new HashMap<>();

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
            if (m_storedTiers.containsKey(getLocaleID(strHostName, strTierName))) {
                return false;
            } else {
                m_storedTiers.put(getLocaleID(strHostName, strTierName), new Locale(strHostName, strTierName, tierType));
                //System.out.printf("[Debug] I have added %s - total %d tiers now.", strTierName, m_storedTiers.size());
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }
    // Nan
    synchronized boolean removeLocale(String strHostName, String strTierName) {
        String strLocaleID = getLocaleID(strHostName, strTierName);
        try {
            if (m_storedTiers.containsKey(strLocaleID)) {
                m_storedTiers.remove(strLocaleID);
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
         return m_storedTiers.containsKey(getLocaleID(strHostName, strTierName));
    }

    public Locale getLocale(String strHostName, String strTierName){
        if(hasLocale(strHostName, strTierName) == true) {
            return m_storedTiers.get(getLocaleID(strHostName, strTierName));
        }

        return null;
    }

    public TierInfo.TIER_TYPE getTierType(String strHostName, String strTierName) {
        if (hasLocale(strHostName, strTierName) == true) {
            return m_storedTiers.get(getLocaleID(strHostName, strTierName)).getTierType();
        }

        return null;
    }


    //Get Local Fastest one.
    public Locale getFastestLocale(boolean bOnlyLocal) {
        Locale potentialLocale = null;

        //Find local (fastest) tier first and remote
        for (Locale locale : m_storedTiers.values()) {
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
        return m_storedTiers;
    }

}