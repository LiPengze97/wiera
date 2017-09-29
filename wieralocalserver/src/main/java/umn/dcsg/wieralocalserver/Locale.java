package umn.dcsg.wieralocalserver;

import com.sleepycat.persist.model.Persistent;

import java.util.LinkedList;
import java.util.List;

import static umn.dcsg.wieralocalserver.TierInfo.TIER_TYPE.REMOTE_TIER;

/**
 * Created by with IntelliJ IDEA.
 * User: Kwangsung
 * Date: 7/26/2017 11:05 AM
 * Notion of Locale comes from TripS Data can be stored on multiple locations
 */
@Persistent
public class Locale {
    private String m_strHostname;
    private String m_strTierName;
    private TierInfo.TIER_TYPE m_tierType;

    Locale() {
        this("", "", TierInfo.TIER_TYPE.UNKNOWN);
    }

    public Locale(String strHostName, String strTierName, TierInfo.TIER_TYPE tierType) {
        m_strHostname = strHostName;
        m_strTierName = strTierName;
        m_tierType = tierType;

        if(m_strHostname.length() == 0) {
            m_strHostname = LocalServer.getHostName();
        }
    }

    public String getTierName() {
        return m_strTierName;
    }

    public String getHostName() {
        return m_strHostname;
    }

    public TierInfo.TIER_TYPE getTierType() {
        return m_tierType;
    }

    public String getLocaleID() {
        return m_strHostname + ':' + m_strTierName;
    }

    public boolean isLocalLocale() {
        return m_tierType != REMOTE_TIER;
    }

    public static List<Locale> getLocalesWithoutTierName(List<String> lstHostname) {
        List<Locale> lstLocale = new LinkedList<>();

        for (String strHostname : lstHostname) {
            lstLocale.add(new Locale(strHostname, "", REMOTE_TIER));
        }

        return lstLocale;
    }

    public static String getLocaleID(String strHostName, String strTierName) {
        return strHostName + ':' + strTierName;
    }

    @Override
    public boolean equals(Object obj) {
        Locale targetLocale = (Locale)obj;
        return getLocaleID().equals(targetLocale.getLocaleID()) == true;
    }

    @Override
    public int hashCode() {
        return getLocaleID().hashCode();
    }
}