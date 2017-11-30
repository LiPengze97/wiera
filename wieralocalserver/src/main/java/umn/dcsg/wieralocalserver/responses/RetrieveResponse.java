package umn.dcsg.wieralocalserver.responses;

import umn.dcsg.wieralocalserver.*;

import static umn.dcsg.wieralocalserver.Constants.*;

import java.util.Map;

/**
 * Nan:
 *  For now, only support retrieve from local instance, no remote instance is considered.
 *  If no "from" is provided, the default target_locale is local instance's default tier. [The tier set as default in policy].
 *  If no version is provide, return the latest version
 *  initParam
 *  in-params:Key[:From:Version]
 *
 *  out-param:Key:Version:Value:Target_Locale:[:From]
 *
 *
 * Created by ajay on 11/13/12.
 */
public class RetrieveResponse extends Response {
    public RetrieveResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
    }

    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(KEY);
        m_lstRequiredParams.add(TARGET_LOCALE);
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        boolean bRet;
        byte[] value;

        String strKey = (String) responseParams.get(KEY);
        Locale targetLocale = (Locale) responseParams.get(TARGET_LOCALE);
        int nVer = MetaObjectInfo.NO_SUCH_VERSION;
        if(m_localInstance.isVersionSupported()){
            if (responseParams.containsKey(VERSION) == true){
                nVer = (int)(double) responseParams.get(VERSION);
            } else if (m_initParams.containsKey(VERSION) == true) {
                nVer= (int)(double) m_initParams.get(VERSION);
            }else{
                nVer = m_localInstance.getLatestVersion(strKey);
                if(nVer == MetaObjectInfo.NO_SUCH_KEY){
                    responseParams.put(RESULT, false);
                    responseParams.put(REASON, "Not found this key-value pair");
                    return false;
                }
            }
        }

        //For TripS Case
        if (targetLocale.isLocalLocale()) {
            value = m_localInstance.get(strKey, nVer, targetLocale.getTierName());
        } else {
            //Forward request
            value = null;
        }

        //Result
        if (value == null) {
            bRet = false;
        } else {
            bRet = true;
            responseParams.put(VALUE, value);
            responseParams.put(VERSION,nVer);
            //To update end of response chain
            addObjsToUpdate(m_localInstance.getMetadata(strKey), responseParams);
        }

        responseParams.put(RESULT, bRet);
        return bRet;
    }

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {
        Locale targetLocale;
        String strLocaleID = null;
        String strHostName = null;
        String strTierName = null;
        TierInfo.TIER_TYPE tierType;

        if (responseParams.containsKey(FROM) == true) {
            strLocaleID = (String) responseParams.get(FROM);
        } else if (m_initParams.containsKey(FROM) == true) {
            strLocaleID = (String) m_initParams.get(FROM);
        }

        if(strLocaleID != null) {
            strHostName = strLocaleID.split(":")[0];
            strTierName = strLocaleID.split(":")[1];
        }

        //Set default to local hostname
        if (strHostName == null || strHostName.isEmpty() == true) {
            strHostName = LocalServer.getHostName();
        }

        //Set default to local tiername
        if (strTierName == null) {
            //If local, set to default
            if (strHostName.equals(LocalServer.getHostName())) {
                strTierName = m_localInstance.m_strDefaultTierName;
            } else {
                strTierName = "";
            }
        }

        if (strHostName.equals(LocalServer.getHostName()) == true) {
            tierType = m_localInstance.getLocalStorageTierType(strTierName);
        } else {
            tierType = TierInfo.TIER_TYPE.REMOTE_TIER;
        }

        targetLocale = new Locale(strHostName, strTierName, tierType);
        responseParams.put(TARGET_LOCALE, targetLocale);



    }
}