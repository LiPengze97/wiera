package umn.dcsg.wieralocalserver.responses;

import com.sleepycat.persist.impl.Store;
import umn.dcsg.wieralocalserver.*;
import umn.dcsg.wieralocalserver.responses.peers.ForwardGetResponse;

import static umn.dcsg.wieralocalserver.Constants.*;

import java.util.Map;

/**
 * Created by ajay on 11/13/12.
 */
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

        //For TripS Case
        if (targetLocale.isLocalLocale()) {
            value = m_localInstance.get(strKey, targetLocale.getTierName());
        } else {
            //Forward request
            if(respondAtRuntimeWithClass(m_localInstance, ForwardGetResponse.class, responseParams) == false) {
                //If failed to write locally return false.
                value = null;
            } else {
                value = (byte[]) responseParams.get(VALUE);
            }
        }

        //Result
        if (value == null) {
            bRet = false;
            responseParams.put(REASON, "Failed to retrieve Key:" + strKey);
        } else {
            bRet = true;
            //To update end of response chain
            responseParams.put(SIZE, value.length);

            //Store local access
            if (targetLocale.isLocalLocale()) {
                addMetaToUpdate(m_localInstance.getMetadata(strKey), responseParams);
            }
        }

        responseParams.put(VALUE, value);
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

        if(responseParams.containsKey(TARGET_LOCALE) == false) {
            if (responseParams.containsKey(FROM) == true) {
                strLocaleID = (String) responseParams.get(FROM);
            } else if (m_initParams.containsKey(FROM) == true) {
                strLocaleID = (String) m_initParams.get(FROM);
            }

            if (strLocaleID != null) {
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
                    strTierName = Locale.defaultLocalLocale.getTierName();
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

    @Override
    public boolean doCheckResponseConditions(Map<String, Object> responseParams) {
        return true;
    }
}