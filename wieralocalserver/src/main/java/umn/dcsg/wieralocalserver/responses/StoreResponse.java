package umn.dcsg.wieralocalserver.responses;

import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.LocalServer;
import umn.dcsg.wieralocalserver.Locale;
import umn.dcsg.wieralocalserver.MetaObjectInfo;
import umn.dcsg.wieralocalserver.info.OperationLatency;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import static umn.dcsg.wieralocalserver.Constants.*;
import static umn.dcsg.wieralocalserver.MetaObjectInfo.NO_SUCH_VERSION;

/**
 * Nan:
 * Data source preference order: params(dynamic) > initParam (static) > default
 * Default version number is the existed latest version plus one if versioning is enabled.
 * Default version number is NO_SUCH_VERSION is  versioning is disabled.
 *
 *  initParam
 *  in-Param:Key:Value:To[:Version]
 *
 *  Out-param:Key:Value:To:Target_locale:Version:last_modified_time:reason:result[:tag]
 *
 *
 *
 * Created by ajay on 11/13/12.
 */
public class StoreResponse extends Response {
    public StoreResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
    }

    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(KEY);
        m_lstRequiredParams.add(VALUE);
        m_lstRequiredParams.add(TARGET_LOCALE);
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        boolean bRet = false;
        String strReason = NOT_HANDLED;
        int nVer = NO_SUCH_VERSION;
        String strTag = "";
        long lLastModifiedTime = 0;
        Locale targetLocale = null;
        MetaObjectInfo obj;

        try {
            //necessary
            String strKey = (String) responseParams.get(KEY);
            byte[] value = (byte[]) responseParams.get(VALUE);
            targetLocale = (Locale) responseParams.get(TARGET_LOCALE);
            //optional
            if (responseParams.containsKey(TAG)) {
                strTag = (String) responseParams.get(TAG);
            }

            if(m_localInstance.isVersionSupported()) {
                if (responseParams.containsKey(VERSION)) {
                    nVer = (int) responseParams.get(VERSION);
                } else if (m_initParams.containsKey(VERSION)) {
                    nVer = (int) m_initParams.get(VERSION);
                } else {
                    nVer = m_localInstance.getLatestVersion(strKey);
                    if (nVer == MetaObjectInfo.NO_SUCH_KEY) {
                        nVer = 0;
                    }else{
                        nVer += 1;
                    }
                }
            }

            if (targetLocale.isLocalLocale() == true) {
                //Put locally
                obj = m_localInstance.put(strKey, nVer, value, targetLocale.getTierName());

                if (obj != null) {
                    strReason = OK;
                    bRet = true;
                    nVer = obj.getLatestVersion();
                    lLastModifiedTime = obj.getLastModifiedTime();

                    //If operation latency is set then change tiername to real tier used for runtime tier changed
                    if (responseParams.containsKey(OPERATION_LATENCY) == true) {
                        OperationLatency operationLatency = (OperationLatency) responseParams.get(OPERATION_LATENCY);
                        operationLatency.updateTierName(targetLocale.getTierName());
                    }

                    // Nan: Comment them out
                    // Which keys in which locale and version?
                    /* Map<Locale, Map<MetaObjectInfo, Vector<Integer>>> keyList = new HashMap<>();

                    Map<MetaObjectInfo, Vector<Integer>> keys = new HashMap<>();
                    Vector<Integer> vers = new Vector<>();
                    vers.add(nVer);

                    keys.put(obj, vers);
                    keyList.put((Locale) responseParams.get(TARGET_LOCALE), keys);

                    //Fill-out params for the next response if exists
                    responseParams.put(KEY_LIST, keyList);*/

                    responseParams.put(VERSION, nVer);
                    responseParams.put(LAST_MODIFIED_TIME, lLastModifiedTime);
                    if(strTag != null && strTag != ""){
                        responseParams.put(TAG, strTag);
                    }
                    responseParams.put(TIER_NAME, targetLocale.getTierName());
                    // Nan: why need the following function?
                    addObjsToUpdate(obj, responseParams);
                } else {
                    strReason = "Failed to put data into Tier: " + targetLocale.getTierName();
                }
            } else {
                // forward put operation
                // Meta data will be updated by broadcasting later with this forward
                // What if peers also don't have this key. For example, just after the wiera is set up.
                bRet = Response.respondAtRuntimeWithClass(m_localInstance, ForwardGetResponse.class, responseParams);

                if (bRet == false) {
                    strReason = "Failed to forward: " + responseParams.get(REASON);
                }
            }
        } catch (Exception e) {
            bRet = false;
            strReason = e.getMessage();
            e.printStackTrace();
        }

        //Result
        responseParams.put(RESULT, bRet);
        responseParams.put(REASON, strReason);
        return bRet;
    }

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {
        Locale targetLocale;
        String strLocaleID;

        if (responseParams.containsKey(TO) == true) {
            strLocaleID = (String) responseParams.get(TO);
        } else if (m_initParams.containsKey(TO) == true) {
            strLocaleID = (String) m_initParams.get(TO);
        } else {
            strLocaleID = Locale.getLocaleID(LocalServer.getHostName(), m_localInstance.m_strDefaultTierName);
        }

        targetLocale = m_localInstance.getLocaleWithID(strLocaleID);

        //Set default tiername (for update from other peer)
        //It may not be set if there is no default storage in the policy
        if (m_localInstance.m_strDefaultTierName == null && targetLocale.isLocalLocale() == true) {
            m_localInstance.m_strDefaultTierName = targetLocale.getTierName();
        }

        //Set runtime param
        responseParams.put(TARGET_LOCALE, targetLocale);



    }
}