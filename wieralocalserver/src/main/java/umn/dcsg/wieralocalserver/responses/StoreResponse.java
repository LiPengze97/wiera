package umn.dcsg.wieralocalserver.responses;

import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.Locale;
import umn.dcsg.wieralocalserver.MetaObjectInfo;
import umn.dcsg.wieralocalserver.info.OperationLatency;
import umn.dcsg.wieralocalserver.responses.peers.ForwardPutResponse;
import umn.dcsg.wieralocalserver.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import static umn.dcsg.wieralocalserver.Constants.*;
import static umn.dcsg.wieralocalserver.MetaObjectInfo.NEW_VERSION;
import static umn.dcsg.wieralocalserver.MetaObjectInfo.NO_SUCH_VERSION;

/**
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
        String strReason = NOT_HANDLED + " in " + getClass().getSimpleName();
        int nVer = NO_SUCH_VERSION;
        String strTag = "";
        long lLastModifiedTime = 0;
        Locale targetLocale = null;
        MetaObjectInfo meta;

        try {
            String strKey = (String) responseParams.get(KEY);
            byte[] value = (byte[]) responseParams.get(VALUE);
            targetLocale = (Locale) responseParams.get(TARGET_LOCALE);

            if (responseParams.containsKey(TAG)) {
                strTag = (String) responseParams.get(TAG);
            }

            if (responseParams.containsKey(VERSION)) {
                nVer = Utils.convertToInteger(responseParams.get(VERSION));
            }

            if (targetLocale.isLocalLocale() == true) {
                //Put locally
                if (nVer >= NEW_VERSION) {
                    meta = m_localInstance.put(strKey, nVer, value, targetLocale.getTierName(), strTag, false);
                } else {
                    meta = m_localInstance.put(strKey, value, targetLocale.getTierName(), strTag, false);
                }

                if (meta != null) {
                    strReason = OK;
                    bRet = true;
                    nVer = meta.getLastestVersion();
                    lLastModifiedTime = meta.getLastModifiedTime();

                    //If operation latency is set then change tiername to real tier used for runtime tier changed
                    if (responseParams.containsKey(OPERATION_LATENCY) == true) {
                        OperationLatency operationLatency = (OperationLatency) responseParams.get(OPERATION_LATENCY);
                        operationLatency.updateTierName(targetLocale.getTierName());
                    }

                    //Which keys in which locale and version?
                    Map<Locale, Map<MetaObjectInfo, Vector<Integer>>> keyList = new HashMap<>();

                    Map<MetaObjectInfo, Vector<Integer>> keys = new HashMap<>();
                    Vector<Integer> vers = new Vector<>();
                    vers.add(nVer);

                    keys.put(meta, vers);
                    keyList.put((Locale) responseParams.get(TARGET_LOCALE), keys);

                    //Fill-out params for the next response if exists
                    responseParams.put(KEY_LIST, keyList);
                    responseParams.put(VERSION, nVer);
                    responseParams.put(LAST_MODIFIED_TIME, lLastModifiedTime);
                    responseParams.put(TAG, strTag);
                    responseParams.put(TIER_NAME, targetLocale.getTierName());

                    addMetaToUpdate(meta, responseParams);
                } else {
                    strReason = "Failed to put data into Tier: " + targetLocale.getTierName();
                }
            } else {
                //forward put operation
                //Meta data will be updated by broadcasting later with this forward
                bRet = Response.respondAtRuntimeWithClass(m_localInstance, ForwardPutResponse.class, responseParams);

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
        Locale targetLocale = null;
        String strLocaleID;

        if (responseParams.containsKey(TO) == true) {
            strLocaleID = (String) responseParams.get(TO);
            targetLocale = m_localInstance.getLocaleWithID(strLocaleID);
        } else if (m_initParams.containsKey(TO) == true) {
            strLocaleID = (String) m_initParams.get(TO);
            targetLocale = m_localInstance.getLocaleWithID(strLocaleID);
        }

        if (targetLocale == null) {
            targetLocale = Locale.defaultLocalLocale;
        } else {
            //Set default tiername (for update from other peer)
            //It may not be set if there is no default storage in the policy
            if (Locale.defaultLocalLocale == null && targetLocale.isLocalLocale() == true) {
                Locale.defaultLocalLocale = targetLocale;
            }
        }

        if (targetLocale == null) {
            System.out.println("[debug] This should not happen. There is not Target Locale specified.");
        }

        //Set runtime param
        responseParams.put(TARGET_LOCALE, targetLocale);
    }

    @Override
    public boolean doCheckResponseConditions(Map<String, Object> responseParams) {
        return true;
    }
}