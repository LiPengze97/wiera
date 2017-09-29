package umn.dcsg.wieralocalserver.responses;

import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.Locale;
import umn.dcsg.wieralocalserver.MetaObjectInfo;

import java.security.MessageDigest;
import java.util.Map;
import java.util.Vector;

import javax.xml.bind.annotation.adapters.HexBinaryAdapter;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by ajay on 3/22/14.
 */
public class StoreOnceResponse extends StoreResponse {
    public StoreOnceResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        String strKey = (String) responseParams.get(KEY);
        byte[] value = (byte[]) responseParams.get(VALUE);
        Locale locale = (Locale) responseParams.get(TARGET_LOCALE);
        String strReason;
        boolean bRet = false;

        try {
            MetaObjectInfo obj = null;
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hash = md.digest(value);
            String strHash = (new HexBinaryAdapter()).marshal(hash);
            Map<MetaObjectInfo, Vector<Integer>> keyList = m_localInstance.m_metadataStore.getWithTags(strHash);

            if (keyList != null && keyList.isEmpty()) {
                obj = m_localInstance.put(strKey, value, locale.getTierName(), strHash, false);
            }

            if (obj == null) {
                bRet = false;
                strReason = "Failed to storeOnce";
            } else {
                bRet = true;
                strReason = OK;
                addObjsToUpdate(obj, responseParams);
            }
        } catch (Exception e) {
            strReason = e.getMessage();
        }

        //Result
        responseParams.put(RESULT, bRet);
        if (bRet == false) {
            responseParams.put(REASON, strReason);
        }

        return bRet;
    }
}