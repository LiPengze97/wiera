package umn.dcsg.wieralocalserver.responses;

import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.Locale;
import umn.dcsg.wieralocalserver.MetaObjectInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by with IntelliJ IDEA.
 * User: Kwangsung
 * Date: 9/11/2017 10:46 AM
 */
public class SearchKeysResponse extends Response {
    public enum QUERY_TYPE_SUPPORTED {
        QUERY_NOT_SUPPORTED(-1),
        QUERY_DIRTY(0),
        QUERY_ALL(1),
        QUERY_OLDEST(2),
        QUERY_NEWEST(3);
//        ACCESSED_IN_PERIOD(1),
//        NOT_ACCESSED_IN_PERIOD(2);

        private final int m_queryType;

        QUERY_TYPE_SUPPORTED(final int newQueryType) {
            m_queryType = newQueryType;
        }

        public int getQueryType() {
            return m_queryType;
        }
    }


    public SearchKeysResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
    }

    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(TARGET_LOCALE); //Find keys from which tier?
        m_lstRequiredParams.add(QUERY_TYPE); //Find keys from which tier?
    }

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {
        try {
            String strQueryType = (String) m_initParams.get(QUERY_TYPE);
            Locale targetLocale = m_localInstance.getLocaleWithID((String) m_initParams.get(FROM));
            QUERY_TYPE_SUPPORTED queryType;
            responseParams.put(TARGET_LOCALE, targetLocale);

            switch (strQueryType) {
                case DIRTY:
                    queryType = QUERY_TYPE_SUPPORTED.QUERY_DIRTY;
                    break;
                case ALL:
                    queryType = QUERY_TYPE_SUPPORTED.QUERY_ALL;
                    break;
                case OLDEST:
                    queryType = QUERY_TYPE_SUPPORTED.QUERY_OLDEST;
                    break;
                case NEWEST:
                    queryType = QUERY_TYPE_SUPPORTED.QUERY_NEWEST;
                    break;
                default:
                    queryType = QUERY_TYPE_SUPPORTED.QUERY_NOT_SUPPORTED;
                    break;
            }

            responseParams.put(QUERY_TYPE, queryType);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        boolean bRet = true;/*
        try {
            QUERY_TYPE_SUPPORTED queryType = (QUERY_TYPE_SUPPORTED) responseParams.get(QUERY_TYPE);
            Map<MetaObjectInfo, Vector<Integer>> keys = null;
            Map<Locale, Map<MetaObjectInfo, Vector<Integer>>> keyList = new HashMap<>();
            Locale targetLocale = (Locale) responseParams.get(TARGET_LOCALE);

            switch (queryType) {
                case QUERY_DIRTY:
                    keys = m_localInstance.m_metadataStore.searchDirtyObject(targetLocale);
                    break;
                case QUERY_ALL:
                    keys = m_localInstance.m_metadataStore.searchAllObject(targetLocale);
                    break;
                case QUERY_OLDEST:
                    keys = m_localInstance.m_metadataStore.searchOldestObject(targetLocale);
                    break;
                case QUERY_NEWEST:
                    keys = m_localInstance.m_metadataStore.searchNewestObject(targetLocale, true);
                    break;
                default:
                    //Not supported Yet.
                    //Empty Key
                    responseParams.put(REASON, NOT_SUPPORTED_QUERY);
                    bRet = false;
            }

            if(bRet == true && keys != null) {
                keyList.put(targetLocale, keys);
                responseParams.put(KEY_LIST, keyList);
            }

            responseParams.put(RESULT, bRet);
            return bRet;

        } catch (Exception e) {
            e.printStackTrace();
        }*/

        return false;
    }
}