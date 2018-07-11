package umn.dcsg.wieralocalserver;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.gson.Gson;
import org.apache.commons.codec.binary.Base64;
import org.json.JSONObject;
import umn.dcsg.wieralocalserver.info.OperationLatency;
import umn.dcsg.wieralocalserver.storageinterfaces.StorageInterface;
import umn.dcsg.wieralocalserver.thriftinterfaces.ApplicationToLocalInstanceIface;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by ajay on 4/26/14.
 */
public class ApplicationToLocalInstanceInterface implements ApplicationToLocalInstanceIface.Iface {
    LocalInstance m_localInstance = null;
    //String emptyStr = new String("");

    public ApplicationToLocalInstanceInterface(LocalInstance localInstance) {
        this.m_localInstance = localInstance;
    }

    public ByteBuffer putTo(String strKey, ByteBuffer value, List<String> to, boolean bSync) {
        //Basically
        List<Locale> localeList = Locale.getLocalesWithoutTierName(to);

        m_localInstance.m_localInfo.incrementalRequestCnt(PUT);
        OperationLatency operationLatency = m_localInstance.m_localInfo.addOperationLatency(strKey, Locale.getLocaleID(LocalServer.getHostName(), ""), ACTION_PUT_EVENT, Constants.PUT_LATENCY);

        //Start overall timer
        //Each timer need to be _triggerPutEvent by each response
        operationLatency.start();

        boolean bRet = false;
        byte[] bytes = new byte[value.remaining()];

        //read value into bytes
        value.get(bytes);

        //Set event params
        HashMap<String, Object> eventParams = new HashMap<>();
        eventParams.put(KEY, strKey);
        eventParams.put(VALUE, bytes);
        eventParams.put(OPERATION_LATENCY, operationLatency);
        eventParams.put(TO, DYNAMIC_LOCALES);
        eventParams.put(TARGET_LOCALE, localeList);
        eventParams.put(SYNC, bSync);
        eventParams.put(SHARE_META_INFO, false);

        ByteBuffer ret = _triggerPutEvent(eventParams);

        //Stop overall timer
        operationLatency.stop();
        return ret;
    }

    public ByteBuffer put(String strKey, ByteBuffer value) {
        //Update request and operation information
        //Update number of request
        m_localInstance.m_localInfo.incrementalRequestCnt(PUT);
        OperationLatency operationLatency = m_localInstance.m_localInfo.addOperationLatency(strKey, Locale.getLocaleID(LocalServer.getHostName(), ""), ACTION_PUT_EVENT, Constants.PUT_LATENCY);

        //Start overall timer
        //Each timer need to be _triggerPutEvent by each response
        operationLatency.start();

        byte[] bytes = new byte[value.remaining()];

        //read value into bytes
        value.get(bytes);

        //Set event params
        HashMap<String, Object> eventParams = new HashMap<>();
        eventParams.put(KEY, strKey);
        eventParams.put(VALUE, bytes);
        eventParams.put(OPERATION_LATENCY, operationLatency);

        ByteBuffer ret = _triggerPutEvent(eventParams);

        //Stop overall timer
        operationLatency.stop();
        return ret;
    }

    private ByteBuffer _triggerPutEvent(HashMap<String, Object> eventParams) {
        boolean bRet = false;
        Object result;
        Iterator<UUID> putEventIterator = m_localInstance.onPutEvents.iterator();

        try {
            while (putEventIterator.hasNext()) {
                result = m_localInstance.m_eventRegistry.evaluateEvent(putEventIterator.next(), eventParams, ACTION_PUT_EVENT);

                if(result == null) {
                    continue;
                }

                if (result instanceof Boolean) {
                    bRet = (Boolean) result;
                } else if (result instanceof Map) {
                    bRet = (boolean) ((Map) result).get(RESULT);
                }

                if (bRet == false) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        //Trigger other events Only when operation is succeed
        if (bRet == true) {
            m_localInstance.onPutSignalLock.lock();
            m_localInstance.putEventOccurred.signal();
            m_localInstance.onPutSignalLock.unlock();
        }

        //Now assume that WieraClient is running with application
        //Result
        JSONObject response = new JSONObject();
        response.put(TYPE, PUT);
        response.put(RESULT, bRet);
        response.put(SIZE, eventParams.get(SIZE));
        response.put(REASON, eventParams.get(REASON));
        return ByteBuffer.wrap(response.toString().getBytes());
    }

    public ByteBuffer update(String key, int nVersion, ByteBuffer value) {
        JSONObject response = new JSONObject();
        response.put(TYPE, UPDATE);
        response.put(RESULT, false);
        response.put(REASON, NOT_SUPPORTED_QUERY);
        return ByteBuffer.wrap(response.toString().getBytes());
    }

    public ByteBuffer get(String strKey){
        //Update request and operation information
        //Update number of request
        m_localInstance.m_localInfo.incrementalRequestCnt(GET);
        OperationLatency operationLatency = m_localInstance.m_localInfo.addOperationLatency(strKey, Locale.getLocaleID(LocalServer.getHostName(), ""), ACTION_GET_EVENT, Constants.GET_LATENCY);

        //Start overall timer
        //Each timer need to be _triggerPutEvent by each response
        operationLatency.start();

        boolean bRet;
        String strReason;
        byte[] value = null;
        Object result;

        //Set event params
        HashMap<String, Object> eventParams = new HashMap<>();
        eventParams.put(KEY, strKey);
        eventParams.put(OPERATION_LATENCY, operationLatency);

        try {
            Iterator<UUID> getEventIterator = m_localInstance.onGetEvents.iterator();

            while (getEventIterator.hasNext()) {
                result = m_localInstance.m_eventRegistry.evaluateEvent(getEventIterator.next(), eventParams, ACTION_GET_EVENT);

                if (result instanceof byte[]) {
                    value = (byte[]) result;
                } else if (result instanceof Map) {
                    value = (byte[]) ((Map) result).get(VALUE);
                }
            }

            if (value != null) {
                bRet = true;
            } else {
                //Fail
                value = NULL_VALUE.getBytes();
                bRet = false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            bRet = false;
        }

        //Trigger other events Only when operation is succeed
        if (bRet == true) {
            m_localInstance.onGetSignalLock.lock();
            m_localInstance.getEventOccurred.signal();
            m_localInstance.onGetSignalLock.unlock();
        }

        //Stop overall timer
        operationLatency.stop();

        //Now assume that WieraClient is running with application
        //Result
        JSONObject response = new JSONObject();
        response.put(TYPE, GET);
        response.put(RESULT, bRet);
        response.put(SIZE, value.length);
        response.put(VALUE, Base64.encodeBase64String(value));
        response.put(REASON, eventParams.get(REASON));
        return ByteBuffer.wrap(response.toString().getBytes());
    }

    public ByteBuffer getVersion(String key, int nVersion){
        boolean bRet;
        String strReason;
        byte[] value = m_localInstance.get(key, nVersion);

        if (value != null) {
            bRet = true;
            strReason = OK;
        } else {
            bRet = false;
            value = NULL_VALUE.getBytes();
            strReason = NO_VERSION;
        }

        JSONObject response = new JSONObject();
        response.put(TYPE, GET_VERSION);
        response.put(RESULT, bRet);
        response.put(VALUE, Base64.encodeBase64String(value));
        response.put(REASON, strReason);

        return ByteBuffer.wrap(response.toString().getBytes());
    }

    //Need to modify data type. JSON? Map? Whatever, For now String (0=LocalInstanceDataOject... 1= ....)
    public ByteBuffer getVersionList(String key) {
        /*boolean bRet;
        String strReason;
        MetaObjectInfo obj = m_localInstance.getMetadata(key);
        byte[] value;

        Gson gson = new Gson();
        String strVersionLIst = gson.toJson(m_localInstance.getVersionList(key));
        return ByteBuffer.wrap(strVersionLIst.getBytes());*/

        JSONObject response = new JSONObject();
        response.put(TYPE, GET_VERSION);
        response.put(RESULT, false);
        response.put(VALUE, NULL_VALUE.getBytes());
        response.put(REASON, NOT_SUPPORTED);

        return ByteBuffer.wrap(response.toString().getBytes());
    }

    public ByteBuffer getMetaData(String strKey) {
        boolean bRet;
        String strReason;
        String value = "";

        //Operation
        MetaObjectInfo obj = m_localInstance.getMetadata(strKey);

        if (obj != null) {
            bRet = true;
            strReason = OK;
            Gson gson = new Gson();
            value = gson.toJson(obj.getMetaData());
        } else {
            //Write in progress
            bRet = false;

            if(m_localInstance.m_keyLocker.getLock(strKey).writeLock().tryLock() == false) {
                strReason = PUT_IN_PROGRESS_FOR_KEY;
            } else {
                strReason = NO_META;
            }
        }

        //System.out.println("[debug] Check meta data + " + strReason + " Key:" + strKey);

        JSONObject response = new JSONObject();
        response.put(TYPE, GET_META_DATA);
        response.put(RESULT, bRet);
        response.put(VALUE, value);
        response.put(REASON, strReason);

        return ByteBuffer.wrap(response.toString().getBytes());
    }

    public ByteBuffer rename(String strSrcKey, String strTargetKey) {
        boolean bRet = false;
        String strReason = NOT_HANDLED  + " in " + getClass().getSimpleName();

        MetaObjectInfo srcObj = m_localInstance.getMetadata(strSrcKey);
        Tier tier;

        if(srcObj != null) {
            if(m_localInstance.getMetadata(strTargetKey) == null) {
                MetaObjectInfo meta = m_localInstance.updateMetadata(strTargetKey, srcObj.getSize(), "", srcObj.getLastestVersion());

                //Update meta for tag
                Set <String> tags = srcObj.getTags();
                for(String strTag: tags) {
                    meta.addTag(strTag);
                }

                Map<String, Locale> localeList = srcObj.getLocaleList();
                Locale locale;

                //Add locales into meta
                for(String strLocaleID: localeList.keySet()) {
                    locale = localeList.get(strLocaleID);
                    meta.addLocale(0, locale);
                }

                //Meta is ready. do rename
                StorageInterface storageInterface;

                for(String strLocaleID: localeList.keySet()) {
                    locale = localeList.get(strLocaleID);
                    tier = m_localInstance.m_tiers.getTier(locale.getTierName());
                    bRet = tier.getInterface().rename(strSrcKey, strTargetKey);

                    if (bRet == false) {
                        strReason = "Failed to rename from Key: " + strSrcKey + " to Key:" + strTargetKey;
                        break;
                    }
                }

                if(bRet == true) {
                    m_localInstance.commitMeta(meta);
                    strReason = OK;
                }
            } else {
                strReason = "Target key: " + strTargetKey + " already exists";
            }
        } else {
            strReason = "Failed to find meta of source key: " + strSrcKey;
        }

        JSONObject response = new JSONObject();
        response.put(RESULT, bRet);
        response.put(REASON, strReason);
        return ByteBuffer.wrap(response.toString().getBytes());
    }

    public ByteBuffer copy(String strSrcKey, String strTargetKey) {
        boolean bRet = false;
        String strReason = NOT_HANDLED  + " in " + getClass().getSimpleName();

        MetaObjectInfo srcObj = m_localInstance.getMetadata(strSrcKey);
        Tier tier;
        boolean bEnoughSpace = true;

        if(srcObj != null) {
            if(m_localInstance.getMetadata(strTargetKey) == null) {
                Map<String, Locale> localeList = srcObj.getLocaleList();
                Locale locale;

                //Check first if all tier has enough space
                for(String strLocaleID: localeList.keySet()) {
                    locale = localeList.get(strLocaleID);
                    tier = m_localInstance.m_tiers.getTier(locale.getTierName());

                    if(tier.getFreeSpace() < srcObj.getSize()) {
                        strReason = "Tier: " + tier.getTierName() + " does not have enough space for copying";
                        bEnoughSpace = false;
                        break;
                    }
                }

                //All tier has enough space for copying
                if(bEnoughSpace == true) {
                    StorageInterface storageInterface;

                    //Check first if all tier has enough space
                    for(String strLocaleID: localeList.keySet()) {
                        locale = localeList.get(strLocaleID);
                        tier = m_localInstance.m_tiers.getTier(locale.getTierName());
                        bRet = tier.getInterface().copy(strSrcKey, strTargetKey);

                        if (bRet == false) {
                            strReason = "Failed to copy from Key: " + strSrcKey + " to Key:" + strTargetKey;
                            break;
                        }
                    }

                    if(bRet == true) {
                        MetaObjectInfo meta = m_localInstance.updateMetadata(strTargetKey, srcObj.getSize(), "", srcObj.getLastestVersion());

                        //Update meta for tag
                        Set <String> tags = srcObj.getTags();
                        for(String strTag: tags) {
                            meta.addTag(strTag);
                        }

                        //Add locales
                        for(String strLocaleID: localeList.keySet()) {
                            locale = localeList.get(strLocaleID);
                            meta.addLocale(0, locale);
                        }

                        m_localInstance.commitMeta(meta);
                        strReason = OK;
                    }
                }
            } else {
                strReason = "Target key: " + strTargetKey + " already exists";
            }
        } else {
            strReason = "Failed to find meta of source key: " + strSrcKey;
        }

        JSONObject response = new JSONObject();
        response.put(RESULT, bRet);
        response.put(REASON, strReason);
        return ByteBuffer.wrap(response.toString().getBytes());
    }

    public ByteBuffer remove(String key) {
        return removeVersion(key, MetaObjectInfo.LATEST_VERSION);
    }

    public ByteBuffer removeVersion(String key, int nVersion) {
        JSONObject response = new JSONObject();
        response.put(TYPE, GET_META_DATA);

        boolean ret = false;

        try {
            ret = m_localInstance.delete(key, nVersion);

            response.put(RESULT, ret);
            response.put(REASON, "Failed to delete the key: " + key);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return ByteBuffer.wrap(response.toString().getBytes());
    }
}