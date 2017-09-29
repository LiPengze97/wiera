package umn.dcsg.wieralocalserver;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import umn.dcsg.wieralocalserver.info.OperationLatency;
import umn.dcsg.wieralocalserver.thriftinterfaces.ApplicationToLocalInstanceIface;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by ajay on 4/26/14.
 */
public class ApplicationToLocalInstanceInterface implements ApplicationToLocalInstanceIface.Iface {
    LocalInstance m_localInstance = null;
    String emptyStr = new String("");

    public ApplicationToLocalInstanceInterface(LocalInstance localInstance) {
        this.m_localInstance = localInstance;
    }

    public boolean put(String strKey, ByteBuffer value) {
        boolean bRet = false;
        byte[] bytes = new byte[value.remaining()];

        //Update request and operation information
        //Update number of request
        m_localInstance.m_localInfo.incrementalRequestCnt(PUT);
        OperationLatency operationLatency = m_localInstance.m_localInfo.addOperationLatency(strKey, Locale.getLocaleID(LocalServer.getHostName(), ""), ACTION_PUT_EVENT, Constants.PUT_LATENCY);

        //Start overall timer
        //Each timer need to be set by each response
        operationLatency.start();

        //read value into bytes
        value.get(bytes);
        Object result;
        Iterator<UUID> putEventIterator = m_localInstance.onPutEvents.iterator();

        //Set event params
        HashMap<String, Object> eventParams = new HashMap<>();
        eventParams.put(KEY, strKey);
        eventParams.put(VALUE, bytes);
        eventParams.put(OPERATION_LATENCY, operationLatency);

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

        if (bRet == true) {
            m_localInstance.onPutSignalLock.lock();
            m_localInstance.putEventOccurred.signal();
            m_localInstance.onPutSignalLock.unlock();
        }

        //Stop overall timer
        operationLatency.stop();
        return bRet;
    }

    public boolean update(String key, int nVersion, ByteBuffer value) {
        return true;
    }

    public ByteBuffer get(String strKey){
        byte[] bytes = null;

        try {
            bytes = null;
            Object result;
            Iterator<UUID> getEventIterator = m_localInstance.onGetEvents.iterator();

            //Update request and operation information
            //Update number of request
            m_localInstance.m_localInfo.incrementalRequestCnt(GET);
            OperationLatency operationLatency = m_localInstance.m_localInfo.addOperationLatency(strKey, Locale.getLocaleID(LocalServer.getHostName(), ""), ACTION_GET_EVENT, Constants.GET_LATENCY);

            //Start overall timer
            //Each timer need to be set by each response
            operationLatency.start();

            //Set event params
            HashMap<String, Object> eventParams = new HashMap<>();
            eventParams.put(KEY, strKey);
            eventParams.put(OPERATION_LATENCY, operationLatency);

            while (getEventIterator.hasNext()) {
                result = m_localInstance.m_eventRegistry.evaluateEvent(getEventIterator.next(), eventParams, ACTION_GET_EVENT);

                if (result instanceof byte[]) {
                    bytes = (byte[]) result;
                } else if (result instanceof Map) {
                    bytes = (byte[]) ((Map) result).get(VALUE);
                }
            }

            if (bytes != null) {
                m_localInstance.onGetSignalLock.lock();
                m_localInstance.getEventOccurred.signal();
                m_localInstance.onGetSignalLock.unlock();
            } else {
                return ByteBuffer.wrap(emptyStr.getBytes()); //Fail to get the value for the key.
            }

            //Stop overall timer
            operationLatency.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return ByteBuffer.wrap(bytes);
    }

    public ByteBuffer getVersion(String key, int nVersion){
        byte[] value = m_localInstance.get(key, nVersion);

        if (value != null) {
            return ByteBuffer.wrap(value);
        } else {
            return ByteBuffer.wrap(emptyStr.getBytes());
        }
    }

    //Need to modify data type. JSON? Map? Whatever, For now String (0=LocalInstanceDataOject... 1= ....)
    public ByteBuffer getVersionList(String key) {
        String strVersionLIst = m_localInstance.getVersionList(key).toString();
        return ByteBuffer.wrap(strVersionLIst.getBytes());
    }

    public boolean remove(String key) {
        //System.out.println("Also event driven? Need to check. Try Key: " + key);
        return removeVersion(key, MetaObjectInfo.LATEST_VERSION);
    }

    public boolean removeVersion(String key, int nVersion) {
        boolean ret = false;

        try {
            ret = m_localInstance.delete(key, nVersion);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return ret;
    }
}