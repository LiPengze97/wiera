package umn.dcsg.wieralocalserver.responses;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import com.google.common.util.concurrent.RateLimiter;
import umn.dcsg.wieralocalserver.*;
import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by ajay on 7/13/13.
 */
public class  CopyResponse extends Response {
    public CopyResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
    }

    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(KEY_LIST);
        m_lstRequiredParams.add(TO);
        m_lstRequiredParams.add(FROM);
        m_lstRequiredParams.add(RATE);
    }

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {
        if (m_initParams.containsKey(FROM) == true) {
            responseParams.put(FROM, m_initParams.get(FROM));
        }

        if (m_initParams.containsKey(TO) == true) {
            responseParams.put(TO, m_initParams.get(TO));
        }

        if (responseParams.containsKey(RATE) == false) {
            if(m_initParams.containsKey(RATE) == true) {
                responseParams.put(RATE, m_initParams.get(RATE));
            } else {
                responseParams.put(RATE, 5.0);
            }
        }
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        boolean bRet = true;
/*
        //Locale (from) : Key : versions
        HashMap<Locale, Map<MetaObjectInfo, Vector<Integer>>> keyList = (HashMap<Locale, Map<MetaObjectInfo, Vector<Integer>>>) responseParams.get(KEY_LIST);
        Locale fromLocale = m_localInstance.getLocaleWithID((String) responseParams.get(FROM));
        Locale targetLocale = m_localInstance.getLocaleWithID((String) responseParams.get(TO));
        Double rate = (Double) responseParams.get(RATE);
        RateLimiter rateLimiter = RateLimiter.create(rate);
        MetaObjectInfo obj;
        String strKey;
        String strVersionedKey;
        int nVer;
        Vector verList;
        String strReason = NOT_HANDLED;

        System.out.println(((Locale)keyList.keySet().toArray()[0]).getLocaleID());
        System.out.println(fromLocale.getLocaleID());


        //Locale now override equals()
        if(keyList.containsKey(fromLocale) == true && keyList.get(fromLocale).size() > 0) {
            Map<MetaObjectInfo, Vector<Integer>> keys = keyList.get(fromLocale);
            for (Map.Entry<MetaObjectInfo, Vector<Integer>> entry : keys.entrySet()) {
                rateLimiter.acquire();

                obj = entry.getKey();
                verList = entry.getValue();
                strKey = obj.getKey();

                System.out.printf("[debug] Key: %s will be copied from %s to %s\n", strKey, fromLocale.getTierName(), targetLocale.getTierName());

                ReentrantReadWriteLock lock = m_localInstance.m_keyLocker.getLock(strKey);

                try {
                    int nVerCnt = verList.size();

                    lock.writeLock().lock();

                    for (int i = 0; i < nVerCnt; i++) {
                        nVer = (int) verList.get(i);
                        strVersionedKey = obj.getVersionedKey(nVer);

                        //Check whether version is in required tier.
                        //Need to be optimized to avoid multiple search
                        //Todo, We might simply ask to targetInstance to move data into other rather than retrieving it to local
                        if (obj.hasLocale(fromLocale.getLocaleID()) == false) {
                            System.out.println("[debug] Cannot find locale in the meta object in copy response.");
                            continue;
                        }

                        byte[] value;

                        //Check from locale
                        if(fromLocale.isLocalLocale() == true) {
                            value = m_localInstance.get(strVersionedKey, fromLocale.getTierName());
                        } else {
                            //forward get operation if not local locale
                            bRet = Response.respondAtRuntimeWithClass(m_localInstance, ForwardGetResponse.class, responseParams);

                            if (bRet == false) {
                                strReason = "Failed to forward get in CopyResponse: " + responseParams.get(REASON);
                                break;
                            } else {
                                value = (byte[]) responseParams.get(VALUE);
                            }
                        }

                        //Check target locale
                        if(targetLocale.isLocalLocale() == true) {
                            bRet = m_localInstance.putInternal(strVersionedKey, value, targetLocale.getTierName());
                        } else {
                            bRet = Response.respondAtRuntimeWithClass(m_localInstance, ForwardPutResponse.class, responseParams);

                            if (bRet == false) {
                                strReason = "Failed to forward put in CopyResponse: " + responseParams.get(REASON);
                                break;
                            }
                        }

                        if (bRet == true) {
                            obj.addLocale(nVer, targetLocale);

                            //Todo dirty bit can apply multiple versions.
                            obj.clearDirty();
                        }
                    }
                } catch(Exception e) {
                    e.printStackTrace();
                } finally{
                    lock.writeLock().unlock();

                    if (!bRet) {
                        break;
                    } else {
                        addObjsToUpdate(obj, responseParams);
                    }
                }
            }
        }

        //Put the reason
        responseParams.put(REASON, strReason);
    */
        return bRet;
    }

}