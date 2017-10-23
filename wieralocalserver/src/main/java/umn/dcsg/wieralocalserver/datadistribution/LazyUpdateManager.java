package umn.dcsg.wieralocalserver.datadistribution;

import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.MetaObjectInfo;
import umn.dcsg.wieralocalserver.Locale;
import umn.dcsg.wieralocalserver.info.Latency;
import umn.dcsg.wieralocalserver.responses.BroadcastResponse;
import umn.dcsg.wieralocalserver.responses.Response;
import umn.dcsg.wieralocalserver.info.OperationLatency;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * This class is for distributing update to other LocalInstance instances in the background and used for responses.
 *
 * @author Kwangsung Oh
 */
public class LazyUpdateManager {
    private LocalInstance m_localInstance;
    private LinkedBlockingQueue<Map<String, Object>> m_queue;
    private Boolean m_moreItem;
    private boolean m_continued;
    private List m_updateThreadsList;

    public LazyUpdateManager(LocalInstance instance, int nWorkerCnt) {
        m_localInstance = instance;
        m_queue = new LinkedBlockingQueue<Map<String, Object>>();
        m_moreItem = new Boolean(false);
        m_continued = true;
        m_updateThreadsList = new LinkedList<Thread>();
        Thread updater;

        for (int i = 0; i < nWorkerCnt; i++) {
            updater = new Thread(new Runnable() {
                @Override
                public void run() {
                    lazyUpdate();
                }
            });

            updater.start();
            m_updateThreadsList.add(updater);
        }
    }

    public void stopRunning() {
        m_continued = false;

        synchronized (m_moreItem) {
            m_moreItem.notifyAll();
        }

        //Join
        Thread updater;

        for (int i = 0; i < m_updateThreadsList.size(); i++) {
            updater = (Thread) m_updateThreadsList.get(i);

            try {
                updater.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        m_updateThreadsList.clear();
    }

    //for now read from local storage (can be memory or EBS as defined)
    private void lazyUpdate() {
        long lastModifiedTime = 0;
        Map<String, Object> responseParams;
       /* while (true) {
            try {
                responseParams = m_queue.take();

                //Wait a new item or close
                if (responseParams == null) {
                    if (m_continued == false) {
                        break;
                    }

                    synchronized (m_moreItem) {
                        m_moreItem.wait();
                    }
                } else {
                    //Check whether version has been changed while in the QueueResponse
                    String strKey = (String)responseParams.get(KEY);
                    byte[] value = null;

                    if(responseParams.containsKey(VALUE) == true) {
                        value = (byte[])responseParams.get(VALUE);
                    } else {
                        MetaObjectInfo obj = m_localInstance.getMetadata(strKey);

                        //If metadata is not updated yet.
                        if(obj == null) {
                            if(responseParams.containsKey(OBJS_LIST) == true) {
                                Map<String, MetaObjectInfo> objsList = (Map) responseParams.get(OBJS_LIST);

                                if (objsList.containsKey(strKey) == true) {
                                    obj = objsList.get(strKey);
                                }
                            }
                        }

                        if(obj != null) {
                            value = m_localInstance.getInternal(obj.getVersionedKey((int)responseParams.get(VERSION)), (String)responseParams.get(TIER_NAME));
                        }
                    }

                    if (value == null) {
                        System.out.format("Failed to getObject the value for the key: %s from storage tier: %s.\n",
                                responseParams.get(KEY), responseParams.get(TIER_NAME));
                    } else {
                        //This is for lazy broadcasting to other instances.
                        OperationLatency operationLatency = (OperationLatency)responseParams.get(OPERATION_LATENCY);
                        Latency latency = operationLatency.addTimer(BroadcastResponse.class.getSimpleName());
                        latency.start();
                        //Create Broadcasting response and run
                        if(Response.respondAtRuntimeWithClass(m_localInstance, BroadcastResponse.class, responseParams) == false)
                        {
                            System.out.println("Failed to broadcast: " + responseParams.get(REASON));
                        }
                        latency.stop();
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }*/
    }

    public boolean putToQueue(List<Locale> targetList, Map<String, Object> params) {//String strKey, int nVer, byte[] value, String strTierName, String strTag, OperationLatency operationLatency) {
        try {
            m_queue.put(new HashMap<>(params));
            synchronized (m_moreItem) {
                m_moreItem.notify();
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}