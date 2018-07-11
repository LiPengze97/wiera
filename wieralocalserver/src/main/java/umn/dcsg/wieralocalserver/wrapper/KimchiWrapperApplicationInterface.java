package umn.dcsg.wieralocalserver.wrapper;

import com.google.gson.Gson;
import org.apache.thrift.TException;
import org.json.JSONObject;
import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.LocalServer;
import umn.dcsg.wieralocalserver.thriftinterfaces.KimchiWrapperApplicationIface;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by with IntelliJ IDEA.
 * User: Kwangsung
 * Date: 4/17/2018 8:14 AM
 */
public class KimchiWrapperApplicationInterface implements KimchiWrapperApplicationIface.Iface{
    LocalInstance m_localInstance;
    Map<Integer, Map<Integer, String>> m_placement;
    Map<Integer, Integer> m_shuffleIdToChildStageId;
    int m_nDebugReadCount = 0;
    int m_nDebugWriteCount = 0;

    long m_nDebugReadSize = 0;
    long m_nDebugWriteSize = 0;
    private static final Object countReadLock = new Object();
    private static final Object countWriteLock = new Object();

    private ConcurrentHashMap<Integer, ConcurrentLinkedDeque> m_queueForPush = new ConcurrentHashMap<>();

    Map<String, AtomicInteger> m_read = new ConcurrentHashMap<>();

    public KimchiWrapperApplicationInterface(LocalInstance instance) {
        m_localInstance = instance;
    }

/*
    @Override
    public String setTaskPlacement(int nChildStageId, Map<Integer, String> placement) throws TException {
        //Need to be modified to update task placement for each shuffle id for queuing
        //This will be called several times now.
        if(m_placement == null) {
            m_placement = new HashMap<Integer, Map<Integer, String>>();
        }

        m_placement.put(nChildStageId, placement);

        //Invoke a thread to push intermeidate data in the queue
        //Thread

        //Return
        JSONObject ret = new JSONObject();
        ret.put(RESULT, true);
        ret.put(VALUE, "OK");

        return ret.toString();
    }
*/

      @Override
      public String setTaskPlacement(Map<Integer, Map<Integer, String>> placement, Map<Integer, Integer> shuffleIdToChildStage) throws TException {
        //Need to be modified to update task placement for each shuffle id for queuing
        //This will be called several times now.
        System.out.println("I'm here for task placement");
        m_placement = placement;
        m_shuffleIdToChildStageId = shuffleIdToChildStage;

        //Return
        JSONObject ret = new JSONObject();
        ret.put(RESULT, true);
        ret.put(VALUE, "OK");

        return ret.toString();
    }

    @Override
    public String preReplicatingWithPlacement(String strKey, ByteBuffer value, boolean bSync) throws TException {
        //Based on placement, pre replicating data into the target destinations
        //"%s_shuffle_%d_%d_%d
        String strReason = NOT_HANDLED + " in " + getClass().getSimpleName();
        String[] tokens = strKey.split("_");
        String strOriginHost = tokens[0];
        int nShuffleId = Integer.parseInt(tokens[2]);
        int nPartition = Integer.parseInt(tokens[4]);

        int nChildStageId = m_shuffleIdToChildStageId.get(nShuffleId);
        List<String> targetHostList = new LinkedList<String>();
        JSONObject ret = null;
        boolean bRet = false;

        //Check task placement is available.
        if(m_placement.containsKey(nChildStageId) == true) {
            targetHostList.add(m_placement.get(nChildStageId).get(nPartition));
            //Avoid to write again to local
            //Here simply assume Wiera instance running with worker
            if(targetHostList.contains(LocalServer.getHostName()) == true) {
                targetHostList.remove(LocalServer.getHostName());
            }

            if(targetHostList.size() > 0) {
                //Need to write local?
                //For teting for now. allow write local instance even spark has the same data in the local block manager
                ByteBuffer result = m_localInstance.m_applicationToLocalInstanceInterface.putTo(strKey, value, targetHostList, bSync);
                ret = new JSONObject(new String(result.array(), result.position(), result.remaining()));
                bRet = ret.getBoolean(RESULT);

                if(bRet == true) {
                   /* synchronized (countWriteLock) {
                        m_nDebugWriteCount++;
                        m_nDebugWriteSize += ret.getInt(SIZE);
                    System.out.printf("W:%d (%d bytes), R:%d (%d bytes), key: %s\n",
                            m_nDebugWriteCount, m_nDebugWriteSize, m_nDebugReadCount, m_nDebugReadSize, strKey);
                    }*/
                } else {
                    strReason = "Failed to put into local instance.";
                    System.out.println(strReason);
                }
            } else {
                ret = new JSONObject();
                strReason = "No target (remote) host is available. Avoiding same data locally again.";
                //System.out.println(strReason);
            }
        } else {
            //Task placement is not ready yet. The request needs to be queued.
            //Store locally with an assumption that this will use memory cache
            ByteBuffer result = m_localInstance.m_applicationToLocalInstanceInterface.put(strKey, value);

            ret = new JSONObject(new String(result.array(), result.position(), result.remaining()));
            bRet = ret.getBoolean(RESULT);

            if(bRet == true) {
                ConcurrentLinkedDeque queue;
                if(m_queueForPush.containsKey(nChildStageId) == false) {
                    queue = new ConcurrentLinkedDeque();
                    m_queueForPush.put(nChildStageId, queue);
                } else {
                    queue = m_queueForPush.get(nChildStageId);
                }

                //Add for later
                queue.add(strKey);
                strReason = "Intermediate data is being queued locally.";
            } else {
                strReason = "Failed to put into local instance for queuing!!!!!!!!!!!!!!";
                System.out.println(strReason);
            }
        }

        ret.put(RESULT, bRet);
        ret.put(REASON, strReason);

        Gson gson = new Gson();
        ret.put(TARGET_LOCALE_LIST, gson.toJson(targetHostList));

        return ret.toString();
    }

    //Need to update
    @Override
    public String put(String strKey, ByteBuffer value) throws TException {
        List<String> list = new LinkedList<String>();
        list.add(LocalServer.getHostName());
        ByteBuffer result = m_localInstance.m_applicationToLocalInstanceInterface.putTo(strKey, value, list, false);

        JSONObject ret = new JSONObject(new String (result.array(), result.position(), result.remaining()));
        boolean bRet = ret.getBoolean(RESULT);

        JSONObject resultRet = new JSONObject();
        resultRet.put(RESULT, bRet);

        if(bRet == false) {
            System.out.printf("Failed to put the key:%s (- %s)\n", strKey, ret.get(REASON));
            resultRet.put(REASON, ret.get(REASON));
        } else {
            resultRet.put(VALUE, "OK");
        }

        return resultRet.toString();
    }

    //Need to update
    @Override
    public String get(String strKey) throws TException {
        ByteBuffer result = m_localInstance.m_applicationToLocalInstanceInterface.get(strKey);

        JSONObject ret = new JSONObject(new String (result.array(), result.position(), result.remaining()));
        boolean bRet = ret.getBoolean(RESULT);

        if(bRet == true) {
            /*AtomicInteger counter = m_read.getOrDefault(strKey, new AtomicInteger());

            if(m_read.containsKey(strKey) == false) {
                m_read.put(strKey, counter);
            }

            int curCount = counter.incrementAndGet();

            if (curCount > 2) {
                System.out.printf("Key: %s, accessed : %d\n", strKey, curCount);
            }

            synchronized (countReadLock) {
                m_nDebugReadCount++;
                m_nDebugReadSize += ret.getInt(SIZE);
            }

            System.out.printf("W:%d (%d bytes), R:%d (%d bytes), key: %s\n",
                    m_nDebugWriteCount, m_nDebugWriteSize, m_nDebugReadCount, m_nDebugReadSize, strKey);
*/
        } else {
            System.out.printf("Failed to retrieve the key: %s\n", strKey);
        }

        return ret.toString();//new String(result.array(), result.position(), result.remaining());
    }

    @Override
    public String getMetaData(String strKey) throws TException {
        //Result
        ByteBuffer result = m_localInstance.m_applicationToLocalInstanceInterface.getMetaData(strKey);
        return new String(result.array(), result.position(), result.remaining());

    }
}