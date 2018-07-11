package umn.dcsg.wieralocalserver.responses.peers;

//import com.backblaze.erasure.ReedSolomon;
import umn.dcsg.wieralocalserver.KeyLocker;
import umn.dcsg.wieralocalserver.Tier;
import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.responses.Response;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static umn.dcsg.wieralocalserver.Constants.KEY;
import static umn.dcsg.wieralocalserver.Constants.REASON;

/**
 * Created by Kwangsung on 4/1/2016.
 * This class is crated only for testing for now.
 * Will be elaborated.
 */

public class MergeDataResponse extends PeerResponse {
    class MergeData implements Runnable {
        String m_strKey;
        Tier m_tierInfo;
        int m_nIndex;
        CountDownLatch m_latch;

        public MergeData(String strKey, Tier tierInfo, int nIndex, CountDownLatch latch) {
            m_strKey = strKey;
            m_tierInfo = tierInfo;
            m_nIndex = nIndex;
            m_latch = latch;
        }

        @Override
        public void run() {
            if (m_tierInfo != null) {
                String chunks_key = String.format("%s_splitted_%d", m_strKey, m_nIndex);
                byte[][] shards = m_map.get(m_strKey);
                shards[m_nIndex] = m_tierInfo.getInterface().doGet(chunks_key);

                try {
                    System.out.printf("Tiername: %s Index: %d  count Down: %d\n", m_tierInfo.getTierName(), m_nIndex, m_latch.getCount());
                    m_latch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //Where to putObject list of tier
    String m_strTierName;
    KeyLocker m_KeyLocker = new KeyLocker();
    Map<String, byte[][]> m_map = new HashMap<String, byte[][]>();

    protected List m_tierList = null;
    protected int m_nDataShardCnt;
    protected int m_nParityShardCnt;
    protected int m_nFilterLayer = 0;

    public MergeDataResponse(LocalInstance localInstance, String strEventName, Map<String, Object> params) {
        super(localInstance, strEventName, params);
    }

    @Override
    public boolean doCheckPeerResponseConditions(Map<String, Object> responseParams) {
        responseParams.put(REASON, "NOT completed yet");
        return false;
    }

/*
    public MergeDataResponse(LocalInstance instance, List tierList, int nDataShard, int nParityShard, int nFilterLayer) {
        super(instance);
        m_tierList = tierList;
        m_nDataShardCnt = nDataShard;
        m_nParityShardCnt = nParityShard;
        m_nFilterLayer = nFilterLayer;
    }
*/


    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(KEY);
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        boolean bRet;

        int nTotalShard = m_nDataShardCnt + m_nParityShardCnt;
        String strKey = (String) responseParams.get(KEY);

        ReentrantReadWriteLock lock = m_localInstance.m_keyLocker.getLock(strKey);
        lock.readLock().lock();
        CountDownLatch latch = new CountDownLatch(m_nDataShardCnt);

        byte[][] shards = new byte[nTotalShard][];
        boolean[] shardPresent = new boolean[nTotalShard];
        m_map.put(strKey, shards);

        MergeData mergeData;
        Thread mergeThread;
        LinkedHashMap<Thread, MergeData> mergeList = new LinkedHashMap<Thread, MergeData>();
        int nStart = 0;
        int nIndex = 0;
        int nTierCnt = m_tierList.size();
        ArrayList<Integer> taken = new ArrayList<Integer>(nTotalShard);

        for (int nFilter = 0; nFilter < m_nFilterLayer; nFilter++) {
            nStart = nTierCnt - (nFilter * 2);

            //Hardcoded
            for (int i = 0; i < Math.pow(2, 4 - m_nFilterLayer); i++) {
                nIndex = (nStart + i) % nTierCnt;

                if (taken.contains(nIndex) == false) {
                    taken.add(nIndex);

                    //System.out.printf("Get Tier Name: %s, chunk_index: %s\n", ((Tier) m_tierList.getObject(i)).getLocale(), nIndex);
                    mergeData = new MergeData(strKey, (Tier) m_tierList.get(i), nIndex, latch);
                    mergeThread = new Thread(mergeData);
                    mergeList.put(mergeThread, mergeData);
                }
            }
        }

        //Start all download thread
        for (Thread mergeT : mergeList.keySet()) {
            mergeT.start();
        }

        //Wait the minimum number of thread done their jobs
        try {
            System.out.println("Wait other threads from main");
            latch.await(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.out.println("Fail to wait the downloads threads.");
        }

        int nShardSize = 0;
        int nCnt = 0;
        boolean bRegenerate = false;

        for (int i = 0; i < nTotalShard; i++) {
            if (shards[i] != null) {
                //Get Shard Size
                if (nShardSize < shards[i].length) {
                    nShardSize = shards[i].length;
                }

                shardPresent[i] = true;
                nCnt++;
            } else        // To pass the check EC
            {
                if (bRegenerate == false && i < m_nDataShardCnt) {
                    //Need to re-generate missing shards
                    bRegenerate = true;
                }

                shards[i] = new byte[nShardSize];
            }
        }

        System.out.printf("Received shard count:%d\n", nCnt);

        if (bRegenerate == true) {
            try {
                //ReedSolomon reedSolomon = ReedSolomon.create(m_nDataShardCnt, m_nParityShardCnt);

                // Use Reed-Solomon to fill in the missing shards
                long start_time = System.currentTimeMillis();
                //reedSolomon.decodeMissing(shards, shardPresent, 0, nShardSize);
                long end_time = System.currentTimeMillis();
                long elapsed = end_time - start_time;
                System.out.printf("Elapsed for decoding (%d, %d) :%d ms\n", m_nDataShardCnt, m_nParityShardCnt, elapsed);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // Combine the data shards into one buffer for convenience.
        // (This is not efficient, but it is convenient.)
        byte[] value = new byte[nShardSize * m_nDataShardCnt];

        for (int i = 0; i < m_nDataShardCnt; i++) {
            System.arraycopy(shards[i], 0, value, nShardSize * i, nShardSize);
        }

        lock.readLock().unlock();

        return true;
    }
	/*
		//Wait all - should be no much overhead for waiting each instance
		for (Thread mergeT : mergeList.keySet())
		{
			try
			{
				mergeT.join();
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}
		}

		int nSize=0;

		//Get whole size
		for (MergeData data : mergeList.values())
		{
			nSize += data.m_value.length;
		}

		byte[] value = new byte[nSize];
		int nCurSize = 0;

		for (MergeData data : mergeList.values())
		{
			if(data.m_value.length > 0)
			{
				System.arraycopy(data.m_value, 0, value, nCurSize, data.m_value.length);
				nCurSize += data.m_value.length;
			}
		}*/

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {

    }
}