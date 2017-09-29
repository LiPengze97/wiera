package umn.dcsg.wieralocalserver.responses;

//import com.backblaze.erasure.ReedSolomon;
import umn.dcsg.wieralocalserver.Tier;
import umn.dcsg.wieralocalserver.LocalInstance;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static umn.dcsg.wieralocalserver.Constants.*;
import static umn.dcsg.wieralocalserver.Constants.REASON;

/**
 * Created by ajay on 7/13/13.
 */
/**
 * Created by Kwangsung on 4/1/2016.
 * This class is crated only for testing for now.
 * Will be elaborated.
 */

//May be need to spilt the function split and ec later.
public class SplitDataResponse extends Response {
    class SplitData implements Runnable {
        String m_strKey;
        byte[] m_value;
        Tier m_tierInfo;
        boolean m_bRet = false;

        public SplitData(String strKey, byte[] value, Tier tierInfo) {
            m_strKey = strKey;
            m_value = value;
            m_tierInfo = tierInfo;
        }

        @Override
        public void run() {
            if (m_tierInfo != null) {
                //Timer latency check
                long start_time = System.currentTimeMillis();
                m_bRet = m_tierInfo.getInterface().doPut(m_strKey, m_value);
                long end_time = System.currentTimeMillis();
                double elapsed = end_time - start_time;
            }
        }
    }

    protected List m_tierList = null;
    protected int m_nDataShardCnt;
    protected int m_nParityShardCnt;
    protected int m_nFilterLayer;

    /*public SplitDataResponse(LocalInstance instance, List tierList, int nDataShard, int nParityShard, int nFilterLayer) {
        //super(instance);
        m_tierList = tierList;
        m_nDataShardCnt = nDataShard;
        m_nParityShardCnt = nParityShard;
        m_nFilterLayer = nFilterLayer;
    }*/

    public SplitDataResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
    }


    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(KEY);
        m_lstRequiredParams.add(VALUE);
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        boolean bRet;
        String strReason = NOT_HANDLED;
        ReentrantReadWriteLock lock = null;

        String strKey = (String) responseParams.get(KEY);
        byte[] value = (byte[]) responseParams.get(VALUE);

        try {
            lock = m_localInstance.m_keyLocker.getLock(strKey);
            lock.writeLock().lock();

            //SplitData
            //Erasure Coding? //May need to split to functions (split, ec)
            byte[][] shards = splitData(value, m_nDataShardCnt, m_nParityShardCnt);

            //Assign chunks to nodes based on multiple algorithm.
            //MDS
            int nNodeCnt = 10;
            Map<Thread, SplitData> assignedThreads = assignChunksToTiers(strKey, shards, nNodeCnt);

            //Timer latency check
            //long start_time = System.currentTimeMillis();
            //Need to how to wait
            //Wait all - should be no much overhead for waiting each instance
            for (Thread splitT : assignedThreads.keySet()) {
                splitT.join();
            }

            bRet = true;
        } catch (InterruptedException e) {
            bRet = false;
            strReason = e.getMessage();
        } catch (Exception e) {
            bRet = false;
            strReason = e.getMessage();
        } finally {
            if (lock != null) {
                lock.writeLock().unlock();
            }
        }

        //Result
        responseParams.put(RESULT, bRet);
        if (bRet == false && strReason != null) {
            responseParams.put(REASON, strReason);
        }
        return bRet;
    }

    //Need to rewrite with various algorithm
    //This will be changed to LocalInstance from tier.
    private Map<Thread, SplitData> assignChunksToTiers(String strKey, byte[][] shards, int nNodeCnt) {
        SplitData splitData;
        Thread splitSThread;
        Map<Thread, SplitData> splitList = new HashMap<Thread, SplitData>();
        String chunks_key;
        int nStart = 0;
        int nIndex = 0;
        int nTierCnt = m_tierList.size();

        for (int nFilter = 0; nFilter < m_nFilterLayer; nFilter++) {
            nStart = nTierCnt - (nFilter * 2);

            for (int i = 0; i < shards.length; i++) {
                nIndex = (nStart + i) % nTierCnt;
                chunks_key = String.format("%s_splitted_%d", strKey, nIndex);

                System.out.printf("Store Tier Name: %s, Key: %s\n", ((Tier) m_tierList.get(i)).getTierName(), chunks_key);

                splitData = new SplitData(chunks_key, shards[nIndex], (Tier) m_tierList.get(i));

                //Thread run
                splitSThread = new Thread(splitData);
                splitSThread.start();

                //Add to map
                splitList.put(splitSThread, splitData);
            }
        }

        return splitList;
    }

    public static byte[][] splitData(byte[] value, int nDataShardCnt, int nParityShardCny) {
        int nToalShard = nDataShardCnt + nParityShardCny;
        int nDataSize = value.length;

        //From Backblaze Test file
        // Figure out how big each shard will be.  The total size stored
        // will be the file size (8 bytes) plus the file.
        //final int storedSize = nDataSize + BYTES_IN_INT;
        //int nShardSize = (nDataSize + m_nDataShardCnt - 1) / m_nDataShardCnt;
        int nStart = 0;
        int nEnd = 0;
        int nShardSize = (int) Math.ceil((double) nDataSize / (double) nDataShardCnt);
        int nCurSahrdSize;

        // Make the buffers to hold the shards.
        byte[][] shards = new byte[nToalShard][nShardSize];

        for (int i = 0; i < nDataShardCnt; i++) {
            nEnd = Math.min(nDataSize, nStart + nShardSize);

            try {
                nCurSahrdSize = nEnd - nStart;
                System.out.printf("%d shard Size :%d\n", i, nCurSahrdSize);
                System.arraycopy(value, nStart, shards[i], 0, nCurSahrdSize);
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }

            nStart = nEnd;
        }

        try {
            // Use Reed-Solomon to calculate the parity.
            //ReedSolomon reedSolomon = ReedSolomon.create(nDataShardCnt, nParityShardCny);

            long start_time = System.currentTimeMillis();
            //reedSolomon.encodeParity(shards, 0, nShardSize);
            long end_time = System.currentTimeMillis();
            long elapsed = end_time - start_time;
            System.out.printf("Elapsed for creating EC parities (%d, %d) :%d ms size:%d Chunk Size:%d\n", nDataShardCnt, nParityShardCny, elapsed, nDataSize, nShardSize);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return shards;
    }

/*		List<byte[]> result = new ArrayList<byte[]>();
		int nStart = 0;
		int nEnd = 0;
		byte[] split;
		int nChunkSize = (int) Math.ceil((double) value.length / (double) nChunkCnt);

		for(int i=0;i<nChunkCnt;i++)
		{
			nEnd = Math.min(value.length, nStart + nChunkSize);

			try
			{
				split = Arrays.copyOfRange(value, nStart, nEnd);

				if(split.length > 0)
				{
					result.add(split);
				}

				System.out.format("Split Size : %d\n", split.length);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				break;
			}

			nStart = nEnd;
		}

		return result;
	}*/

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {

    }
}