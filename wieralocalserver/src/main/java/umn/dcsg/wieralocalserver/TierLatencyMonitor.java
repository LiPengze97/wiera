package umn.dcsg.wieralocalserver;

import umn.dcsg.wieralocalserver.info.Latency;
import umn.dcsg.wieralocalserver.storageinterfaces.StorageInterface;
import umn.dcsg.wieralocalserver.utils.Utils;

import java.util.ArrayList;
import java.util.Arrays;

import static umn.dcsg.wieralocalserver.Constants.DATA_SIZE;

/**
 * Created by Kwangsung on 12/25/2016.
 */
//Running in background and check latency if tier is idle (not used)
public class TierLatencyMonitor {
	Metrics m_statInfo;
	Thread m_monitoringThread;
	boolean m_bStop;

	TierLatencyMonitor(Metrics statInfo) {
		m_statInfo = statInfo;
	}

	public void startMonitoring() {
		m_bStop = false;
		m_monitoringThread = new Thread(new Monitoring());
		m_monitoringThread.start();
	}

	public void stopMonitoring() {
		m_bStop = true;
	}

	public class Monitoring implements Runnable {
		String strRandomKey = "monitoring_key";

		public void run() {
			//List of tier in this instance;
			//check its latency in every period configurable.
			String strTierName;

			ArrayList<String> tierList = m_statInfo.getLocalTierList();
			int nTierCnt = tierList.size();
			long lLastPutOpTime = 0;
			long lLastGetOpTime = 0;
			long lCurTime = 0;
			byte[] dummyData = Utils.createDummyData(DATA_SIZE);
			byte[] received;

			Latency putLatency;
			Latency getLatency;

			while (m_bStop == false) {
				for (int i = 0; i < nTierCnt; i++) {
					strTierName = tierList.get(i);

					//Put and Get test for each tier if not access a while (period set)
					lLastPutOpTime = m_statInfo.getLatestOperationTime(strTierName, Constants.PUT_LATENCY);
					lLastGetOpTime = m_statInfo.getLatestOperationTime(strTierName, Constants.GET_LATENCY);
					lCurTime = System.currentTimeMillis();

					//Set to 3 seconds for now.
					if (lCurTime - lLastPutOpTime > 3000 || lCurTime - lLastGetOpTime > 3000) {
						StorageInterface inter = m_statInfo.m_localInstance.m_tiers.getTier(strTierName).getInterface();

						//Put latency
						putLatency = new Latency();
						putLatency.start();
						inter.doPut(strRandomKey, dummyData);

						putLatency.stop();
						m_statInfo.addTierLatency(strTierName, Constants.PUT_LATENCY, putLatency);

						//Get latency
						getLatency = new Latency();
						getLatency.start();
						received = inter.doGet(strRandomKey);

						getLatency.stop();
						m_statInfo.addTierLatency(strTierName, Constants.GET_LATENCY, getLatency);

						if (Arrays.equals(dummyData, received) == false) {
							System.out.println("Data does not equal in monitoring.");
						} else {
//:							System.out.printf("[debug]Measured tier latency %s - Put: %d ms, Get: %d\n", strTierName, putLatency.getLatencyInMills(), getLatencyInMills.getLatencyInMills());
						}
					}
				}

				//Wait 3 secs
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
}