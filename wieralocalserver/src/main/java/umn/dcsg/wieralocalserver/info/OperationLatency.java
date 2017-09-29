package umn.dcsg.wieralocalserver.info;

import java.util.LinkedHashMap;

/**
 * Created by Kwangsung on 11/8/2016.
 */
public class OperationLatency extends Latency {
	private String m_strKey;
	private String m_strLocaleID;
	private String m_strEventName;    //Response?
	LinkedHashMap<String, Latency> m_latencies; //Each response will set the time

	public OperationLatency(String strKey, String strLocaleID, String strEventName) {
		m_latencies = new LinkedHashMap<String, Latency>();

		this.m_strEventName = strEventName;
		this.m_strKey = strKey;
		this.m_strLocaleID = strLocaleID;
	}

	public void updateTierName(String strNewTierName) {
		m_strLocaleID = strNewTierName;
	}

	public Latency addTimer(String strTimerName) {
		int i = 1;
		while (m_latencies.containsKey(strTimerName) == true) {
			strTimerName = String.format("%s-%d", strTimerName, i);
		}

		Latency latency = new Latency(System.currentTimeMillis(), System.currentTimeMillis());
		m_latencies.put(strTimerName, latency);

		return latency;
	}

	public double getOverheadTime() {
		double dbOverhead = getLatencyInMills();

		for (String strTimerName : m_latencies.keySet()) {
			dbOverhead -= m_latencies.get(strTimerName).getLatencyInMills();
		}

		return dbOverhead;
	}

	public void printAllLatencyInfo()
	{
		System.out.printf("Overall time: %f ms\n", getLatencyInMills());
		for (String strTimerName : m_latencies.keySet()) {
			System.out.printf(" - %s, %f ms\n",strTimerName,  m_latencies.get(strTimerName).getLatencyInMills());
		}
	}
}