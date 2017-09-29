package umn.dcsg.wieralocalserver.info;

/**
 * Created by Kwangsung on 11/8/2016.
 */
public class Latency {
	private long m_lStartDate = 0;
	private long m_lEndDate = 0;
	private long m_lStartTime = 0;
	private long m_lEndTime = 0;
	private double m_dbAdjustTime = 0;

	private boolean m_bInit = false;
	private boolean m_bDone = false;

	public Latency() {
		this.m_lStartTime = System.currentTimeMillis();
		this.m_lEndTime = 0;
		m_dbAdjustTime = 0;
	}

	public Latency(long lStartTime, long lEndTime) {
		this.m_lStartTime = lStartTime;
		this.m_lEndTime = lEndTime;
		m_dbAdjustTime = 0;
	}

	//Now it based nano seconds
	public long start() {
		if (m_bInit == true) {
			//Thread.dumpStack();
			System.out.println("Timer has started already.");
			return 0;
		}

		m_bInit = true;
		m_lStartTime = System.nanoTime();
		m_lStartDate = System.currentTimeMillis();
		return m_lStartTime;
	}

	public long stop() {
		if (m_bInit == false) {
			System.out.println("Timer has started yet.");
			return 0;
		}

		m_bInit = false;
		m_lEndTime = System.nanoTime();
		m_lEndDate = System.currentTimeMillis();
		m_bDone = true;

		return m_lEndTime;
	}

	//in nano second
	public void setAdjustTime(double dbAdjustTime) {
		m_dbAdjustTime = dbAdjustTime;
	}

	public long getStartDate() {
		return m_lStartDate;
	}

	public double getLatencyInMills() {
		return ((m_lEndTime - m_lStartTime) - m_dbAdjustTime) / 1000000f;
	}

	public void setStartDate(long lStart) {
		m_lStartTime = lStart;
	}

	public void setEndDate(long lEnd) {
		m_lEndTime = lEnd;
	}

	public long getEndDate() {
		return m_lEndDate;
	}

	public boolean isDone() {
		return m_bDone;
	}
}