package umn.dcsg.wieralocalserver;

import umn.dcsg.wieralocalserver.utils.Utils;
import org.json.JSONObject;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by Kwangsung on 11/8/2016.
 */
public class TierInfo {
	public enum TIER_TYPE {
		MEMORY(0),
		SSD(1),
		HDD(2),
		CLOUD_STORAGE(3),
		CLOUD_ARCHIVAL(4),
		WIERA_INSTANCE(5),
		REMOTE_TIER(99),        //Only used for cluster mode and updated by peer
		UNKNOWN(999);

		private final int m_tierType;

		TIER_TYPE(final int newTierType) {
			m_tierType = newTierType;
		}

		public int getTierType() {
			return m_tierType;
		}
	}

	public enum STORAGE_PROVIDER {
		S3(0),
		AS(1),
		GS(2);

		private final int m_type;

		STORAGE_PROVIDER(final int service) {
			m_type = service;
		}

		public int getType() {
			return m_type;
		}
	}

	String m_strTierName;
	TIER_TYPE m_tierType;
	long m_lAllocatedSize;
	long m_lUsedSpace;
	long m_lExpectedLatency; //in ms
    boolean m_bDefault;

	TierInfo(JSONObject storageInfo) {
		//Need to fix
		m_tierType = TierInfo.TIER_TYPE.values()[storageInfo.getInt(TIER_TYPE)];
		m_strTierName = storageInfo.getString(TIER_NAME);

		//Set available size for each storage
		//Default size if not set 134217728 for now
		m_lAllocatedSize = Utils.getSizeFromHumanReadable(storageInfo.get(TIER_SIZE), 134217728);
		m_lExpectedLatency = storageInfo.getLong(TIER_EXPECTED_LATENCY);
        m_bDefault = storageInfo.getBoolean(DEFAULT);
	}

	public String getTierName() {
		return m_strTierName;
	}

	public long getAllocatedSpace() {
		return m_lAllocatedSize;
	}

	public long getExpectedLatency() {
		return m_lExpectedLatency;
	}

	public long getFreeSpace() {
		return m_lAllocatedSize - m_lUsedSpace;
	}

	public long getUsedSpace() {
		return m_lUsedSpace;
	}

	public double getUsedRate() {
		return (double)m_lUsedSpace / (double)m_lAllocatedSize;
	}

	public TIER_TYPE getTierType() {
		return m_tierType;
	}

	public void useSpace(long lSize) {
		m_lUsedSpace += lSize;

		//Debug code in case used space is used more than assigned
		if (m_lUsedSpace > m_lAllocatedSize) {
			Thread.dumpStack();
		}
	}

	public void freeSpace(long lSize) {
		m_lUsedSpace -= lSize;

		//Debug code in case used space is less than 0
		if (m_lUsedSpace < 0) {
			Thread.dumpStack();
		}
	}

	public boolean isDefault() {
        return m_bDefault;
    }

	public long growTierByPercent(int byPercent){
		m_lAllocatedSize = (int) (m_lAllocatedSize * (1 + byPercent / 100.0));
		return m_lAllocatedSize;
	}

	public long growTierBySize(int bySize){
		m_lAllocatedSize = (int) m_lAllocatedSize + bySize;
		return m_lAllocatedSize;
	}
}