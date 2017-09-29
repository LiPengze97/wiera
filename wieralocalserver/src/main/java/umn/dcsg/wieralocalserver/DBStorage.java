package umn.dcsg.wieralocalserver;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
/**
 * Created with IntelliJ IDEA. User: ajay Date: 29/03/13 Time: 12:42 PM To
 * change this template use File | Settings | File Templates.
 *
 * This tracks a single object created and moved around LocalInstance.
 */
//Store Key and data location with version. -- ks

@Entity
public class DBStorage {
	//For now this class will not be used.
	@PrimaryKey
	String m_strStorageName = null;

	long m_lAssignedSpace = 0;
	long m_lUsedSpace = 0;

	DBStorage(long lAvailableSize) {
		m_lAssignedSpace = lAvailableSize;
	}

	void addObjectSize(long lSize) {
		m_lUsedSpace += lSize;
	}

	boolean checkFreeSpace(long lAddedSize) {
		return (m_lUsedSpace + lAddedSize) <= m_lAssignedSpace;

	}
}