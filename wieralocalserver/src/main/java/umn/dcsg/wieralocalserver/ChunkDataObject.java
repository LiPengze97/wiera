package umn.dcsg.wieralocalserver;

import java.util.*;

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
public class ChunkDataObject
{
	@PrimaryKey
	String m_key = null;

	ChunkDataObject(String Key)
	{
		//For each key.
		m_key = Key;

		m_peerList = new HashSet<>();
		m_tierList = new HashSet<>();
	}

	public boolean addTier(String strTierName)
	{
		return m_tierList.add(strTierName);
	}

	public boolean addPeer(String strHostName)
	{
		return m_peerList.add(strHostName);
	}

	public Set<String> getTierList()
	{
		return m_tierList;
	}

	public Set<String> getPeerList()
	{
		return m_peerList;
	}

	Set<String> m_peerList; 	//for Wiera
	Set<String> m_tierList;	//for LocalInstance
}