package umn.dcsg.wieralocalserver.responses.instoragecomputing;

import umn.dcsg.wieralocalserver.LocalInstance;

/**
 * Created by Kwangsung on 2/4/2016.
 */
// A response only needs to know how to respond
public abstract class InStorageComputing {
	LocalInstance m_instance = null;

	InStorageComputing(LocalInstance Instance)
	{
		m_instance = Instance;
	}

	public abstract byte[] doComputing(Object... args);
}