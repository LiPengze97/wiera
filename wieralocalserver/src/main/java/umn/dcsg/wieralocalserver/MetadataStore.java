package umn.dcsg.wieralocalserver;

import java.io.File;
import java.io.IOException;
import java.util.*;

//import org.apache.log4j.Logger;

import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;
import com.sleepycat.persist.StoreConfig;

public class MetadataStore {

	private static String defaultEnvHome = "./localInstance.db";

	private static int defaultCachePercent = 60;

	private Environment env = null;

	private EntityStore store = null;

	//Index for object
	private PrimaryIndex<String, MetaObjectInfo> m_objectPIndex = null;
	private SecondaryIndex<String, String, MetaObjectInfo> m_objectSIndexTags = null;
	private SecondaryIndex<Long, String, MetaObjectInfo> m_objectSIndexAccessTime = null;

	//Index for storage (currently used only for storage size)
	private PrimaryIndex<String, DBStorage> m_storagePIndex = null;

	private String envHome = null;

	private int cachePercent = 0;

	MetadataStore() {
		envHome = defaultEnvHome;
		cachePercent = defaultCachePercent;
	}

	MetadataStore(String strWieraID) {
		envHome = "./" + strWieraID + ".db";
		cachePercent = defaultCachePercent;
	}

	MetadataStore(String EnvHome, Integer CachePercent) {
		envHome = EnvHome;
		cachePercent = CachePercent;
	}

	public void init() throws IOException {
		EnvironmentConfig envConfig = new EnvironmentConfig();
		StoreConfig storeConfig = new StoreConfig();
		EnvironmentMutableConfig envMutConfig = new EnvironmentMutableConfig();

		envConfig.setAllowCreate(true);
		storeConfig.setAllowCreate(true);

		File envHomeFile = new File(envHome);
		if (!envHomeFile.exists()) {
			envHomeFile.mkdirs();
		}
		env = new Environment(envHomeFile, envConfig);
		store = new EntityStore(env, "LocalMetaStore", storeConfig);

		// We will no longer use our own read caches, we will rely on BDB's buffers
		envMutConfig.setCachePercent(cachePercent);
		env.setMutableConfig(envMutConfig);

		m_objectPIndex = store.getPrimaryIndex(String.class, MetaObjectInfo.class);
		m_objectSIndexTags = store.getSecondaryIndex(m_objectPIndex, String.class, "tags");
		m_objectSIndexAccessTime = store.getSecondaryIndex(m_objectPIndex, Long.class, "accessTime");

		//m_storagePIndex = store.getPrimaryIndex(String.class, DBStorage.class);
	}

	public void close() {
		//Put metadata into database
		store.sync();

		store.close();
		env.close();
	}

	public void putObject(MetaObjectInfo obj) {
		m_objectPIndex.put(obj);
	}

	// By default this method uses the primary key to retrieve an object
	protected MetaObjectInfo getObject(String key) {
		return m_objectPIndex.get(key);
	}

	public HashMap<Integer, MetaVerInfo> getVersionList(String key) {
		MetaObjectInfo obj = getObject(key);
		return obj.getVersionList();
	}

	public void deleteObject(String key) {
		m_objectPIndex.delete(key);
	}

	public Map<MetaObjectInfo, Vector<Integer>> searchDirtyObject(Locale targetLocale) {
		Map<MetaObjectInfo, Vector<Integer>> keyList = new HashMap<>();

		EntityCursor<MetaObjectInfo> entityCursor = m_objectPIndex.entities();

		try {
			for (MetaObjectInfo obj : entityCursor) {
				//Check whether it is dirty or not

				if (obj.isDirty() == true) {
					if (obj.getLocaleList().containsKey(targetLocale.getLocaleID())) {
						keyList.put(obj, new Vector<>(obj.getVersionList().keySet()));
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			entityCursor.close();
		}

		return keyList;
	}

	public Map<MetaObjectInfo, Vector<Integer>> searchOldestObject(Locale targetLocale) {
		Map<MetaObjectInfo, Vector<Integer>> keyList = new HashMap<>();
		SortedMap<Long, MetaObjectInfo> accessTimeIndex = m_objectSIndexAccessTime.sortedMap();

		for (MetaObjectInfo obj : accessTimeIndex.values()) {
			System.out.println(obj.m_key);
		}

		if (accessTimeIndex.size() > 0) {
			Object[] keySet = accessTimeIndex.keySet().toArray();
			MetaObjectInfo obj;

			for (int i = 0; i < keySet.length; i++) {
				obj = m_objectSIndexAccessTime.get((Long)keySet[i]);

				try {
					if (obj.hasLocale(targetLocale.getLocaleID()) == true) {
						keyList.put(obj, new Vector<>(obj.getVersionList().keySet()));
						break;
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		return keyList;
	}

	public Map<MetaObjectInfo, Vector<Integer>> searchNewestObject(Locale targetLocale, boolean bSecondLast) {
		Map<MetaObjectInfo, Vector<Integer>> keyList = new HashMap<>();
		SortedMap<Long, MetaObjectInfo> accessTimeIndex = m_objectSIndexAccessTime.sortedMap();

		for (MetaObjectInfo obj : accessTimeIndex.values()) {
			System.out.println(obj.m_key);
		}

		if (accessTimeIndex.size() > 0) {
			Object[] keySet = accessTimeIndex.keySet().toArray();
			MetaObjectInfo obj;
			int nStartFrom = 1;

			if(bSecondLast == true) {
				nStartFrom = 2;
			}

			//Find second latest one
			for (int i = keySet.length-nStartFrom; i >= 0; i--) {
				obj = m_objectSIndexAccessTime.get((Long)keySet[i]);

				try {
					if (obj.hasLocale(targetLocale.getLocaleID()) == true) {
						keyList.put(obj, new Vector<>(obj.getVersionList().keySet()));
						break;
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		return keyList;
	}

	public Map<MetaObjectInfo, Vector<Integer>> searchAllObject(Locale targetLocale) {
		Map<MetaObjectInfo, Vector<Integer>> keyList = new HashMap<>();
		EntityCursor<MetaObjectInfo> entityCursor = m_objectPIndex.entities();

		try {
			for (MetaObjectInfo obj : entityCursor) {
				if (obj.hasLocale(targetLocale.getLocaleID()) == true) {
					keyList.put(obj, new Vector<>(obj.getVersionList().keySet()));
				}
			}
		} finally {
			entityCursor.close();
		}

		return keyList;
	}

	public Map<MetaObjectInfo, Vector<Integer>> searchColdObject(long lThresholdInSec, Locale targetLocale) {
		Map<MetaObjectInfo, Vector<Integer>> keyList = new HashMap<>();
		EntityCursor<MetaObjectInfo> entityCursor = m_objectPIndex.entities();

		long curTime = System.currentTimeMillis();
		long elapsed = 0;
		long lThreshold = lThresholdInSec * 1000;

		try {
			for (MetaObjectInfo obj : entityCursor) {
				elapsed = curTime - obj.getLAT();

				//Check whether it is cold or not
				if (lThreshold < elapsed && obj.hasLocale(targetLocale.getLocaleID()) == true) {
					keyList.put(obj, new Vector<>(obj.getVersionList().keySet()));
				}
			}
		} finally {
			entityCursor.close();
		}

		return keyList;
	}

	public Map<MetaObjectInfo, Vector<Integer>> getWithTags(String strTag) {
		Map<MetaObjectInfo, Vector<Integer>> keyList = new HashMap<>();
		EntityCursor<MetaObjectInfo> entityCursor = m_objectSIndexTags.subIndex(strTag).entities();

		try {
			for (MetaObjectInfo obj : entityCursor) {
				keyList.put(obj, new Vector<>(obj.getVersionList().keySet()));
			}
		} finally {
			entityCursor.close();
		}

		return keyList;
	}

	public void putStorage(DBStorage storage) {
		m_storagePIndex.put(storage);
	}

	// By default this method uses the primary key to retrieve an objec
	public DBStorage getStorage(String key) {
		return m_storagePIndex.get(key);
	}

	public void deleteStorage(String key) {
		m_storagePIndex.delete(key);
	}
}