package umn.dcsg.wieralocalserver;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by ajay on 12/22/13.
 */
public class KeyLocker {
    private ConcurrentHashMap<String, ReentrantReadWriteLock> lockList = null;

    public KeyLocker() {
        lockList = new ConcurrentHashMap<>(50000, 0.75f, 16);
    }

    public ReentrantReadWriteLock getLock(String strKey) {
        if (!lockList.containsKey(strKey)) {
            lockList.put(strKey, new ReentrantReadWriteLock());
        }
        return lockList.get(strKey);
    }

    void deleteLock(String key) {
        if (lockList.containsKey(key)) {
            lockList.remove(key);
        }
    }
}