package umn.dcsg.wieralocalserver.storageinterfaces;

import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created with IntelliJ IDEA. User: ajay Date: 21/02/13 Time: 11:33 PM To
 * change this template use File | Settings | File Templates.
 */

// We can associate action events with not just generic actions
// but also with actions on particular 

public abstract class StorageInterface {
    private AtomicInteger putCount = null;
    private AtomicInteger getCount = null;

    private ReentrantReadWriteLock lck = null;
    private boolean active = false;

    StorageInterface() {
        lck = new ReentrantReadWriteLock();
        putCount = new AtomicInteger(0);
        getCount = new AtomicInteger(0);
        active = true;
    }

    public boolean doPut(String key, byte[] value) {
        boolean ret = false;
        try {
            lck.readLock().lock();
            if (active) {
                putCount.incrementAndGet();
                ret = put(key, value);
            }
        } finally {
            lck.readLock().unlock();
        }
        return ret;
    }


    public byte[] doGet(String key) {
        byte[] ret = null;
        try {
            lck.readLock().lock();
            if (active) {
                getCount.incrementAndGet();
                ret = get(key);
            }
        } finally {
            lck.readLock().unlock();
        }
        return ret;
    }

    public boolean doDelete(String key) {
        boolean ret = false;
        try {
            lck.readLock().lock();
            if (active) {
                ret = delete(key);
            }
        } finally {
            lck.readLock().unlock();
        }
        return ret;
    }

    public boolean doGrowTier(int byPercent) {
        boolean res = false;
        try {
            lck.writeLock().lock();
            res = growTier(byPercent);
        } finally {
            lck.writeLock().unlock();
        }
        return res;
    }

    public boolean doShrinkTier(int byPercent) {
        boolean res = false;
        try {
            lck.writeLock().lock();
            res = shrinkTier(byPercent);
        } finally {
            lck.writeLock().unlock();
        }
        return res;
    }

    public void disable() {
        lck.writeLock().lock();
        active = false;
        lck.writeLock().unlock();
    }

    public void enable() {
        lck.writeLock().lock();
        active = false;
        lck.writeLock().unlock();
    }

    protected abstract boolean put(String key, byte[] value);

    protected abstract byte[] get(String key);

    protected abstract boolean delete(String key);

    protected abstract boolean growTier(int byPercent);

    protected abstract boolean shrinkTier(int byPercent);

    protected Vector<String> getKeys() {
        return new Vector<String>();
    }
}