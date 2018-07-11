package umn.dcsg.wieralocalserver.storageinterfaces;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.KetamaConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

/**
 * Created with IntelliJ IDEA.
 * User: ajay
 * Date: 21/02/13
 * Time: 11:36 PM
 * To change this template use File | Settings | File Templates.
 */
public class MemcachedInterface extends StorageInterface {
    private MemcachedClient mcClient = null;
    private String serversAdded = null;

    public MemcachedInterface(String ServerList) {
        try {
            mcClient = new MemcachedClient(new KetamaConnectionFactory(),
                    AddrUtil.getAddresses(ServerList));
            serversAdded = ServerList;
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public boolean put(String key, byte[] value) {
//	System.out.println("Test putObject into memcached");
        boolean retVal = false;
        OperationFuture<Boolean> fut = mcClient.set(key, 0, value);

        try {
            retVal = fut.get(5, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            fut.cancel(true);
            System.out.println("Failed to putObject object from Memcached");
            e.printStackTrace();
        }
        return retVal;
    }

    public byte[] get(String key) {
        byte[] value = null;
        Future<Object> fut = mcClient.asyncGet(key);
        try {
            value = (byte[]) fut.get(5, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            fut.cancel(true);
            System.out.println("Failed to getObject object from Memcached");
            e.printStackTrace();
            value = null;
        }
        return value;
    }

    @Override
    public boolean rename(String oldKey, String newKey) {
        return false;
    }

    @Override
    public boolean copy(String oldKey, String newKey) {
        return false;
    }

    public boolean delete(String key) {
        Boolean result = false;
        Future<Boolean> fut = mcClient.delete(key);
        try {
            result = fut.get(5, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            fut.cancel(true);
            System.out.println("Failed to deleteObject object in Memcached");
            e.printStackTrace();
        }

        return result;
    }

    @Override
    public boolean growTier(int byPercent) {
        String AddServer = findServerToAdd(byPercent);
        StringBuilder newServersAdded = new StringBuilder(serversAdded);
        newServersAdded.append(" ").append(AddServer);
        serversAdded = newServersAdded.toString();
        try {
            if (mcClient != null) {
                mcClient.shutdown();
            }
            mcClient = new MemcachedClient(new KetamaConnectionFactory(),
                    AddrUtil.getAddresses(serversAdded));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return true;
    }

    private String findServerToAdd(int byPercent) {
        return "";
    }

    @Override
    public boolean shrinkTier(int byPercent) {
        String RemoveServer = findServerToRemove(byPercent);
        int sIdx = serversAdded.indexOf(RemoveServer);
        int eIdx = sIdx + RemoveServer.length();
        StringBuilder newServersAdded = new StringBuilder(serversAdded.substring(0, sIdx));
        if (eIdx + 1 < serversAdded.length()) {
            newServersAdded.append(serversAdded.substring(eIdx + 1));
        }

        serversAdded = newServersAdded.toString();
        try {
            if (mcClient != null) {
                mcClient.shutdown();
            }
            mcClient = new MemcachedClient(new KetamaConnectionFactory(),
                    AddrUtil.getAddresses(serversAdded));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return true;
    }

    private String findServerToRemove(int byPercent) {
        return "";
    }
}