package umn.dcsg.wieralocalserver.responses;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.LocalServer;

import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by Kwangsung on 7/26/2017.
 */
public class LockGlobalWriteResponse extends Response {
    String m_strGlobalLockPath;
    CuratorFramework m_zkClient;

    public LockGlobalWriteResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
        m_strGlobalLockPath = "/wiera/" + m_localInstance.getPolicyID() + "/";
        m_zkClient = LocalServer.getCuratorFramework();
    }

    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(KEY);
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        boolean bRet;

        try {
            String strKey = (String) responseParams.get(KEY);

            //Remove dual separators
            //From fusepy / is comming at the begging
            String strLockKey = m_strGlobalLockPath + strKey;
            strLockKey = strLockKey.replaceAll("//", "/");

            InterProcessReadWriteLock lockEntry = new InterProcessReadWriteLock(m_zkClient, strLockKey);

            bRet = true;
            lockEntry.writeLock().acquire();
            responseParams.put(GLOBAL_LOCK, lockEntry.writeLock());
        } catch (Exception e) {
            bRet = false;
            responseParams.put(REASON, e.getMessage());
            e.printStackTrace();
        }

        responseParams.put(RESULT, bRet);
        return bRet;
    }

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {

    }
}