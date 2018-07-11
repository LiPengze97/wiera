package umn.dcsg.wieralocalserver.responses;

import umn.dcsg.wieralocalserver.LocalServer;
import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.MetaObjectInfo;

import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by ajay on 11/13/12.
 */
public class DeleteResponse extends Response {
    public DeleteResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
    }

    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(KEY);
        m_lstRequiredParams.add(TIER_NAME);
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        boolean bRet;

        String strKey = (String) responseParams.get(KEY);
        String strTierName = (String) responseParams.get(TIER_NAME);
        MetaObjectInfo obj = m_localInstance.getMetadata(strKey);
        ReentrantReadWriteLock lock = m_localInstance.m_keyLocker.getLock(strKey);
        try {
            lock.writeLock().lock();
            m_localInstance.deleteInternal(obj, strTierName);
            obj.removeLocale(LocalServer.getHostName(), strTierName);
/*          obj.removeLevel(level);

            if (obj.getLocale() == null)
            {
				//Move to slowest tier.
                // Object is not stored on any level and can be removed
                // from the metadata store
				//need to decidd how to handle deleteObject strKey
				// m_instance.m_metadataStore.deleteObject(obj.m_key);
            }*/

            //Result
            bRet = true;
        } catch (Exception e) {
            bRet = false;
            responseParams.put(REASON, e.getMessage());
        } finally {
            lock.writeLock().unlock();
        }

        responseParams.put(RESULT, bRet);
        return bRet;
    }

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {

    }

    @Override
    public boolean doCheckResponseConditions(Map<String, Object> responseParams) {
        return true;
    }
}