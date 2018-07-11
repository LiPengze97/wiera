package umn.dcsg.wieralocalserver.responses.central;;

import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.responses.Response;

import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by Kwangsung on 7/26/2017.
 */
public class UnLockGlobalResponse extends Response {
    public UnLockGlobalResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
    }

    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(GLOBAL_LOCK);
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        boolean bRet;
        InterProcessMutex gLock = (InterProcessMutex) responseParams.get(GLOBAL_LOCK);

        try {
            gLock.release();
            bRet = true;
        } catch (Exception e) {
            bRet = false;
            e.printStackTrace();
            responseParams.put(REASON, e.getMessage());
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