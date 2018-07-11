package umn.dcsg.wieralocalserver.responses;


import umn.dcsg.wieralocalserver.LocalInstance;

import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by ajay on 7/13/13.
 */
public class ShrinkResponse extends Response {
    public ShrinkResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
    }

    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(TIER_NAME);
        m_lstRequiredParams.add(PERCENT);
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        boolean bRet;

        String strTierName = (String) responseParams.get(TIER_NAME);
        Integer byPercent = (Integer) responseParams.get(PERCENT);

        bRet = m_localInstance.m_tiers.getTierInterface(strTierName).doShrinkTier(byPercent);

        //Result
        responseParams.put(RESULT, bRet);
        if (bRet == false) {
            responseParams.put(REASON, "Failed shrink storage: " + strTierName);
        }

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