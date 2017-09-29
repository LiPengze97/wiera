package umn.dcsg.wieralocalserver.responses;


import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.utils.Utils;

import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by ajay on 7/13/13.
 */

public class GrowResponse extends Response {
    public GrowResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
    }

    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(TIER_NAME);
        m_lstRequiredParams.add(PERCENT);
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        System.out.println("[debug] In growing response");
        boolean bRet = false;
        try {
            String strTierName = (String) responseParams.get(TIER_NAME);
            Integer byPercent = Utils.convertToInteger(responseParams.get(PERCENT));

            //Result
            if (m_localInstance.m_tiers.getTierInterface(strTierName).doGrowTier(byPercent) == false) {
                responseParams.put(RESULT, false);
                responseParams.put(REASON, "Failed to update grow tier space.");
            }
            else {
                long lCurrentSpace = m_localInstance.m_tiers.getTier(strTierName).growSpaceByPercent(byPercent);
                System.out.println("[debug] grow tier " + strTierName + " by " + byPercent + "% to " + lCurrentSpace);
                bRet = true;
            }
        }catch(Exception e) {
            e.printStackTrace();
            responseParams.put(REASON, e.getMessage());
        }

        responseParams.put(RESULT, bRet);
        return bRet;
    }

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {
        if(responseParams.containsKey(TIER_NAME) == false) {
            responseParams.put(TIER_NAME, m_initParams.get(TIER_NAME));
        }

        if(responseParams.containsKey(PERCENT) == false) {
            responseParams.put(PERCENT, m_initParams.get(PERCENT));
        }
    }
}