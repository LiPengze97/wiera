package umn.dcsg.wieralocalserver.responses;

import umn.dcsg.wieralocalserver.LocalInstance;

import java.util.Map;

/**
 * Created by Kwangsung on 1/8/2017.
 */
public class ChangeDataPlacementResponse extends Response {
    long m_lLastTimeUpdated;

    public ChangeDataPlacementResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
    }

    @Override
    protected void InitRequiredParams() {
    }

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {

    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        return false;
    }
}