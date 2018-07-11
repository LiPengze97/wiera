package umn.dcsg.wieralocalserver.responses;

import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.utils.Utils;

import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.REASON;
import static umn.dcsg.wieralocalserver.Constants.RESULT;
import static umn.dcsg.wieralocalserver.Constants.VERSION;

/**
 * Created by with IntelliJ IDEA.
 * User: Kwangsung
 * Date: 7/5/2018 12:10 PM
 */
public class IncreaseVersionResponse extends Response {
    public IncreaseVersionResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
    }

    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(VERSION);
    }

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {

    }

    @Override
    public boolean doCheckResponseConditions(Map<String, Object> responseParams) {
        return true;
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        int nVer = Utils.convertToInteger(responseParams.get(VERSION));
        nVer++;
        responseParams.put(VERSION, nVer);
        return true;
    }
}
