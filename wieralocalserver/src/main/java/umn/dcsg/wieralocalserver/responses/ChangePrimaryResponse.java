package umn.dcsg.wieralocalserver.responses;

import umn.dcsg.wieralocalserver.Constants;
import umn.dcsg.wieralocalserver.LocalInstance;
import org.apache.thrift.TException;
import org.json.JSONObject;

import java.util.Map;
import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by Kwangsung on 12/21/2015.
 */
public class ChangePrimaryResponse extends Response {
    public ChangePrimaryResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
    }

    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(PRIMARY);
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        boolean bRet;
        String strReason = NOT_HANDLED;

        try {
            String strNewPrimaryInstance = (String) responseParams.get(PRIMARY);
            JSONObject req = new JSONObject();

            req.put(ID, m_localInstance.getPolicyID());
            req.put(Constants.PRIMARY, strNewPrimaryInstance);

            m_localInstance.getWieraClient().requestPolicyChange(req.toString());
            bRet = true;
        } catch (TException e) {
            bRet = false;
            strReason = e.getMessage();
        } finally {
            m_localInstance.releaseWieraClient();
        }

        //Result
        responseParams.put(RESULT, bRet);
        if (bRet == false) {
            responseParams.put(REASON, strReason);
        }

        return bRet;
    }

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {

    }
}