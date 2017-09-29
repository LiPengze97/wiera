package umn.dcsg.wieralocalserver.responses;

import umn.dcsg.wieralocalserver.Constants;
import umn.dcsg.wieralocalserver.LocalInstance;
import org.apache.thrift.TException;
import org.json.JSONObject;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by Kwangsung on 12/17/2015.
 */
public class ChangeEventResponseResponse extends Response {
    List<String> m_lstEvent = null; //Reposes on which events will be replaced
    List<Response> m_lstResponse = null;
    /**
     * Constructor with response param
     * Response can be generated with policy or other response dynamically
     *
     * @param localInstance LocalInstance localInstance
     * @param strEventName
     * @param params        Repsonse class to handle the request (or event)  @return void
     * @see Response
     */
    public ChangeEventResponseResponse(LocalInstance localInstance, String strEventName, Map<String, Object> params) {
        super(localInstance, strEventName, params);
        m_lstEvent = new LinkedList<>((List)params.get(EVENTS));
        m_lstResponse = new LinkedList((List)params.get(RESPONSES));

        //Map<String, Object> ;
        //response = Response.createResponse(this, strResponseType, strEventName, params);
    }

    @Override
    protected void InitRequiredParams() {
        //Events and responses are pre-determined by constructor
        //m_lstRequiredParams.add(EVENTS);
        //m_lstRequiredParams.add(RESPONSES);
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        boolean bRet;
        String strReason = NOT_HANDLED;

        try {
            JSONObject req = new JSONObject();

            req.put(ID, m_localInstance.getPolicyID());
            req.put(Constants.EVENTS, m_lstEvent);
            req.put(Constants.RESPONSES, m_lstResponse);

            System.out.println("[debug] Send request data distribution change");
            m_localInstance.getWieraClient().requestPolicyChange(req.toString());
            bRet = true;
        } catch (TException e) {
            strReason = e.getMessage();
            bRet = false;
        } finally {
            m_localInstance.releaseWieraClient();
        }

        //Result
        bRet = true;
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