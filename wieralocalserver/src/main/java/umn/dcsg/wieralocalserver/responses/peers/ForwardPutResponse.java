package umn.dcsg.wieralocalserver.responses.peers;

import org.apache.commons.codec.binary.Base64;
import org.apache.thrift.TException;
import org.json.JSONObject;
import umn.dcsg.wieralocalserver.Constants;
import umn.dcsg.wieralocalserver.LocalServer;
import umn.dcsg.wieralocalserver.Locale;
import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.responses.Response;
import umn.dcsg.wieralocalserver.thriftinterfaces.LocalInstanceToPeerIface;

import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by Kwangsung on 7/26/2017.
 */
public class ForwardPutResponse extends PeerResponse {
    public ForwardPutResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
    }

    @Override
    public boolean doCheckPeerResponseConditions(Map<String, Object> responseParams) {
        return true;
    }

    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(KEY);
        m_lstRequiredParams.add(VALUE);
        m_lstRequiredParams.add(TARGET_LOCALE);
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        String strReason = NOT_HANDLED  + " in " + getClass().getSimpleName();
        boolean bRet = false;
        String strKey = (String) responseParams.get(KEY);
        byte[] value = (byte[]) responseParams.get(VALUE);

        Locale targetLocale = (Locale) responseParams.get(TARGET_LOCALE);
        LocalInstanceToPeerIface.Client peerClient = getPeerClient(targetLocale.getHostName());

        try {
            if (peerClient != null) {
                JSONObject req = new JSONObject();
                req.put(KEY, strKey);
                req.put(VALUE, Base64.encodeBase64String(value));
                req.put(FROM, LocalServer.getHostName()); //Need to set for metrics

                if (responseParams.containsKey(TAG)) {
                    req.put(TAG, responseParams.get(TAG));
                }

                String strReq = req.toString();
                String strResponse = peerClient.forwardPutRequest(strReq);

                if (strResponse.length() > 0) {
                    JSONObject response = new JSONObject(strResponse);

                    bRet = (boolean) response.get(RESULT);

                    if (bRet == false) {
                        strReason = (String) response.get(Constants.REASON);
                    }
                } else {
                    strReason = "This should not happen as this line is not reachable. Target Hostname: " + targetLocale.getHostName();
                }
            } else {
                strReason = "Failed to find a instance to forward a request. Target Hostname: " + targetLocale.getHostName();
            }
        } catch (TException e) {
            e.printStackTrace();
            strReason = e.getMessage();
        } finally {
            //Should not forget.
            //Maybe we can use thrift client pool but for now.
            releasePeerClient(targetLocale.getHostName(), peerClient);
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
        if(responseParams.containsKey(TARGET_LOCALE) == false) {
            responseParams.put(TARGET_LOCALE, Locale.getLocalesWithoutTierName(m_peerInstanceManager.getPrimaryPeerHostname()));
        }
    }
}