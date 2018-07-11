package umn.dcsg.wieralocalserver.responses.peers;

import org.apache.commons.codec.binary.Base64;
import org.apache.thrift.TException;
import org.json.JSONObject;
import umn.dcsg.wieralocalserver.*;
import umn.dcsg.wieralocalserver.thriftinterfaces.LocalInstanceToPeerIface;

import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by Kwangsung on 7/26/2017.
 */
public class ForwardGetResponse extends PeerResponse {
    public ForwardGetResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
    }

    @Override
    public boolean doCheckPeerResponseConditions(Map<String, Object> responseParams) {
        return true;
    }

    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(KEY);
        m_lstRequiredParams.add(TARGET_LOCALE);
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        boolean bRet = false;
        String strReason = NOT_HANDLED  + " in " + getClass().getSimpleName();
        Locale targetLocale = null;
        LocalInstanceToPeerIface.Client peerClient = null;

        try {
            String strKey = (String) responseParams.get(KEY);
            targetLocale = (Locale) responseParams.get(TARGET_LOCALE);
            peerClient = getPeerClient(targetLocale.getHostName());

            if (peerClient != null) {
                JSONObject req = new JSONObject();
                req.put(KEY, strKey);
                req.put(FROM, LocalServer.getHostName()); //Need to set for metrics

                if (responseParams.containsKey(VERSION) == true) {
                    req.put(VERSION, responseParams.get(VERSION));
                }

                if (responseParams.containsKey(TIER_NAME) == true) {
                    req.put(TIER_NAME, responseParams.get(TIER_NAME));
                } else {
                    req.put(TIER_NAME, targetLocale.getTierName());
                }

                String strReq = req.toString();
                String strResponse = peerClient.get(strReq);

                if (strResponse.length() > 0) {
                    JSONObject response = new JSONObject(strResponse);
                    bRet = (boolean) response.get(RESULT);

                    if (bRet == false) {
                        strReason = (String) response.get(REASON);
                    } else {
                        responseParams.put(VALUE, Base64.decodeBase64((String)response.get(VALUE)));
                    }
                } else {
                    strReason = "This should not happen as this line is not reachable. Target Hostname: " + targetLocale.getHostName();
                }
            } else {
                strReason = "Failed to find a instance to forward a request. Target Hostname: " + targetLocale.getHostName();
            }
        }
        catch (TException e) {
            e.printStackTrace();
            strReason = e.getMessage();
        } finally {
            //Should not forget.
            //Maybe we can use thrift client pool but for now.
            if(targetLocale != null && peerClient != null) {
                releasePeerClient(targetLocale.getHostName(), peerClient);
            }
        }

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