package umn.dcsg.wieralocalserver.responses;

import umn.dcsg.wieralocalserver.LocalInstance;

import javax.crypto.Cipher;

import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by ajay on 7/13/13.
 */
public class EncryptResponse extends Response {
    public EncryptResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
    }

    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(VALUE);
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        boolean bRet;
        byte[] value = (byte[]) responseParams.get(VALUE);

        try {
            Cipher cipher = Cipher.getInstance(m_localInstance.encryptionAlgorithm);
            cipher.init(Cipher.ENCRYPT_MODE, m_localInstance.encryptionKey);
            value = cipher.doFinal(value);

            //Result
            bRet = true;
            responseParams.put(VALUE, value);
        } catch (Exception e) {
            bRet = false;
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