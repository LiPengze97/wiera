package umn.dcsg.wieralocalserver.responses;

import umn.dcsg.wieralocalserver.LocalInstance;

import javax.crypto.Cipher;
import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.REASON;
import static umn.dcsg.wieralocalserver.Constants.RESULT;
import static umn.dcsg.wieralocalserver.Constants.VALUE;

/**
 * Created by ajay on 7/13/13.
 */
public class DecryptResponse extends Response {
    public DecryptResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
    }

    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(VALUE);
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        boolean bRet;

        try {
            Cipher cipher = Cipher.getInstance(m_localInstance.encryptionAlgorithm);
            cipher.init(Cipher.DECRYPT_MODE, m_localInstance.encryptionKey);
            byte[] encryptedValue = (byte[]) responseParams.get(VALUE);
            encryptedValue = cipher.doFinal(encryptedValue);

            //Result
            responseParams.put(VALUE, encryptedValue);
            bRet = true;

        } catch (Exception e) {
            responseParams.put(REASON, e.getMessage());
            bRet = false;
        }

        responseParams.put(RESULT, bRet);
        return bRet;
    }

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {

    }
}