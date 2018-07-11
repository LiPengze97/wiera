package umn.dcsg.wieralocalserver.responses;

import umn.dcsg.wieralocalserver.LocalInstance;

import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by ajay on 3/22/14.
 */
public class CompressResponse extends Response {
    public CompressResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
    }

    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(VALUE);
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        boolean bRet = false;

        try {
            byte[] value = (byte[]) responseParams.get(VALUE);

            //Check alrealy compressed
            if ((value[0] != (byte) (GZIPInputStream.GZIP_MAGIC)) || (value[1] != (byte) (GZIPInputStream.GZIP_MAGIC >> 8)))
            {
                ByteArrayOutputStream oos = new ByteArrayOutputStream();
                GZIPOutputStream gzip = new GZIPOutputStream(oos);
                gzip.write(value);
                gzip.flush();
                gzip.close();

                byte[] newValue = oos.toByteArray();

                //Replace value for next response
                responseParams.put(VALUE, newValue);
                bRet = true;
            }
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

    @Override
    public boolean doCheckResponseConditions(Map<String, Object> responseParams) {
        return true;
    }
}