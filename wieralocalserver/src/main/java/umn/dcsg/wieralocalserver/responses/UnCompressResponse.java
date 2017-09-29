package umn.dcsg.wieralocalserver.responses;

import umn.dcsg.wieralocalserver.LocalInstance;

import java.io.*;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by ajay on 6/22/13.
 */

public class UnCompressResponse extends Response {
    public UnCompressResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
        super(instance, strEventName, params);
    }

    public static boolean isCompressed(final byte[] compressed) {
        return (compressed[0] == (byte) (GZIPInputStream.GZIP_MAGIC)) && (compressed[1] == (byte) (GZIPInputStream.GZIP_MAGIC >> 8));
    }

    @Override
    protected void InitRequiredParams() {
        m_lstRequiredParams.add(VALUE);
    }

    @Override
    public boolean respond(Map<String, Object> responseParams) {
        boolean bRet = false;
        String strReason = "";
        byte[] value;

        try {
            byte[] compressed = (byte[]) responseParams.get(VALUE);

            //Check data is compressed
            if ((compressed[0] == (byte) (GZIPInputStream.GZIP_MAGIC)) && (compressed[1] == (byte) (GZIPInputStream.GZIP_MAGIC >> 8))) {
                GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(compressed));
                BufferedReader bf = new BufferedReader(new InputStreamReader(gis, "UTF-8"));

                String line;
                StringBuilder newValue = new StringBuilder();

                while ((line = bf.readLine()) != null) {
                    newValue.append(line);
                }
                bf.close();
                gis.close();

                value = newValue.toString().getBytes();
                //Success
                bRet = true;
                responseParams.put(VALUE, value);
            }
        } catch (UnsupportedEncodingException e) {
            bRet = false;
            strReason = e.getMessage();
        } catch (Exception e) {
            bRet = false;
            strReason = e.getMessage();
        }

        responseParams.put(RESULT, bRet);
        //Failed Result
        if (bRet == false) {
            responseParams.put(REASON, strReason);
        }

        return bRet;
    }

    @Override
    public void doPrepareResponseParams(Map<String, Object> responseParams) {

    }
}