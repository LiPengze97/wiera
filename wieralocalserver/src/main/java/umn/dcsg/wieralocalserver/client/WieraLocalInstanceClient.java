package umn.dcsg.wieralocalserver.client;



import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.json.JSONArray;
import org.json.JSONObject;
import umn.dcsg.wieralocalserver.*;
import umn.dcsg.wieralocalserver.responses.SplitDataResponse;
import umn.dcsg.wieralocalserver.thriftinterfaces.ApplicationToLocalInstanceIface;
import umn.dcsg.wieralocalserver.thriftinterfaces.ApplicationToWieraIface;
import umn.dcsg.wieralocalserver.utils.Utils;

import java.io.*;
import java.nio.ByteBuffer;


import static umn.dcsg.wieralocalserver.Constants.*;

public class WieraLocalInstanceClient {
    protected ApplicationToLocalInstanceIface.Client wieraLocalInstanceClient = null;

    public WieraLocalInstanceClient(String strIPAddress, int nPort){
        TTransport transport;

        transport = new TSocket(strIPAddress, nPort);
        TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport, 1048576 * 200));
        ApplicationToLocalInstanceIface.Client client = null;

        try {
            transport.open();
            client = new ApplicationToLocalInstanceIface.Client(protocol);
        } catch (TException x) {
            x.printStackTrace();
        }

        this.wieraLocalInstanceClient = client;
    
    
    }
    public boolean set(String key, ByteBuffer value){
        boolean bRet = false;
        if(wieraLocalInstanceClient == null){
            return false;
        }
        try {
            bRet = wieraLocalInstanceClient.put(key, value);
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
        return bRet;
    }

    public ByteBuffer get(String key){
        if(wieraLocalInstanceClient == null){
            return null;
        }
        ByteBuffer value = null;
        try {
            value = wieraLocalInstanceClient.get(key);//ByteBuffer.allocate(1);
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
        return value;
    }

    public ByteBuffer get(String key, int nVersion){
        if(wieraLocalInstanceClient == null){
            return null;
        }
        ByteBuffer value = null;
        try {  
            value = wieraLocalInstanceClient.getVersion(key, nVersion);//ByteBuffer.allocate(1);
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
        return value;
    }
}
