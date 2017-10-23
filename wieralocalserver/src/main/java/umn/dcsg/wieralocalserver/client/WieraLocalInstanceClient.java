package umn.dcsg.wieralocalserver.client;



import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import umn.dcsg.wieralocalserver.thriftinterfaces.ApplicationToLocalInstanceIface;

import java.nio.ByteBuffer;


/**
 * WieraLocalInstanceClient is used to store and retrieve the data in a storage instances.
 *
 * <p>User can directly create the object, or obtain an object by the WieraCentralClient.</p>
 * @see WieraCentralClient#getLocalInstance(String, int)
 * */

public class WieraLocalInstanceClient {
    protected ApplicationToLocalInstanceIface.Client wieraLocalInstanceClient = null;
    /**
     * Creates a client object used to manipulate data.
     *
     * @param strIPAddress The IP address of the Wiera central server.
     * @param nPort The port number of the Wiera central server service.
     *
     * */
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
    /**
     * Store this key-value pair to this instance.
     *
     * @param key The key associated with the data. Also used to retrieve the data.
     * @param value The content of the data.
     * @return True if success, otherwise false
     * */
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
    /**
     * Retrieve the data by its key.
     * @param key The key associated with the data.
     * @return The content of data, null if fails.
     * */
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
