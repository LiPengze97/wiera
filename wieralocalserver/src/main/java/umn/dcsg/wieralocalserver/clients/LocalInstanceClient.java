package umn.dcsg.wieralocalserver.clients;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.codec.binary.Base64;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.json.JSONObject;
import umn.dcsg.wieralocalserver.MetaVerInfo;
import umn.dcsg.wieralocalserver.ThriftClientPool;
import umn.dcsg.wieralocalserver.thriftinterfaces.ApplicationToLocalInstanceIface;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import static umn.dcsg.wieralocalserver.Constants.*;
import static umn.dcsg.wieralocalserver.MetaObjectInfo.LATEST_VERSION;

public class LocalInstanceClient {
    private String m_strHostName;
    private int m_nPort;
    private String m_strIPAddress;
    private boolean m_bConnected = false;
    ThriftClientPool m_clientPool;
    //ApplicationToLocalInstanceIface.Client m_client;

    public LocalInstanceClient(String strHostName, String strIPAddress, int nPort) {
        m_strHostName = strHostName;
        initConnection(strIPAddress, nPort, 1);
    }

    public LocalInstanceClient(String strHostName, String strIPAddress, int nPort, int nPool) {
        m_strHostName = strHostName;
        initConnection(strIPAddress, nPort, nPool);
    }

    private void initConnection(String strIPAddress, int nPort, int nPool) {
        m_strIPAddress = strIPAddress;
        m_nPort = nPort;

        try {
            m_clientPool = new ThriftClientPool(strIPAddress, nPort, nPool, ApplicationToLocalInstanceIface.Client.class);
            m_bConnected = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getHostName() {
        return m_strHostName;
    }

    public boolean isConnected() {
        return m_bConnected;
    }

    public boolean put(String strKey, byte[] value) {
        return put(strKey, ByteBuffer.wrap(value));
    }

    public boolean put(String strKey, String value) {
        return put(strKey, ByteBuffer.wrap(value.getBytes()));
    }

    public boolean put(String strKey, ByteBuffer value) {
        if (m_bConnected == false) {
            return false;
        }

        //Will be blocked if connection is not available
        ApplicationToLocalInstanceIface.Client client = (ApplicationToLocalInstanceIface.Client) m_clientPool.getClient();

        try {
            ByteBuffer result = client.put(strKey, value);
            JSONObject obj = new JSONObject(new String(result.array(), result.position(), result.remaining()));
            boolean bRet = obj.getBoolean(RESULT);

            if (bRet == true) {
                return true;
            } else {
                System.out.println("Failed to put. Reason: " + obj.getString(REASON));
            }
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } finally {
            //Should be called.
            m_clientPool.releasePeerClient(client);
        }

        return false;
    }

    public boolean putTo(String strKey, byte[] value, List<String> targetList, boolean bSync) {
        return putTo(strKey, ByteBuffer.wrap(value),  targetList, bSync);
    }

    public boolean putTo(String strKey, ByteBuffer value, List<String> targetList, boolean bSync) {
        if (m_bConnected == false) {
            return false;
        }

        //Will be blocked if connection is not available
        ApplicationToLocalInstanceIface.Client client = (ApplicationToLocalInstanceIface.Client) m_clientPool.getClient();

        try {
            //false -> async
            //true -> sync
            ByteBuffer result = client.putTo(strKey, value, targetList, bSync);
            JSONObject obj = new JSONObject(new String(result.array(), result.position(), result.remaining()));
            boolean bRet = obj.getBoolean(RESULT);

            if (bRet == true) {
                return true;
            } else {
                System.out.println("Failed to put. Reason: " + obj.getString(REASON));
            }
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } finally {
            //Should be called.
            m_clientPool.releasePeerClient(client);
        }

        return false;
    }

    public byte[] get(String strKey) {
        return get(strKey, LATEST_VERSION);
    }

    public HashMap<String, Object> getMetaData(String strKey) {
        if (m_bConnected == false) {
            return null;
        }

        //Will be blocked if connection is not available
        ApplicationToLocalInstanceIface.Client client = (ApplicationToLocalInstanceIface.Client) m_clientPool.getClient();

        try {
            ByteBuffer result = client.getMetaData(strKey);
            JSONObject obj = new JSONObject(new String(result.array(), result.position(), result.remaining()));
            boolean bRet = obj.getBoolean(RESULT);

            if (bRet == true) {
                Gson gson = new Gson();
                Type type = new TypeToken<HashMap<String, Object>>() {
                }.getType();

                String strMeta = obj.getString(VALUE);
                HashMap<String, Object> meta = gson.fromJson(strMeta, type);
                return meta;
            } else {
                System.out.println("Failed to get meta data. key:" + strKey + ", Reason: " + obj.getString(REASON));

                if(obj.getString(REASON).equals(PUT_IN_PROGRESS_FOR_KEY) == true) {
                    //Empty Meta means writing does not finish.
                    return new HashMap<String, Object>();
                }
            }
        } catch (TTransportException e) {
            e.printStackTrace();

        } catch (TException e) {
            e.printStackTrace();
        } finally {
            //Should be called.
            m_clientPool.releasePeerClient(client);
        }

        return null;
    }

    public byte[] get(String strKey, int nVersion) {
        if (m_bConnected == false) {
            return null;
        }

        //Will be blocked if connection is not available
        ApplicationToLocalInstanceIface.Client client = (ApplicationToLocalInstanceIface.Client) m_clientPool.getClient();

        try {
            ByteBuffer result;

            if (nVersion < 0 || nVersion == LATEST_VERSION) {
                result = client.get(strKey);
            } else {
                result = client.getVersion(strKey, nVersion);
            }

            JSONObject obj = new JSONObject(new String(result.array(), result.position(), result.remaining()));
            boolean bRet = obj.getBoolean(RESULT);

            if (bRet == true) {
                return Base64.decodeBase64((String) obj.get(VALUE));
            } else {
                System.out.println("Failed to get key:" + strKey + ", Reason: " + obj.getString(REASON));
            }
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } finally {
            //Should be called.
            m_clientPool.releasePeerClient(client);
        }

        return null;
    }

    public List<Integer> getVersionList(String strKey) {
        if (m_bConnected == false) {
            return null;
        }

        //Will be blocked if connection is not available
        ApplicationToLocalInstanceIface.Client client = (ApplicationToLocalInstanceIface.Client) m_clientPool.getClient();

        try {
            ByteBuffer result = client.getVersionList(strKey);
            JSONObject obj = new JSONObject(new String(result.array(), result.position(), result.remaining()));
            boolean bRet = obj.getBoolean(RESULT);

            if (bRet == true) {
                Gson gson = new Gson();
                Type type = new TypeToken<HashMap<Integer, MetaVerInfo>>() {
                }.getType();

                String strList = new String(result.array(), result.position(), result.remaining());
                HashMap<Integer, MetaVerInfo> info = gson.fromJson(strList, type);
                return new LinkedList(info.keySet());
            } else {
                System.out.println("Failed to get getVersionList:" + strKey + ", Reason: " + obj.getString(REASON));
            }
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } finally {
            //Should be called.
            m_clientPool.releasePeerClient(client);
        }

        return null;
    }

    public boolean remove(String strKey) {
        if (m_bConnected == false) {
            return false;
        }

        //Will be blocked if connection is not available
        ApplicationToLocalInstanceIface.Client client = (ApplicationToLocalInstanceIface.Client) m_clientPool.getClient();

        try {
            ByteBuffer result = client.remove(strKey);
            JSONObject obj = new JSONObject(new String(result.array(), result.position(), result.remaining()));
            boolean bRet = obj.getBoolean(RESULT);

            if (bRet == true) {
                return true;
            } else {
                System.out.println("Failed to delete:" + strKey + ", Reason: " + obj.getString(REASON));
            }
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } finally {
            //Should be called.
            m_clientPool.releasePeerClient(client);
        }

        return false;
    }

    public boolean copy(String strSrcKey, String strNewKey) {
        if (m_bConnected == false) {
            return false;
        }

        //Will be blocked if connection is not available
        ApplicationToLocalInstanceIface.Client client = (ApplicationToLocalInstanceIface.Client) m_clientPool.getClient();

        try {
            ByteBuffer result = client.copy(strSrcKey, strNewKey);
            JSONObject obj = new JSONObject(new String(result.array(), result.position(), result.remaining()));
            boolean bRet = obj.getBoolean(RESULT);

            if (bRet == true) {
                return true;
            } else {
                System.out.println("Failed to copy:" + strSrcKey + ", Reason: " + obj.getString(REASON));
            }
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } finally {
            //Should be called.
            m_clientPool.releasePeerClient(client);
        }

        return false;
    }

    public boolean rename(String strSrcKey, String strNewKey) {
        if (m_bConnected == false) {
            return false;
        }

        //Will be blocked if connection is not available
        ApplicationToLocalInstanceIface.Client client = (ApplicationToLocalInstanceIface.Client) m_clientPool.getClient();

        try {
            ByteBuffer result = client.rename(strSrcKey, strNewKey);
            JSONObject obj = new JSONObject(new String(result.array(), result.position(), result.remaining()));
            boolean bRet = obj.getBoolean(RESULT);

            if (bRet == true) {
                return true;
            } else {
                System.out.println("Failed to rename:" + strSrcKey + ", Reason: " + obj.getString(REASON));
            }
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } finally {
            //Should be called.
            m_clientPool.releasePeerClient(client);
        }

        return false;
    }
}