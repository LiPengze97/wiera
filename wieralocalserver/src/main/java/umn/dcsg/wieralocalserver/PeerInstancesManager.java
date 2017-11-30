package umn.dcsg.wieralocalserver;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

import com.google.gson.Gson;
import umn.dcsg.wieralocalserver.info.Latency;
import umn.dcsg.wieralocalserver.thriftinterfaces.LocalInstanceToPeerIface.Client;
import umn.dcsg.wieralocalserver.thriftinterfaces.LocalInstanceToWieraIface;
import umn.dcsg.wieralocalserver.utils.Utils;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import org.json.*;

public class PeerInstancesManager {
    private static final String m_strExternalIP = LocalServer.getExternalIP();

    //Now server becomes thrift server.
    public Thread m_localInstancePeerServerThread = null;
    public Thread m_pingThread = null;
    public LocalPeerInstancesServer m_localInstancePeerServer;
    LocalInstance m_instance = null;

    HashMap<String, ThriftClientPool> m_peersList;
    String m_strPrimaryHostname;

    //This class will be used for ping and getting information with piggybacking

    public class Ping implements Runnable {
        HashMap<String, Client> m_pingClient;

        Ping(HashMap<String, Client> pool) {
            m_pingClient = pool;
        }

        @Override
        public void run() {
            long lTime = 3; //every 30 seconds
            Client peerClient;

            while (true) {
                for (String strHostName : m_peersList.keySet()) {
                    peerClient = m_pingClient.get(strHostName);

                    try {
                        //Need to check ping (latency)
                        Latency latency = new Latency();
                        latency.start();
                        String strRet = peerClient.ping();
                        latency.stop();

                        JSONObject ret = new JSONObject(strRet);

                        boolean bResult = (boolean) ret.get(Constants.RESULT);
                        double op_time = Utils.convertToDouble(ret.get(Constants.OP_TIME));

                        String strStorageInfo = (String) ret.get(Constants.VALUE);
                        String strNetworkInfo = (String) ret.get(Constants.VALUE2);
                        String strAccessInfo = (String) ret.get(Constants.VALUE3);

                        if (bResult == true) {
                            latency.setAdjustTime(op_time);

                            m_instance.m_localInfo.addNetworkLatency(strHostName, latency);

                            //Set other peers information
                            m_instance.m_localInfo.updatePeerInformation(strHostName, strStorageInfo, strNetworkInfo, strAccessInfo);

                            //System.out.printf("Ping from : %s\n %s\n" , strHostName, strNetworkInfo);
                        } else {
                            System.out.println("This is impossible return value for Ping.");
                        }
                    } catch (TTransportException e) {
                        e.printStackTrace();
                    } catch (TException e) {
                        e.printStackTrace();
                    }
                }

                //Update monitoring data to Wiera every 30 seconds
                if (lTime == 3) {
                    updateMonitoringData();
                }

                //Every 10 seconds
                //Todo need to be configurable
                try {
                    lTime += 1;
                    lTime %= 4;
                    Thread.sleep(1666);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void setPrimaryPeerHostname(String strHostname) {
        m_strPrimaryHostname = strHostname;
    }

    public String getPrimaryPeerHostname() {
        return m_strPrimaryHostname;
    }

    public Client getPeerClient(String strHostName) {
        ThriftClientPool info = m_peersList.get(strHostName);

        if (info != null) {
            return (Client) info.getClient();
        } else {
            //	System.out.println("[debug] Failed to find peer list of " + strHostName);
        }

        return null;
    }
    public List<Client> getAllPeerClient(){
        List <Client> rs = new LinkedList<>();
        for (Map.Entry<String, ThriftClientPool> entry : m_peersList.entrySet()) {
            rs.add((Client)entry.getValue().getClient());
        }
        return rs;
    }
    public void releasePeerClient(String strHostName, Client peerClient) {
        ThriftClientPool info = m_peersList.get(strHostName);

        if (info != null) {
            info.releasePeerClient(peerClient);
        }
    }

    public PeerInstancesManager(LocalInstance instance) {
        m_instance = instance;
        m_peersList = new HashMap<String, ThriftClientPool>();

        //ping (latency) info
        //Can be optimized by allow local server measure latency between DCs
        //Later; For now there is only one (a few) instance in each DC

        //Create instance server
        //This is the server for receiving req from other peers.
        m_localInstancePeerServer = new LocalPeerInstancesServer(this);
        m_localInstancePeerServerThread = (new Thread(m_localInstancePeerServer));
        m_localInstancePeerServerThread.start();
    }



    public HashMap<String, ThriftClientPool> getPeersList() {
        return m_peersList;
    }

    public List<String> getPeersHostnameList() {
        if (m_peersList != null) {
            return new LinkedList<>(getPeersList().keySet());
        } else {
            return null;
        }
    }

    public List<String> getRandomPeers(int nCnt) {
        List lst = new LinkedList<>(getPeersList().keySet());
        Collections.shuffle(lst);
        return lst.subList(0, nCnt);
    }

    public ThriftClientPool findPeerByHostname(String strHostName) {
        return m_peersList.get(strHostName);
    }

    public int getPeersServerPort() {
        return m_localInstancePeerServer.getPort();
    }

    boolean updatePeersInfo(JSONArray peers) {
        int nLen = peers.length();
        String strPeerIP;
        String strPeerHostName;
        int nPort = m_localInstancePeerServer.getPort();
        int nPeerPort = 0;
        JSONArray peer;

        ThriftClientPool pool;
        //Thread only for ping.
        HashMap<String, Client> clientPing = new HashMap<String, Client>();

        if(nLen > 1) {
            for (int i = 0; i < nLen; i++) {
                peer = (JSONArray) peers.get(i);
                strPeerHostName = (String) peer.get(0);
                strPeerIP = (String) peer.get(1);
                nPeerPort = (int) peer.get(3);

                //check to avoid connect to myself.
                //if (m_strExternalIP.equals(strPeerIP) == false || nPort != nPeerPort)
                //For now there can be only one instance at each DC
                if (LocalServer.getHostName().compareTo(strPeerHostName) != 0)// //|| nPort != nPeerPort)
                {
                    //For now only one instance per global_policy
                    if (m_peersList.containsKey(strPeerHostName) == false) {
                        //Thrift client is currently 2->number of peer connection
                        //pool = new ThriftClientPool(strPeerIP, nPeerPort, 12, LocalInstanceClient.class);
                        pool = new ThriftClientPool(strPeerIP, nPeerPort, 5, Client.class);
                        m_peersList.put(strPeerHostName, pool);

                        //For ping
                        try {
                            clientPing.put(strPeerHostName, (Client) pool.createLocalInstancePeerClient());
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        } catch (InstantiationException e) {
                            e.printStackTrace();
                        } catch (NoSuchMethodException e) {
                            e.printStackTrace();
                        } catch (InvocationTargetException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }

            System.out.println("Done to create peer connection.");

            System.out.println("------------------------------");
            System.out.println("- My peer information -");

            for (String peerHostName : m_peersList.keySet()) {
                System.out.println(peerHostName + " (" + m_peersList.get(peerHostName).getIP() + ")");
            }

            //Now starts ping thread for monitoring all information
            m_pingThread = new Thread(new Ping(clientPing));
            m_pingThread.start();
        } else {
            System.out.println("Stand alone instance.");
        }
        return true;
    }

    private boolean updateMonitoringData() {
        try {
            LocalInstanceToWieraIface.Client client = m_instance.getWieraClient();

            if (client != null) {
                Gson gson = new Gson();
                JSONObject req = new JSONObject();
                req.put(Constants.HOSTNAME, LocalServer.getHostName());
                req.put(LocalServer.getHostName(), m_instance.m_localInfo.getMonitoringData());

                String strResponse = client.updateMonitoringData(req.toString());
                m_instance.releaseWieraClient();

                JSONObject response = new JSONObject(strResponse);

                boolean bRet = (boolean) response.get("result");

                if (bRet == false) {
                    String strReason = (String) response.get(Constants.VALUE);
                    System.out.println("Failed to update monitoring information to Wiera Reason: " + strReason);
                } else {
                    return true;
                }
            }
        } catch (TException e) {
            e.printStackTrace();
        }

        return false;
    }
}