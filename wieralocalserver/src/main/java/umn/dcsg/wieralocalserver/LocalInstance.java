package umn.dcsg.wieralocalserver;
import static umn.dcsg.wieralocalserver.Constants.*;
import static umn.dcsg.wieralocalserver.TierInfo.TIER_TYPE.*;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import umn.dcsg.wieralocalserver.events.Event;
import umn.dcsg.wieralocalserver.events.EventDispatch;
import umn.dcsg.wieralocalserver.events.EventRegistry;
import umn.dcsg.wieralocalserver.events.MonitoringEvent;
import umn.dcsg.wieralocalserver.responses.Response;
import umn.dcsg.wieralocalserver.info.Latency;
import umn.dcsg.wieralocalserver.storageinterfaces.StorageInterface;
import umn.dcsg.wieralocalserver.thriftinterfaces.ApplicationToLocalInstanceIface;
import umn.dcsg.wieralocalserver.thriftinterfaces.LocalInstanceToWieraIface;
import umn.dcsg.wieralocalserver.thriftinterfaces.RedisWrapperApplicationIface;
import umn.dcsg.wieralocalserver.thriftinterfaces.WieraToLocalInstanceIface;
import umn.dcsg.wieralocalserver.wrapper.RedisWrapperApplicationInterface;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;

import javax.crypto.KeyGenerator;
import java.io.FileInputStream;
import java.io.IOException;
//import java.net.ServerSocket;
//import java.net.Socket;
//import java.net.SocketTimeoutException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.json.*;

/**
 * LocalInstance's duties
 *      1. Manage metadata
 *      2. Provide services for wiera central, client, peers, and also have peer-client
 *      3. Manage Event and response
 *      4. Storage Tiers
 *
 *
 * */

public class LocalInstance {

    // Utility variables
    public KeyLocker m_keyLocker = null;
    public Metrics m_localInfo = null;
    final private int defaultWorkerCount = 1;
    private int workerCount = defaultWorkerCount;
    //Nan : the following two are not used.
    //final private int defaultEDTCount = 1;
    //private int edtCount = defaultEDTCount;
    private boolean m_bVersioningSupport = false;
    private String m_strDefaultConfig = "config.txt";

    /*AES (128)
    DES (56)
    DESede (168)
    HmacSHA1
    HmacSHA256*/
    public String encryptionAlgorithm = "AES";
    public Key encryptionKey = null;

    // We will start listening for client putObject/getObject on this port
    final private int defaultPort = 55555;
    private int m_nServerPortForApplication = defaultPort;
    private int m_nServerPortForWiera = 0;
    public String m_strManagerIP;
    public int m_nManagerPort = 0;
    protected String m_strPolicyID;
    protected boolean m_bStandAloneMode;

    // Metadata
    public MetadataStore m_metadataStore = null;


    // Communication variables
    public TServer m_applicationServer = null;
    public TServer m_redisWrapperApplicationServer = null;
    public TServer m_wieraServer = null;
    public LocalInstanceToWieraIface.Client m_wieraClient;
    public Lock m_lockForWieraClient = new ReentrantLock();
    public PeerInstancesManager m_peerInstanceManager = null;
    public ApplicationToLocalInstanceInterface m_applicationToLocalInstanceInterface = null;



    // Storage Tiers
    public Tiers m_tiers = null;
    public String m_strDefaultTierName = null;
    private boolean m_bRedisSupport = false;


    // Event related variables

    public ReentrantLock onPutSignalLock = null;
    public Condition putEventOccurred = null;
    public ReentrantLock onGetSignalLock = null;
    public Condition getEventOccurred = null;
    public ReentrantLock onDelSignalLock = null;
    public Condition delEventOccurred = null;

    // The following list track the various action events we would need to execute when the actions happen!
    public EventRegistry m_eventRegistry = null;
    public ArrayList<UUID> onPutEvents = null;
    public ArrayList<UUID> onGetEvents = null;
    public ArrayList<UUID> onDelEvents = null;

    // This list tracks the threhold events the system will evaluate
    public ArrayList<UUID> thresholdEvents = null;










    /////////////////////// Initialization /////////////////////////


    public LocalInstance(String configFileName, JSONObject policy, boolean bStandAlone) throws NoSuchFieldException, IOException {
        m_bStandAloneMode = bStandAlone;
        m_strPolicyID = (String) policy.get(ID);
        JSONObject localPolicy;
        if(configFileName != null){
            m_strDefaultConfig= configFileName;
        }

        if(policy.has(LocalServer.getHostName()) == true)
        {
            localPolicy = policy.getJSONObject(LocalServer.getHostName());
        } else {
            throw new NoSuchFieldException();
        }

        if(m_bStandAloneMode == true) {
            System.out.println("Local instance is running stand alone mode without peers");
        }

        // Parse the config file to see if we want any default values to be overridden
        initConfiguration(configFileName);

        //Connect to local instance manager.
        initComm(localPolicy);

        // As the very first step allocate the important data structures and fill out the important values
        // Initialize the metadata store
        initMetadataStore(m_strPolicyID);

        // Initialize the list of m_tiers we can support
        initTiers(localPolicy);

        // Initialize the events that we need to execute for a policy
        initEvents(localPolicy);

        //This will manage all information related with latency and cost and so on.
        m_localInfo = new Metrics(this);
    }

    /**
     * application_port=port for listen applications' requests;
     * redis=boolean support redis
     * versioning=boolean support versioning
     * encryption=string
     * */
    void initConfiguration(String ConfigFileName) {
        Properties config = new Properties();
        String configFileName = (ConfigFileName == null) ? m_strDefaultConfig : ConfigFileName;

        try {
            FileInputStream foo = new FileInputStream(configFileName);
            String tmp = null;

            config.load(foo);

            tmp = config.getProperty("workercount");
            if (tmp != null) {
                workerCount = Integer.parseInt(tmp);
                if (workerCount <= 0)
                    workerCount = defaultWorkerCount;
            }

            tmp = config.getProperty("redis");
            if (tmp != null) {
                m_bRedisSupport = Boolean.parseBoolean(tmp);
            }

            tmp = config.getProperty("versioning");
            if (tmp != null) {
                m_bVersioningSupport = Boolean.parseBoolean(tmp);
            }

            tmp = config.getProperty("encryption");
            if (tmp != null) {
                encryptionAlgorithm = tmp;
            }

            tmp = config.getProperty("application_port");
            if (tmp != null) {
                m_nServerPortForApplication = Integer.parseInt(tmp);
            }
        } catch (IOException e) {
            System.out.println("LocalInstance: Failed to open config file " + configFileName);
        }

        try {
            encryptionKey = KeyGenerator.getInstance(encryptionAlgorithm).generateKey();
        } catch (NoSuchAlgorithmException e) {
            System.out.println("Failed to generate key");
        }
    }




    /////////////////////// communication  /////////////////////////


    boolean initComm(JSONObject policy) {
        boolean bRet;

        //Thrift server for application
        bRet = InitServerForApplication();    //This is a server for Applications
        if (bRet == false) {
            System.out.print("Failed to init server for Application.");
            return false;
        }

        bRet = InitServerForWiera();    //This is a server for Wiera Instance Manager
        if (bRet == false) {
            System.out.print("Failed to init server for Wiera.");
            return false;
        }

        //Check if there is any peer instance.
        if (m_bStandAloneMode == false) {
            bRet = InitServerForPeers();
            if (bRet == false) {
                System.out.println("Failed to init server for peer.");
                return false;
            }
        }

        if (m_bRedisSupport == true) {
            //Redis Wrapper Server
            bRet = InitRedisWrapperServerForApplication();
            if (bRet == false) {
                System.out.print("Failed to init redis Wrapper server for Application.");
            }
        }

        return true;
    }

    //This will let the peer server run and handle the request.
    private boolean InitRedisWrapperServerForApplication() {
        try {
            //TServerSocket socket = new TServerSocket(9098); //listenPort); -> now become random port for client.
            TServerSocket socket = new TServerSocket(6378); //listenPort); -> now become random port for client.
            TServerTransport serverTransport = socket;
            TProtocolFactory tProtocolFactory = new TBinaryProtocol.Factory(true, true);
            TTransportFactory transportFactory = new TFramedTransport.Factory(1048576 * 200);    //Set max size
            RedisWrapperApplicationInterface redis = new RedisWrapperApplicationInterface(this);
            RedisWrapperApplicationIface.Processor processor = new RedisWrapperApplicationIface.Processor(redis);
            m_redisWrapperApplicationServer = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport)
                    .minWorkerThreads(16) // TODO Take this as arguments from user
                    .maxWorkerThreads(64) // TODO Take this as arguments from user
                    .inputTransportFactory(transportFactory)
                    .outputTransportFactory(transportFactory)
                    .inputProtocolFactory(tProtocolFactory)
                    .outputProtocolFactory(tProtocolFactory)
                    .processor(processor));

            return true;
        } catch (Exception e) {
            System.out.println("Error: Failed to setup listen socket. Exiting");
            e.printStackTrace();

        }

        return false;
    }

    //Provide services for applications(clients)
    private boolean InitServerForApplication() {
        TServerSocket socket = null;

        //55555 is preferred for Application Server
        try {
            socket = new TServerSocket(m_nServerPortForApplication); //random port for client.
        } catch (Exception e) {
            //already taken
            socket = null;
        }

        try {
            if (socket == null) {
                socket = new TServerSocket(0); //random port for client.
            }

            TServerTransport serverTransport = socket;
            TProtocolFactory tProtocolFactory = new TBinaryProtocol.Factory(true, true);
            TTransportFactory transportFactory = new TFramedTransport.Factory(1048576 * 200);    //Set max size
            m_applicationToLocalInstanceInterface = new ApplicationToLocalInstanceInterface(this);
            ApplicationToLocalInstanceIface.Processor processor = new ApplicationToLocalInstanceIface.Processor(m_applicationToLocalInstanceInterface);
            m_applicationServer = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport)
                    .minWorkerThreads(16) // TODO Take this as arguments from user
                    .maxWorkerThreads(64) // TODO Take this as arguments from user
                    .inputTransportFactory(transportFactory)
                    .outputTransportFactory(transportFactory)
                    .inputProtocolFactory(tProtocolFactory)
                    .outputProtocolFactory(tProtocolFactory)
                    .processor(processor));

            //Port for application
            m_nServerPortForApplication = socket.getServerSocket().getLocalPort();
            return true;
        } catch (Exception e) {
            System.out.println("Error: Failed to setup listen socket. Exiting");
            e.printStackTrace();
        }

        return false;
    }
    // Provide services for Wiera central
    private boolean InitServerForWiera() {
        try {
            //For now we set to hard-coded port for Azure.
            TServerSocket socket = new TServerSocket(0); //listenPort); -> now become random port for client.
            TServerTransport serverTransport = socket;
            TProtocolFactory tProtocolFactory = new TBinaryProtocol.Factory(true, true);
            TTransportFactory transportFactory = new TFramedTransport.Factory();
            WieraToLocalInstanceInterface inter = new WieraToLocalInstanceInterface(this);
            WieraToLocalInstanceIface.Processor processor = new WieraToLocalInstanceIface.Processor(inter);
            m_wieraServer = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport)
                    .minWorkerThreads(3) // TODO Take this as arguments from user
                    .maxWorkerThreads(10) // TODO Take this as arguments from user
                    .inputTransportFactory(transportFactory)
                    .outputTransportFactory(transportFactory)
                    .inputProtocolFactory(tProtocolFactory)
                    .outputProtocolFactory(tProtocolFactory)
                    .processor(processor));

            //Port for Wiera
            m_nServerPortForWiera = socket.getServerSocket().getLocalPort();
            return true;
        } catch (Exception e) {
            System.out.println("Error: Failed to setup listen socket. Exiting");
            e.printStackTrace();
        }

        return false;
    }
    //Provides services for peers
    private boolean InitServerForPeers() {
        m_peerInstanceManager = new PeerInstancesManager(this);


        return true;
    }



    //////////////////////// Initalization of metadata ////////////////////


    void initMetadataStore(String strWieraID) throws IOException {
        m_keyLocker = new KeyLocker();
        m_metadataStore = new MetadataStore(strWieraID);
        m_metadataStore.init();
    }



    ////////////////////////////Initialization of Storage tiers ///////////////////
    // Nothing special just populate the list of m_tiers that we can support in the prototype.
    // NOTE: We expect things to be in place, i.e we expect the S3 bucket to already be present
    // and the same goes for EBS volumes and the Ephemeral volumes. We do not
    // validate this!
    void initTiers(JSONObject localPolicy) {
        m_tiers = new Tiers((JSONArray) localPolicy.get(STORAGE_TIERS), workerCount);

        if(m_tiers.getDefaultTier() != null) {
            m_strDefaultTierName = m_tiers.getDefaultTier().getTierName();
        }
    }


    ////////////////////Initialization of Events and response ///////////////////


    List initResponses(JSONArray jsonResponses, String strEventName) {
        JSONObject jsonResponse;
        String strResponseType;
        JSONObject responseParams;

        List responseList = new LinkedList();
        Map<String, Object> params;
        Response response;

        for (int i = 0; i < jsonResponses.length(); i++) {
            jsonResponse = jsonResponses.getJSONObject(i);
            strResponseType = jsonResponse.getString(RESPONSE_TYPE);

            if (jsonResponse.has(RESPONSE_PARAMETERS) == true) {
                responseParams = jsonResponse.getJSONObject(RESPONSE_PARAMETERS);

                //Convert JSon into Map
                params = new Gson().fromJson(responseParams.toString(),
                        new TypeToken<HashMap<String, Object>>() {
                        }.getType());
            } else {
                params = null;
            }

            response = Response.createResponse(this, strResponseType, strEventName, params);
            responseList.add(response);
        }

        return responseList;
    }

    // This function is what we need to change when we want our prototype to
    // execute different events to
    // realize a policy! A sample of how different events can be added is provided
    // as commented code.
    void initEvents(JSONObject policy) throws NoSuchFieldException {
        m_eventRegistry = new EventRegistry();
        (new Thread(m_eventRegistry)).start();

        onPutSignalLock = new ReentrantLock();
        onGetSignalLock = new ReentrantLock();
        onDelSignalLock = new ReentrantLock();

        putEventOccurred = onPutSignalLock.newCondition();
        getEventOccurred = onGetSignalLock.newCondition();
        delEventOccurred = onDelSignalLock.newCondition();

        // Start Action Events
        onPutEvents = new ArrayList<UUID>();
        onGetEvents = new ArrayList<UUID>();
        onDelEvents = new ArrayList<UUID>();
        // End Action Events

        thresholdEvents = new ArrayList<UUID>();

        //Init event with policy
        JSONArray jsonEvents = policy.getJSONArray(EVENTS);
        JSONObject jsonEvent;
        JSONArray jsonEventTrigger;

        String strEventType;
        String strEventTrigger;

        //Let application decides response no matter what happen.
        //Not check whether responses are related with (supported by) event
        Map<String, Object> eventConditions;
        Event event;
        List lstResponses;
        UUID eventID;

        for (int i = 0; i < jsonEvents.length(); i++) {
            jsonEvent = jsonEvents.getJSONObject(i);
            strEventType = jsonEvent.getString(EVENT_TYPE);

            //Event param
            //Convert JSon into Map
            eventConditions = new Gson().fromJson(jsonEvent.getJSONObject(EVENT_CONDITIONS).toString(),
                    new TypeToken<HashMap<String, Object>>() {
                    }.getType());

            //Init Responses
            lstResponses = initResponses(jsonEvent.getJSONArray(RESPONSES), strEventType);

            //Create Event
            event = Event.createEvent(this, strEventType, eventConditions, lstResponses);

            //Add event into registry to manage
            eventID = m_eventRegistry.addEvent(event);

            //Set Event Trigger
            jsonEventTrigger = jsonEvent.getJSONArray(EVENT_TRIGGER);
            for (int j = 0; j < jsonEventTrigger.length(); j++) {
                strEventTrigger = jsonEventTrigger.getString(j);

                switch (strEventTrigger) {
                    case ACTION_GET_EVENT:
                        onGetEvents.add(eventID);
                        break;
                    case ACTION_PUT_EVENT:
                        onPutEvents.add(eventID);
                        break;
                    case TIMER_EVENT:
                        if (jsonEventTrigger.length() > 1 && event instanceof MonitoringEvent) {
                            ((MonitoringEvent) event).initTimer();
                        }
                        break;
                    default:
                        break;
                }
            }
        }


    }


    ///////////////////// Start this instance, called by caller /////////////////////


    public void runForever(JSONObject policy) {
        //Start services for applications (clients)
        Thread applicationServerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                m_applicationServer.serve();
            }
        });
        applicationServerThread.start();

        System.out.printf("Application server is running on port %d\n", m_nServerPortForApplication);

        synchronized (m_applicationServer) {
            while (m_applicationServer.isServing() == false) {
                try {
                    m_applicationServer.wait(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        if (m_bRedisSupport == true) {
            //Now run thirft server forever
            Thread redisWrapperApplicationServerThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    m_redisWrapperApplicationServer.serve();
                }
            });
            redisWrapperApplicationServerThread.start();

            //Make sure WrapperApplications is running
            synchronized (m_redisWrapperApplicationServer) {
                while (m_redisWrapperApplicationServer.isServing() == false) {
                    try {
                        m_redisWrapperApplicationServer.wait(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        Thread wieraThread = null;

        //Start services for Wiera central and register (instance)
        wieraThread = new Thread(new Runnable() {
            @Override
            public void run() {
                m_wieraServer.serve();
            }
        });
        wieraThread.start();

        //To give some time for wieraServer to run to receive request from Wiera Server
        synchronized (m_wieraServer) {
            while (m_wieraServer.isServing() == false) {
                try {
                    m_wieraServer.wait(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        System.out.format("LocalInstance Instance Server starts waiting Wiera's requests from port: %d\n", m_nServerPortForWiera);
        registerToWiera(policy);

        try {
            //Check all threads for thrift are terminated properly.
            applicationServerThread.join();
            System.out.println("applicationServer Thread is terminated.");

            if (m_bStandAloneMode == false) {
                //Check peer servers
                if (m_peerInstanceManager.m_localInstancePeerServerThread != null) {
                    m_peerInstanceManager.m_localInstancePeerServerThread.join();
                }

                System.out.println("peerInstanceServer Thread is terminated.");

                //Check wieraServer
                if (wieraThread != null) {
                    wieraThread.join();
                    System.out.println("LocalInstaceServer for Wiera Thread is terminated.");
                }

                System.out.println("All threads for thrift servers are terminated.");
                return;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Exception but this instance will be terminated.");
    }


    private boolean registerToWiera(JSONObject policy) {
        int nRetryTime = 1;
        m_strManagerIP = (String) policy.get("manager_ip");
        //m_nManagerPort = Utils.convertToLong(policy.get("manager_port"));
        m_nManagerPort = (int) policy.get("manager_port");

        while (true) {
            m_wieraClient = initWieraClient();

            if (m_wieraClient != null) {
                break;
            }

            if (nRetryTime == 1) {
                System.out.format("Fail to connect to instance manager server %s:%d. Try again %d sec later\n", m_strManagerIP, m_nManagerPort, nRetryTime);
            } else {
                System.out.format("Fail to connect to instance manager server %s:%d. Try again %d secs later\n", m_strManagerIP, m_nManagerPort, nRetryTime);
            }

            try {
                Thread.sleep(1 * nRetryTime * 1000);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }

            nRetryTime = nRetryTime * 2;
        }
        // return local Instance information to Wiera central
        try {
            JSONObject req = new JSONObject();
            req.put(HOSTNAME, LocalServer.getHostName());
            req.put(IP_ADDRESS, LocalServer.getExternalIP());

            //Now client listen port will be automatically assigned.
            HashMap<String, Integer> ports = new HashMap<String, Integer>();
            ports.put(APPLICATION_PORT, m_nServerPortForApplication);
            ports.put(INSTANCE_PORT, m_nServerPortForWiera);
            req.put(VALUE, ports);

            if(m_bStandAloneMode == false) {
                ports.put(PEER_PORT, m_peerInstanceManager.getPeersServerPort());
            }

            String strResponse = getWieraClient().registerLocalInstance(req.toString());
            releaseWieraClient();

            JSONObject response = new JSONObject(strResponse);

            boolean bRet = (boolean) response.get("result");

            if (bRet == true) {
                //This will be cost info from now on (not peer info anymore as Wiera will not send it)
                JSONObject costInfo = new JSONObject((String) response.get(VALUE));
                JSONObject goals = new JSONObject((String) response.get(VALUE2));

                if (costInfo != null) {
                    m_localInfo.setCostInfo(costInfo);
                    m_localInfo.setGoals(goals);
                }
            } else {
                String strReason = (String) response.get(VALUE);
                System.out.println("Failed to register to Wiera Reason: " + strReason);
            }

            return true;
        } catch (TException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

        return false;
    }

    //As a client of wiera central
    LocalInstanceToWieraIface.Client initWieraClient() {
        TTransport transport;
        transport = new TSocket(m_strManagerIP, m_nManagerPort);
        TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
        LocalInstanceToWieraIface.Client client = new LocalInstanceToWieraIface.Client(protocol);

        try {
            transport.open();
        } catch (TException x) {
            x.printStackTrace();
            return null;
        }

        return client;
    }

    public LocalInstanceToWieraIface.Client getWieraClient() {
        //only one thread can use this one
        m_lockForWieraClient.lock();
        return m_wieraClient;
    }

    public void releaseWieraClient() {
        m_lockForWieraClient.unlock();
    }

    /////////////////////////// Stop this instance ///////////////////////

    //Close all server and stop working.
    //Stop all server. Need to check all requests are handled first.
    //But for now simply close all server
    public boolean shutdown() {
        //This includes sasving all metadata
        m_metadataStore.close();

        if (m_applicationServer != null && m_applicationServer.isServing() == true) {
            m_applicationServer.stop();
        }

        if (m_peerInstanceManager != null && m_peerInstanceManager.m_localInstancePeerServer != null &&
                m_peerInstanceManager.m_localInstancePeerServer.m_thriftServer != null) {
            m_peerInstanceManager.m_localInstancePeerServer.m_thriftServer.stop();
        }

        if (m_wieraServer != null) {
            m_wieraServer.stop();
        }

        //Close the debug socket for event registry
        m_eventRegistry.closeSocket();

        return true;
    }



    /////////////////////////// Methods ///////////////////////

    public boolean isVersionSupported() {
        return m_bVersioningSupport;
    }

    public boolean isStandAloneMode() {
        return m_bStandAloneMode;
    }

    public String getPolicyID() {
        return m_strPolicyID;
    }


    public String formatTransfer(String strKey, int nVer) {
        String rs_strKey = String.format(KEY_TRANSLATE_FORMAT, strKey, nVer);
        return rs_strKey;
    }




    public MetaObjectInfo getMetadata(String strKey) {
        MetaObjectInfo obj = m_metadataStore.getObject(strKey);
        return obj;
    }
    //If this not called, meta will not updated
    public boolean commitMetadata(MetaObjectInfo obj) {
        try {
            m_metadataStore.putObject(obj);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }



    public boolean containsKey(String key, int nVer, String strTiername){
        boolean rs = false;
        MetaObjectInfo obj = getMetadata(key);
        if(obj != null){
            rs = obj.hasLocale(nVer, strTiername);
        }
        return rs;
    }



    public boolean deleteInternal(String strKey, int nVer, String strTierName, long lSize) {
        boolean ret = false;
        Tier tier = m_tiers.getTier(strTierName);
        try {
            if (tier != null) {
                StorageInterface interf = tier.getInterface();
                if (interf != null) {
                    if(isVersionSupported() == true){
                        strKey = formatTransfer(strKey, nVer);
                    }
                    ret = interf.doDelete(strKey);
                    if (ret == true) {
                        tier.freeSpace(lSize);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }


    public boolean delete(String strKey, int nVer, String strTiername) {
        boolean rs = false;
        ReentrantReadWriteLock lock = m_keyLocker.getLock(strKey);
        try {
            lock.writeLock().lock();
            MetaObjectInfo obj = getMetadata(strKey);
            Locale localLocale;
            if (obj != null && containsKey(strKey, nVer, strTiername)) {
                long size = obj.getSize(nVer);
                if(deleteInternal(strKey, nVer, strTiername, size) ==  true){
                    // update meta data.
                    rs = obj.removeLocale(nVer, strTiername);
                    if(rs == true){
                        commitMetadata(obj);
                    }
                }
            }
        } finally {
            lock.writeLock().unlock();
        }

        return rs;
    }


    /**
     * Nan: To support version
     * Job of putInternal
     *      1. update the used space.
     *      2. translate the identifier name key with version is support versioning.
     * */
    public boolean putInternal(String strKey, int nVer, byte[] value, String strTierName) {
        Tier tier = m_tiers.getTier(strTierName);
        StorageInterface interf;

        if (tier == null) {
            System.out.printf("[Error] Cannot find storage Tier information: %s\n", strTierName);
            return false;
        }
        interf = tier.getInterface();
        if (interf == null) {
            System.out.printf("[Error] Cannot find storage Tier interface: %s\n", strTierName);
            return false;
        }

        long lRequiredSpace = value.length;

        if (tier.checkFreeSpace(lRequiredSpace) == false) {

            return false;
        }
        if(containsKey(strKey, nVer, strTierName) == true) {
            long lSize = getMetadata(strKey).getSize();
            lRequiredSpace -= lSize;
        }
        if(isVersionSupported() == true){
            // translate key
            strKey = formatTransfer(strKey, nVer);
        }

        //Start Tier latency
        Latency latency = new Latency();
        latency.start();

        boolean ret = interf.doPut(strKey, value);

        if (ret == true) {
            tier.useSpace(lRequiredSpace);
            latency.stop();
            if (m_localInfo.addTierLatency(strTierName, PUT_LATENCY, latency) == false) {
                System.out.printf("Failed to put latency into stat : %.2f\n", latency.getLatencyInMills());
            }
        }

        return ret;
    }

    public MetaObjectInfo put(String strKey, int nVer, byte [] value, String strTierName){
        ReentrantReadWriteLock lock = m_keyLocker.getLock(strKey);
        MetaObjectInfo obj = null;
        try {
            lock.writeLock().lock();
            if (putInternal(strKey, nVer, value, strTierName) == true) {
                obj = getMetadata(strKey);
                if(obj != null){
                    obj.updateVersion(nVer, LocalServer.getHostName(), strTierName, m_tiers.getTierType(strTierName), System.currentTimeMillis(), value.length);
                }else{
                    obj = new MetaObjectInfo(nVer, strKey, LocalServer.getHostName(), strTierName, m_tiers.getTierType(strTierName), System.currentTimeMillis(), value.length, "", m_bVersioningSupport);
                }
                commitMetadata(obj);
            }
        } finally {
            lock.writeLock().unlock();
        }
        return obj;
    }




    public byte[] getInternal(String strKey, int nVer, String strTierName) {
        Tier tier = m_tiers.getTier(strTierName);

        if (tier == null) {
            System.out.printf("Cannot find local storage Tier information: %s\n", strTierName);
            return null;
        }

        StorageInterface interf = tier.getInterface();

        if (interf == null) {
            System.out.printf("Cannot find local storage Tier interface: %s\n", strTierName);
            return null;
        }
        Latency latency = new Latency();
        latency.start();

        if(isVersionSupported() ==  true) {
            // translate key
            strKey = String.format(KEY_TRANSLATE_FORMAT , strKey, nVer);
        }
        byte[] ret = interf.doGet(strKey);

        latency.stop();

        if (m_localInfo.addTierLatency(strTierName, GET_LATENCY, latency) == false) {
            System.out.printf("Failed to get latency into stat : %.2f\n", latency.getLatencyInMills());
        }

        return ret;
    }

    public byte[] get(String strKey, int nVer, String strTierName){
        ReentrantReadWriteLock lock = m_keyLocker.getLock(strKey);
        MetaObjectInfo obj = null;
        byte [] value;
        try {
            lock.readLock().lock();
            value = getInternal(strKey, nVer,strTierName);
            if (value != null){
                obj = getMetadata(strKey);
                if(obj != null){
                    // obj update access time ....
                }else{
                    System.out.println("[Error] Inconsistent metadata.");
                    System.exit(1);
                }
            }
        } finally {
            lock.readLock().unlock();
        }
        return value;
    }

    ////////////////////// Version //////////////////////////

    // Nan:
    public int getLatestVersion(String strKey) {
        MetaObjectInfo obj = getMetadata(strKey);

        if (obj != null) {
            return obj.getLatestVersion();
        }

        return MetaObjectInfo.NO_SUCH_KEY;
    }


    HashMap<Integer, MetaVerInfo> getVersionList(String key) {
        return m_metadataStore.getVersionList(key);
    }


    /////////////////////////////////////Get /////////////////////////////////////
    //Nan:
    //These 'get' function assumes that data is stored local locale.
    //Get the one with latest version
    public byte[] get(String strKey) {
        return get(strKey, getLatestVersion(strKey));
    }
    //Nan: not the fastest one, but user default one.
    public byte[] get(String strKey, int nVer) {
        return get(strKey, nVer,  m_strDefaultTierName);
    }
    public byte[] get(String strKey, String strTiername){
        return get(strKey, getLatestVersion(strKey), strTiername);
    }

    //////////////////////////Delete /////////////////////////////////

    public boolean delete(String strKey){
        return delete(strKey, getLatestVersion(strKey));
    }
    public boolean delete(String strKey, int nVer) {
        return delete(strKey, nVer, m_strDefaultTierName);
    }


    //////////////////////////PUT ////////////////////////////
    public MetaObjectInfo put(String strKey, byte[] value){
        return put(strKey, getLatestVersion(strKey) + 1, value);
    }
    public MetaObjectInfo put(String strKey, int nVer, byte [] value){
        return put(strKey, nVer, value, m_strDefaultTierName);
    }
    public MetaObjectInfo put(String strKey,  byte [] value, String strTiername){
        return put(strKey, getLatestVersion(strKey) + 1, value, m_strDefaultTierName);
    }

    //////////////////////Contains key ///////////////////////
    public boolean containsKey(String strKey, String strTierName) {
        return containsKey(strKey, getLatestVersion(strKey), strTierName);

    }


    //Kwangsung
    //Data may not be stored in local
    public boolean isLocalStorageTier(String strTierName) {
        Tier tierInfo = m_tiers.getTier(strTierName);
        return tierInfo != null;
    }
    // Kwangsung
    //Data may not be stored in local
    public TierInfo.TIER_TYPE getLocalStorageTierType(String strTierName) {
        Tier tierInfo = m_tiers.getTier(strTierName);

        if (tierInfo != null) {
            return tierInfo.getTierType();
        } else {
            //This should be the remote
            return REMOTE_TIER;
        }
    }
    // Kwangsung
    //Data may not be stored in local
    public Locale getLocaleWithID(String strLocaleID) {
        String strHostName = strLocaleID.split(":")[0];
        String strTierName;

        if(strLocaleID.split(":").length == 1) {
            strTierName = "";
        } else {
            strTierName = strLocaleID.split(":")[1];
        }

        Tier tierInfo = m_tiers.getTier(strTierName);

        if (tierInfo != null) {
            return new Locale(strHostName, strTierName, tierInfo.getTierType());
        } else {
            //This should be the remote
            return new Locale(strHostName, strTierName, REMOTE_TIER);
        }
    }

}
