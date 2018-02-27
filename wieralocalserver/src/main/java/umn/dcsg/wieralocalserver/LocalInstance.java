package umn.dcsg.wieralocalserver;
import static umn.dcsg.wieralocalserver.Constants.*;
import static umn.dcsg.wieralocalserver.TierInfo.TIER_TYPE.*;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.commons.lang3.text.StrLookup;
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


public class LocalInstance {
    public MetadataStore m_metadataStore = null;
    public KeyLocker m_keyLocker = null;
    public TServer m_applicationServer = null;
    public TServer m_redisWrapperApplicationServer = null;
    public TServer m_wieraServer = null;
    public Tiers m_tiers = null;
    public String m_strDefaultTierName = null;
    public Metrics m_localInfo = null;

    // Threads that process events are stored in this vector, for now they only process threshold events
    //private Vector<EventDispatch> eventDispatchThreads = null;

    // We have a thread(s) waiting for action events to occur, i.e putObject and getObject actions. We use these following conditions
    // to wake those threads up for servicing the putObject/getObject actions also threshold values are evaluated when these external
    // events change the state of LocalInstance. For now since Event Dispatch threads evaluate only threshold events these conditions
    // will be used to indicate to them that threshold events can be evaluated.
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

    // This list tracks the timer events the system will evaluate
    // public static HashMap<String, Class> stringToResponse = null;

    //From config.txt
    final private int defaultWorkerCount = 1;
    private int workerCount = defaultWorkerCount;

    final private int defaultEDTCount = 1;
    private int edtCount = defaultEDTCount;

    private boolean m_bRedisSupport = false;
    private boolean m_bVersioningSupport = false;
    // This is a hack we generate a new key for each incarnation of LocalInstance, instead
    // we should be reading the key from a file
    public String encryptionAlgorithm = "AES";
    public Key encryptionKey = null;

    // We will start listening for client putObject/getObject on this port
    final private int defaultPort = 55555;
    private int m_nServerPortForApplication = defaultPort;
    private int m_nServerPortForWiera = 0;

	/*// The following globals give us the locations where we need to store data
	public String s3BucketName = defaultS3BucketName;
	public String s3Folder = defaultS3Folder;
	public String ebsFolder = defaultEBSFolder;
	public String serverList = defaultServerList;
	public String ephemeralFolder = defaultEphemeralFolder;
	public String s3KeyID = defaultS3KeyID;
	public String s3SecretID = defaultS3SecretID;*/

    // This function doesn't do much now just initializes the metadata store (allocates memory for it).
    // In the future when we move to a key value store of some kind we might need to do more things then
    public LocalInstanceToWieraIface.Client m_wieraClient;
    Lock m_lockForWieraClient = new ReentrantLock();

    String m_strManagerIP;
    int m_nManagerPort = 0;

    //TServer m_localInstanceServer;
    //long m_lLocalInstanceServerPort = 0;

    //Wiera id where the LocalInstance instance are running for
    protected String m_strPolicyID;
    protected boolean m_bStandAloneMode;

    //	public SocketComm m_instanceManagerComm = null;
    public PeerInstancesManager m_peerInstanceManager = null;
    public ApplicationToLocalInstanceInterface m_applicationToLocalInstanceInterface = null; //This is for forwarded request for now.

    public boolean isVersionSupported() {
        return m_bVersioningSupport;
    }

    public boolean isStandAloneMode() {
        return m_bStandAloneMode;
    }

    public String getPolicyID() {
        return m_strPolicyID;
    }

    public LocalInstanceToWieraIface.Client getWieraClient() {
        //only one thread can use this one
        m_lockForWieraClient.lock();
        return m_wieraClient;
    }

    public void releaseWieraClient() {
        m_lockForWieraClient.unlock();
    }

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

    void initMetadataStore(String strWieraID) throws IOException {
        m_keyLocker = new KeyLocker();
        m_metadataStore = new MetadataStore(strWieraID);
        m_metadataStore.init();
    }

    // This function will parse the config file and load up values for m_tiers and other parameters
    void initConfiguration(String ConfigFileName) {
        Properties config = new Properties();
        String configFileName = (ConfigFileName == null) ? "config.txt" : ConfigFileName;

        try {
            FileInputStream foo = new FileInputStream(configFileName);
            String tmp = null;

            // Load config file properties
            config.load(foo);

            tmp = config.getProperty("workercount");
            if (tmp != null) {
                workerCount = Integer.parseInt(tmp);
                if (workerCount <= 0)
                    workerCount = defaultWorkerCount;
            }

            tmp = config.getProperty("edtcount");
            if (tmp != null) {
                edtCount = Integer.parseInt(tmp);
                if (edtCount <= 0)
                    edtCount = defaultEDTCount;
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

        //Sample event
        //Response SimplePutResponse = new StoreResponse(this, "S3");
        //Response SimplePutResponse = new StoreResponse(this, "local_redis");
        //Response SimplePutResponse = new StoreResponse(this, "local_disk");

        //Simple getObject and putObject Event
        //Can be combined with or operator
        //pre-defined storage type : cheapest_storage, fastest_storage, persistent_storage, workload_aware_storage
        //Response simplePutResponse = new StoreResponse(this, "local_disk");
        //Response simplePutResponse = new StoreResponse(this, "ebs-st1");
		/*Response adaptivePutResponse = new StoreAdaptvelyResponse(this, CHEAPEST, 100);
		Event simplePutEvent = new ActionPutEvent(adaptivePutResponse);
		onPutEvents.add(m_eventRegistry.addEvent(simplePutEvent));
*/
        //Action Get Event
        //Response simpleGetResponse = new RetrieveResponse(this);
        //Event simpleGetEvent = new ActionGetEvent(simpleGetResponse);

        //Response adaptiveGetResponse = new FetchAdaptivelyResponse(this, CHEAPEST, 100);
        //Event simpleGetEvent = new ActionGetEvent(adaptiveGetResponse);
        //onGetEvents.add(m_eventRegistry.addEvent(simpleGetEvent));

        //Regularly run event
        //eventDispatchThreads = new Vector<EventDispatch>();

        //Partitioned and merged.
        //Partition data
//		ArrayList tierList = m_tiers.getTierListWithType(Tier.TIER_TYPE.CLOUD_STORAGE);
//
//		Response response = new SplitDataResponse(this, tierList, tierList.size() - 2, 2, 3);
//		Event SimplePutEvent = new ActionPutEvent(response);
//		onPutEvents.add(m_eventRegistry.addEvent(SimplePutEvent));
//
//		//Action Get Event
//		response = new MergeDataResponse(this, tierList, tierList.size() - 2, 2, 3);
//		Event SimpleGetEvent = new ActionGetEvent(response);
//		onGetEvents.add(m_eventRegistry.addEvent(SimpleGetEvent));


        // Start threshold events

        //Experiment 1
        //Latency 800 ms
        //Period 30 seconds
        //EventualConsistencyResponse period 2 seconds Change data distribution at run-time
//		Event latencyThresholdEvent = new MonitoringOperationLatencyEvent(this, new ChangeEventResponseResponse(this), dataDistribution, 800, 30000, 1000);
//		thresholdEvents.add(m_eventRegistry.addEvent(latencyThresholdEvent));

        //Experiment 2
        //Change primary instance dynamically.
        //Event requestThresholdEvent = new MonitoringRequestCntEvent(this, new ChangePrimaryResponse(this), 15000);
        //thresholdEvents.add(m_eventRegistry.addEvent(requestThresholdEvent));

        //Experiment 3
        //Move data to cheaper tier.
        // Last parameter in Second
        //Event coldDataThresholdEvent = new MonitoringColdDataEvent(this, new MoveResponse(this), 10);
        //thresholdEvents.add(m_eventRegistry.addEvent(coldDataThresholdEvent));

        //Wiera Experiment 2
        //Change data placement if needed.
        //Event requestMonitoingEvent = new MonitoringRequestCntEvent(this, new ChangeDataPlacementResponse(this), 10000);
        //thresholdEvents.add(m_eventRegistry.addEvent(requestMonitoingEvent));


        // Start threads that evaluates threshold events when a putObject occurs
//		for (int i = 0; i < edtCount; i++)
//		{
        //Timeout 600000-> 600seconds
        //EventDispatch edtThread = new EventDispatch(thresholdEvents, onPutSignalLock, putEventOccurred, m_eventRegistry, 10000);
        //edtThread.start();
        //eventDispatchThreads.add(edtThread);

        //Thread for finding cold data
        //For every 10 seconds
        //edtThread = new EventDispatch(thresholdEvents, onPutSignalLock, putEventOccurred, m_eventRegistry, 10000);
        //edtThread.start();
        //eventDispatchThreads.add(edtThread);

        // Start threads that evaluates thresholds when a getObject occurs
        //	eventDispatchThreads.add(edtThread);
        //	edtThread = new EventDispatch(thresholdEvents, onGetSignalLock, getEventOccurred, m_eventRegistry );
        //	edtThread.start();
        //	eventDispatchThreads.add(edtThread);
        //}
    }
    //Will be chaged based on information on Wiera.
    public LocalInstance(String configFileName, JSONObject policy, boolean bStandAlone) throws NoSuchFieldException, IOException {
        m_bStandAloneMode = bStandAlone;
        m_strPolicyID = (String) policy.get(ID);
        JSONObject localPolicy;

        if(policy.has(LocalServer.getHostName()) == true) {
            localPolicy = policy.getJSONObject(LocalServer.getHostName());
        } else {
            throw new NoSuchFieldException();
        }

        if(m_bStandAloneMode == true) {
            System.out.println("Local instance is running stand alone mode without peers");
        }

        //Connect to local instance manager.
        initComm(localPolicy);

        // As the very first step allocate the important data structures and fill out the important values
        // Initialize the metadata store
        initMetadataStore(m_strPolicyID);

        // Parse the config file to see if we want any default values to be overridden
        //Worker, Process for event, encrytion
        initConfiguration(configFileName);

        // Initialize the list of m_tiers we can support
        initTiers(localPolicy);

        // Initialize the events that we need to execute for a policy
        initEvents(localPolicy);

        //This will manage all information related with latency and cost and so on.
        m_localInfo = new Metrics(this);
    }

    public void runForever(JSONObject policy) {
        //Now run thrift server forever
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

        //start Wiera server and register
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

        //Since Wiera will call peerInfo when there is a new local instance.
        //Server should be run first.
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

    //This will let the peer server run and handle the request.
    private boolean InitServerForApplication() {
        TServerSocket socket = null;

        //55555 is preferred for Application Server
        try {
            socket = new TServerSocket(m_nServerPortForApplication); //listenPort); -> now become random port for client.
        } catch (Exception e) {
            //already taken
            socket = null;
        }

        try {
            if (socket == null) {
                socket = new TServerSocket(0); //listenPort); -> now become random port for client.
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

    private boolean InitServerForWiera() {
        try {
            //For now we put to hard-coded port for Azure.
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

    private boolean InitServerForPeers() {
        m_peerInstanceManager = new PeerInstancesManager(this);
/*      String strReason = m_peerInstanceManager.setDataDistribution(policy);
        if (strReason != null) {
            System.out.println(strReason);
            return false;
        }
*/

        return true;
    }

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

        try {
            JSONObject req = new JSONObject();
            req.put(HOSTNAME, LocalServer.getHostName());
            req.put(IP_ADDRESS, LocalServer.getExternalIP());

            //Now client listen port will be automatically assigned.
            HashMap<String, Integer> ports = new HashMap<String, Integer>();
            ports.put(APPLICATION_PORT, m_nServerPortForApplication);
            ports.put(INSTANCE_PORT, m_nServerPortForWiera);

            if(m_bStandAloneMode == false) {
                ports.put(PEER_PORT, m_peerInstanceManager.getPeersServerPort());
            }

            req.put(VALUE, ports);

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

    //The key should include the version. That is, verionedKey is required. -KS
    public byte[] getInternal(String key, String strTierName) {
        Tier tierInfo = m_tiers.getTier(strTierName);

        if (tierInfo == null) {
            System.out.printf("Cannot find local storage Tier information: %s\n", strTierName);
            return null;
        }

        StorageInterface interf = tierInfo.getInterface();

        if (interf == null) {
            System.out.printf("Cannot find local storage Tier interface: %s\n", strTierName);
            return null;
        }

        //Start Timing
        Latency latency = new Latency();
        latency.start();

        //For test purpose
        ////////////////////////////////////////////////////////////////////////
        ///*umn.dcsg.local.test.TestUtils.simluatedDelay(10);
        //long nDelayedPeriod = m_peerInstanceManager.getDataDistribution().getSimulatedDelayPeriod();
        //////////////////////////////////////////////////////////////////////////

        byte[] ret = interf.doGet(key);

        latency.stop();

        if (m_localInfo.addTierLatency(strTierName, GET_LATENCY, latency) == false) {
            System.out.printf("Failed to get latency into stat : %.2f\n", latency.getLatencyInMills());
        }

        return ret;
    }

    public int getLatestVersion(String key) {
        MetaObjectInfo obj = getMetadata(key);

        if (obj != null) {
            return obj.getLastestVersion();
        }

        return MetaObjectInfo.NO_SUCH_VERSION;
    }

    //These 'get' function assumes that data is stored local locale.
    public byte[] get(String strKey) {
        //System.out.format("Get operation Key: %s\n", strKey);
        return get(strKey, MetaObjectInfo.NO_SUCH_VERSION);
    }

    //Any (fastest) local tier
    public byte[] get(String strKey, int nVer) {
        return get(strKey, nVer, null, false);
    }

    public byte[] get(String strKey, String strTierName) {
        return get(strKey, MetaObjectInfo.NO_SUCH_VERSION, strTierName, false);
    }

    //The bUpdateMeta will be true only if when the object is accessed by peer (forward)
    public byte[] get(String strKey, int nVer, String strTierName, boolean bUpdateMeta) {
        ReentrantReadWriteLock lock = m_keyLocker.getLock(strKey);
        String strVersionedKey = strKey;
        byte[] retVal = null;

        try {
            // Get the lock for this object
            // Lock the object down before doing anything else
            lock.readLock().lock();
            MetaObjectInfo obj = getMetadata(strKey);

            if (obj != null) {
                if (nVer < 0) {
                    nVer = obj.getLastestVersion();
                }

                if (m_bVersioningSupport == true) {
                    strVersionedKey = obj.getVersionedKey(nVer);
                }

                //Check storage tiername
                //If tier name is not specified, just retrieve from the fastest tier
                if (strTierName == null || strTierName.isEmpty() == true) {
                    strTierName = obj.getLocale(nVer, true).getTierName();
                } else if (obj.hasLocale(nVer, LocalServer.getHostName(), strTierName) == false) {
                    //Compare metadata. Check localhost locale
                    //If meta and desired storage tier is not the same, show error
                    //and use the stored storage tier.
                    Locale targetLocale = obj.getLocale(nVer, true);

                    if(targetLocale == null) {
                        System.out.printf("[debug] Failed to find available locale for the key:%s ver:%d\n", strKey, nVer);
                        return null;
                    }

                    System.out.println("[debug] Requested local Tier Name: " + strTierName + " is not in DB but found: " + targetLocale.getTierName());
                    strTierName = targetLocale.getTierName();
                }

                retVal = getInternal(strVersionedKey, strTierName);
                //System.out.println("[debug] read data:\n" + new String(retVal));

                if(bUpdateMeta == true) {
                    //update information to db only when bUpdate param is true
                    obj.countInc();
                    obj.setLAT();
                    commitMeta(obj);
                }
            } else {
                System.out.println("Failed to find meta data associated with the key for getObject: " + strKey);
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            lock.readLock().unlock();
        }

        return retVal;
    }

    HashMap<Integer, MetaVerInfo> getVersionList(String key) {
        return m_metadataStore.getVersionList(key);
    }

    //Only allow to remove local locale (storage tier)
    public boolean delete(String key, int nVer) {
        ReentrantReadWriteLock lock = m_keyLocker.getLock(key);
        try {
            // Get the lock for this object
            // Lock the object down before doing anything else
            lock.writeLock().lock();

            MetaObjectInfo obj = getMetadata(key);
            Locale localLocale;

            if (obj != null) {
                String strKey = key;

                //Latest as a Default
                if (nVer < 0) {
                    nVer = obj.getLastestVersion();
                }

                //If same versions are replicated to multiple tiers,
                //Remove all
                long lObjSize = obj.getSize();

                while (true) {
                    localLocale = obj.getLocale(nVer, true);

                    if (localLocale == null) {
                        break;
                    } else {

                        if (m_bVersioningSupport == true) {
                            strKey = obj.getVersionedKey(nVer);
                        }

                        //Remove object from the storage
                        if (deleteInternal(strKey, localLocale.getTierName(), lObjSize) == false) {
                            System.out.printf("Failed to delete key from tier: %s\n", localLocale.getTierName());
                            break;
                        }

                        //Remove locale from meta
                        obj.removeLocale(nVer, localLocale);
                    }
                }

                //Remove version
                if(m_bVersioningSupport == false || obj.getVersionList().size() == 0) {
                    m_metadataStore.deleteObject(strKey);
                }

                return true;
            } else {
                //	System.out.println("Failed to find meta data associated with the key: " + key);
            }
        } finally {
            lock.writeLock().unlock();
        }

        return false;
    }

    public boolean deleteInternal(String strKey, String strTierName, long lSize) {
        boolean ret = false;
        Tier tierInfo = m_tiers.getTier(strTierName);

        try {
            if (tierInfo != null) {
                StorageInterface interf = tierInfo.getInterface();
                if (interf != null) {
                    ret = interf.doDelete(strKey);

                    if (ret == true) {
                        tierInfo.freeSpace(lSize);
                    }
                }

                return ret;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    public boolean deleteInternal(MetaObjectInfo obj, String strTierName) {
        return deleteInternal(obj.m_key, strTierName, obj.getSize());
    }
	/*public boolean deleteObject(String key, int level)
	  {
	  boolean retVal = false;
	  write.lock();
	  retVal = (storageInterfaceMap.getObject(level)).deleteObject(key);
	  write.unlock();
	  return retVal;
	  }*/

    //This function does not consider the version. The code which calls this function should make new key. - KS
    //Storing latency for each tier is now added
    public boolean putInternal(String strKey, byte[] value, String strTierName) {
        Tier tierInfo = m_tiers.getTier(strTierName);
        StorageInterface interf;

        if (tierInfo == null) {
            System.out.printf("Cannot find storage Tier information: %s\n", strTierName);
            return false;
        }

        interf = tierInfo.getInterface();

        if (interf == null) {
            System.out.printf("Cannot find storage Tier interface: %s\n", strTierName);
            return false;
        }

        //Check free space first
        long lRequiredSpace = value.length;

        //Check already exist
        if(isVersionSupported() == false && containsKey(strKey, strTierName) == true) {
            long lSize = getMetadata(strKey).getSize();
            lRequiredSpace -= lSize;

            System.out.printf("[debug] RequiredSpace: %d size in meta: %d\n", lRequiredSpace, lSize);
        }

        //Check storage size
        if (tierInfo.checkFreeSpace(lRequiredSpace) == false) {
            System.out.printf("Storage Tier:%s does not have enough free space (freeSpace: %d).\n", strTierName, tierInfo.getFreeSpace());
            return false;
        }

        //Start Tier latency
        Latency latency = new Latency();
        latency.start();

        //Simulated delay as dynamics
        //For test purpose
        ////////////////////////////////////////////////////////////////////////
        //m_peerInstanceManager.getDataDistribution().getSimulatedDelayPeriod()
        //umn.dcsg.local.test.TestUtils.simluatedDelay();
        //////////////////////////////////////////////////////////////////////////
        boolean ret = interf.doPut(strKey, value);

        if (ret == true) {
            //tierInfo.useSpace(value.length);
            //Update used storage size
            tierInfo.useSpace(lRequiredSpace);
            System.out.printf("[debug] Tier: %s free Space: %d\n", tierInfo.getTierName(), tierInfo.getFreeSpace());

            latency.stop();

            if (m_localInfo.addTierLatency(strTierName, PUT_LATENCY, latency) == false) {
                System.out.printf("Failed to put latency into stat : %.2f\n", latency.getLatencyInMills());
            }
        }

        return ret;
    }

    //Now return key nVer
    //Let think the case, local nVer is 1 and remote nVer is 3. How to handle nVer 2. Ignore?
    //Only called in putFromPeer()
    public MetaObjectInfo updateVersion(String key, int nVer, byte[] value, String strTierName, String strTag, long modified_time, boolean bRemovePrevious) {
        boolean bRet;
        MetaObjectInfo obj = null;
        ReentrantReadWriteLock lock = m_keyLocker.getLock(key);

        try {
            lock.writeLock().lock();
            obj = getMetadata(key);

            //This should not happen
            if (obj == null) {
                System.out.println("Failed to find metadata for the key: " + key + "This should not happen!!!");
                return null;
            } else {
                String strCurTierName = obj.getLocale(true).getTierName();
                obj.updateVersion(nVer, modified_time, value.length);

                //try to updates
                String strVersionedKey = obj.getVersionedKey(nVer);
                bRet = putInternal(strVersionedKey, value, strTierName);

                if (bRet == true) {
                    //Need to handle changed location.
                    if (isVersionSupported() == false) {
                        //maybe need to remove previous replica as it is moved to other location if Tiername Changed
                        if (strCurTierName.compareTo(strTierName) != 0) {
                            //Remove previous one in this instance
                            obj.addLocale(LocalServer.getHostName(), strTierName, getLocalStorageTierType(strTierName));
                            obj.removeLocale(LocalServer.getHostName(), strCurTierName);

                            if (bRemovePrevious == true) {
                                //Todo delete operation can be done in the background with dedicated thread
                                deleteInternal(obj, strCurTierName);
                                //System.out.println("!Delete key :" + strVersionedKey + " stored in " + strCurTierName);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.writeLock().unlock();
        }

        return obj;
    }

    public MetaObjectInfo getMetadata(String strKey) {
        MetaObjectInfo obj = m_metadataStore.getObject(strKey);

        //update last accessed time
        if(obj != null) {
            obj.setLAT(System.currentTimeMillis());
        }

        return obj;
    }

    //Assume that all functions calling this function already have a lock for the key
    public MetaObjectInfo updateMetadata(String strKey, long lValueSize, String strTag, Boolean bSupportVersion) {
        MetaObjectInfo obj = getMetadata(strKey);
        long curTime = System.currentTimeMillis();

        //if not exist, create it
        if (obj == null) {
            //Create new
            obj = new MetaObjectInfo(strKey, lValueSize, strTag, bSupportVersion);
        }

        if (bSupportVersion == true) {
            //Add new version for putObject operation.
            obj.addNewVersion(curTime, lValueSize);
        }

        try {
            obj.countInc();
            obj.setLAT(curTime);
            obj.setDirty();
            obj.setLastModifiedTime(obj.getLAT()); //added for EventualConsistencyResponse consistecny for now. (will use vector or something else);
            obj.setSize(lValueSize);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return obj;
    }

    //If this not called, meta will not updated
    public boolean commitMeta(MetaObjectInfo obj) {
        try {
            m_metadataStore.putObject(obj);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    //Now return object info
    //This function only used for a new version
    //The bUpdateMeta will be true only if when the object is accessed by peer (forward)
    public MetaObjectInfo put(String strKey, byte[] value, String strTierName, String strTag, boolean bUpdateMeta) {
        ReentrantReadWriteLock lock = m_keyLocker.getLock(strKey);
        MetaObjectInfo obj;

        try {
            //System.out.format("Put operation Key: %s Size: %d TierName: %s\n", strKey, value.length, strTierName);
            lock.writeLock().lock();

            obj = updateMetadata(strKey, value.length, strTag, m_bVersioningSupport);
            if (obj != null) {
                String strVersionedKey = obj.getVersionedKey();
                if (putInternal(strVersionedKey, value, strTierName) == false) {
                    System.out.format("Failed to putObject the object key: %s\n", strVersionedKey);
                    return null;
                } else {
                    //This function should be called to update meta information
                    obj.addLocale(LocalServer.getHostName(), strTierName, getLocalStorageTierType(strTierName));

                    if(bUpdateMeta == true) {
                        commitMeta(obj);
                    }
                }
            }
        } finally {
            lock.writeLock().unlock();
        }

        return obj;
    }

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

    //Data may not be stored in local
    public boolean isLocalStorageTier(String strTierName) {
        Tier tierInfo = m_tiers.getTier(strTierName);
        return tierInfo != null;
    }

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

    public boolean containsKey(String strKey, String strTierName) {
        return containsKey(strKey, MetaObjectInfo.LATEST_VERSION, strTierName);

    }

    //Data may not be stored in local
    public boolean containsKey(String strKey, int nVer, String strTierName) {
        MetaObjectInfo obj = getMetadata(strKey);

        if(obj != null) {
            return obj.hasLocale(nVer, Locale.getLocaleID(LocalServer.getHostName(), strTierName));
        }

        return false;
    }

    //For now, simple call renaming api of each storage interface stored
    public boolean rename(String strSrcKey, String strNewKey) {
        //Currently, no file version.
        //Check it can be found
        /*MetaObjectInfo obj = getMetadata(strSrcKey);
        MetaVerInfo metaVerInfo;
        Map <Integer, MetaVerInfo> map = obj.getVersionList();

        Map <String, Locale> localeMap;
        Locale locale;

        for(Integer ver: map.keySet()) {
            metaVerInfo = map.get(ver);
            localeMap = metaVerInfo.getLocaleList();

            //Now assume that rename support only local locale
            for(String strLocale: localeMap.keySet()) {
                locale = localeMap.get(strLocale);
                m_tiers.getTierInterface() locale.getLocaleID()

            }
        }*/
        return false;

    }

    public boolean copy(String strSrcKey, String strNewKey) {
        return false;
    }
}