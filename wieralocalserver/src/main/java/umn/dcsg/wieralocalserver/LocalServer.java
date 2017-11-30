package umn.dcsg.wieralocalserver;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import umn.dcsg.wieralocalserver.info.OperationLatency;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;
import org.apache.commons.cli.*;
import org.json.JSONObject;
import umn.dcsg.wieralocalserver.thriftinterfaces.LocalServerToWieraIface;
import umn.dcsg.wieralocalserver.thriftinterfaces.WieraToLocalServerIface;

import java.io.*;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;

import static umn.dcsg.wieralocalserver.Constants.*;

//Todo Create LocalServerWiera thrift server.
//Send server info to Wiera LocalInstance Server Manager with thrift
public class LocalServer {
	private static String m_strExternalIP = null;
	private static String m_strHostName = null;
	private static String m_strFQDN = null;
	private static String m_strZKServerIP = null;

	private static String m_strWieraIP = null;
	private static long m_lWieraPort = 0;
	LocalServerToWieraIface.Client m_wieraClient;
	TServer m_localServer;
	int m_nLocalServerPort = 0;

	//For printing latency info automatically. (For testing purpose)
	boolean m_isAutoLatency = false;
	Map<String, LaunchLocalInstance> m_instanceList = null;

	public static String getZKServerIP() {
		return m_strZKServerIP;
	}

	//This instance can be shared in applications as specified in
	//http://curator.apache.org/curator-framework/
	private static CuratorFramework m_zkClient;

	/*
	*Nan: Because one computer may have more than one network interface card.
	*the developer can specify the interface card he/she wants to use by providing the selfIP
	*This is important when we using Wiera in a LAN. Using http://checkip.amazonaws.com to check IP is not acceptable.
	* */

	/**
	 * Initialize parameters
	 * Get a client object that is served by the Wiera central server.
	 * Generate a server for the Wiera central server.
	 * Parameters
	 * 1. m_lWieraPort: The Port that the Wiera central server listens for local instances.
	 * 2. m_strWieraIP: The IP address of the Wiera central server.
	 * 3. m_strZKServerIP
	 * 4. m_strExternalIP: the IP address of this machine.
	 * 5. m_instanceList: all the launched instance. It is empty initially.
	 *
	 * 6. m_wieraClient: a LocalServerToWieraIface.Client object, which is a client of the Wiera central server
	 * 		Services:
	 *		1). registerLocalServer(String ); // Telling the central server its IP, Port, and hostname (machine name if you are in a LAN)
	 *
	 *
	 * 7. m_localServer: a TServer (Core class: WieraToLocalServerInterface) object that provides service for Wiera Central Server []
	 * 		Serivces:
	 * 		1). ping
	 * 		2). startInstance, a wieraID used to distinguish different local instances,
	 * 						   a nInstanceCnt indicates how many local instances this Wiera has. (if just one instance, it will run as standalone mode)
	 * 						   a policy file specified the rules.
	 * 		3).	stopInstance, a wieraID used to identify the running local instance.
	 *
	 * 8. m_zkClient ?????
	 *
	 * */

	public LocalServer(String strWieraIP, int nWieraPort, String strZKServerIP, String selfIP) {
		long lRetryTime = 1;
		m_lWieraPort = nWieraPort;
		m_strWieraIP = strWieraIP;
		m_strZKServerIP = strZKServerIP;
		m_strExternalIP = selfIP;
		m_instanceList = new HashMap<String, LaunchLocalInstance>();

		while (true) {
			/*
			* If failing to connect to the Wiera Central Server, this will retry and increase the time gap gradually.
			* */
			m_wieraClient = initWieraClient();

			if (m_wieraClient != null) {
				break;
			}

			if (lRetryTime == 1) {
				System.out.format("Fail to connect to server %s:%d. Try again %d sec later\n", m_strWieraIP, m_lWieraPort, lRetryTime);
			} else {
				System.out.format("Fail to connect to server %s:%d. Try again %d secs later\n", m_strWieraIP, m_lWieraPort, lRetryTime);
			}

			try {
				Thread.sleep(1 * lRetryTime * 1000);
			} catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}

			lRetryTime = lRetryTime * 2;
		}

		//Now try to create thrift server.
		try {
			//To do need to change the port for later.
			//For now hard coded port for Azure.
			//TServerSocket tSocket = new TServerSocket(9095);
			TServerSocket tSocket = new TServerSocket(0);
			TServerTransport serverTransport = tSocket;
			TProtocolFactory tProtocolFactory = new TBinaryProtocol.Factory(true, true);
			TTransportFactory transportFactory = new TFramedTransport.Factory();
			WieraToLocalServerInterface localServerInterface = new WieraToLocalServerInterface(this);
			WieraToLocalServerIface.Processor processor = new WieraToLocalServerIface.Processor(localServerInterface);
			m_localServer = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).minWorkerThreads(3) // TODO Take this as arguments from user
					.maxWorkerThreads(10) // TODO Take this as arguments from user
					.inputTransportFactory(transportFactory).outputTransportFactory(transportFactory).inputProtocolFactory(tProtocolFactory).outputProtocolFactory(tProtocolFactory).processor(processor));

			m_nLocalServerPort = tSocket.getServerSocket().getLocalPort();
			System.out.format("My port for wiera: %d\n", m_nLocalServerPort);
		} catch (TTransportException e) {
			e.printStackTrace();
		}

		//Zookeeper instance
		//LocalServer instance will be singletone in whole execution
		//m_zkClient is declared as static.
		m_zkClient = CuratorFrameworkFactory.newClient(LocalServer.getZKServerIP() + ":2181", new ExponentialBackoffRetry(1000, 3));

		if (m_zkClient!= null) {
			m_zkClient.start();
			System.out.println("Zookeeper Client starts.");
		} else {
			System.out.println("Seems there is no ZK running.");
		}

		//Now try to let Wiera know this LocalInstance Server is ready to be used
		JSONObject localServerInfo = new JSONObject();

		localServerInfo.put(IP_ADDRESS, getExternalIP());
		localServerInfo.put(LOCAL_SERVER_PORT, getLocalServerPort());
		localServerInfo.put(HOSTNAME, getHostName());
		System.out.println("Local server info: " + localServerInfo.toString());
		try {

			m_wieraClient.registerLocalServer(localServerInfo.toString());
		} catch (TException e) {
			System.out.println(localServerInfo.toString());
			e.printStackTrace();
		}
	}

	public LocalServer(String strWieraIP, int nWieraPort, String strZKServerIP){
		this(strWieraIP, nWieraPort, strZKServerIP, getExternalIP());
	}

	long getLocalServerPort() {
		return m_nLocalServerPort;
	}

	public Map<String, LaunchLocalInstance> getInstanceList() {
		return m_instanceList;
	}

	LocalServerToWieraIface.Client initWieraClient() {
		TTransport transport;
		transport = new TSocket(m_strWieraIP, (int) m_lWieraPort);
		TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
		LocalServerToWieraIface.Client client = new LocalServerToWieraIface.Client(protocol);

		try {
			transport.open();
		} catch (TException x) {
			x.printStackTrace();
			return null;
		}

		return client;
	}
	/**
	 * Run the local server based on WieraToLocalServerInterface class.
	 * Listening the commands from Wiera central server.
	 * Commands include: ping, startInstance, stopInstance.
	 * */
	public void runForever() {
		System.out.format("LocalInstance Server now waiting requests from Wiera: %d\n", getLocalServerPort());
		m_localServer.serve();
	}

	public static String getExternalIP() {

		if (m_strExternalIP == null) {
			try {
				URL url = new URL("http://checkip.amazonaws.com");
				BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));
				String strExternalIP = in.readLine();

				if (m_strFQDN == null) {
					m_strFQDN = InetAddress.getByName(m_strExternalIP).getCanonicalHostName();
				}

				return strExternalIP;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return m_strExternalIP;
	}

	public static String getExternalDNS() {
		if (m_strExternalIP == null) {
			m_strExternalIP = getExternalIP();
		}

		try {
			if (m_strFQDN == null) {
				m_strFQDN = InetAddress.getByName(m_strExternalIP).getCanonicalHostName();
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

		return m_strFQDN;
	}

	public static String getHostName() {
		if (m_strHostName == null) {
			try {
				m_strHostName = InetAddress.getLocalHost().getHostName();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}

		return m_strHostName;
	}

	/**
	 * Called by the Wiera Central server via thrift to check whether the local server is alive, not a instance.
	 */
	public String pingFromWiera() {
		JSONObject response = new JSONObject();
		response.put(RESULT, true);
		response.put(VALUE, "");    //This will be LocalInstance Server info

		return response.toString();
	}
	/**
	 * Called by the Wiera Central server via thrift to create new local instance.
	 * Create a new thread to run this new instance.
	 * A single local server can hold multiple instances. But it can only have one instance for a specific Wiera group.
	 * What is a Wiera group:
	 * 	Each server can have multiple instances. These instances may belong to different applications. So a Wiera group means that
	 * 	a group of local instances that belong to the same application. We use wieraID to distinguish and identify groups.
	 * */
	public String startInstance(JSONObject policy) {
		JSONObject response = new JSONObject();

		//Get WieraID First
    	String strWieraID = policy.getString(ID);
    	int nInstanceCnt = policy.getInt(INSTANCE_CNT);
		//nInstanceCnt
        if (strWieraID == null || strWieraID.length() == 0){
			response.put(RESULT, false);
			response.put(VALUE, "Failed to getObject policy id from policy"); //Maybe LocalInstance info?
		} else {
			LaunchLocalInstance instanceLauncher = new LaunchLocalInstance(policy, nInstanceCnt == 1);
			Thread newLocalInstance = (new Thread(instanceLauncher));
			newLocalInstance.start();

			//Nan: a single local server can have multiple instances. But it can only have one instance for a specific Wiera group.
			m_instanceList.put(strWieraID, instanceLauncher);

			response.put(RESULT, true);
			response.put(VALUE, ""); //Maybe LocalInstance info?
		}

		return response.toString();
	}
	/**
	 * Called by the Wiera Central server via thrift to stop a local instance.
	 */
	public String stopInstance(JSONObject policy) {
		JSONObject response = new JSONObject();
		response.put(RESULT, true);

		//Get WieraID First
		String strWieraID = (String) policy.get(ID);

		if (strWieraID == null || strWieraID.length() == 0) {
			response.put(RESULT, false);
			response.put(VALUE, "Failed to find policy id from policy"); //Maybe LocalInstance info?
		} else {
			LaunchLocalInstance instanceLauncher = m_instanceList.get(strWieraID);

			if (instanceLauncher.m_localInstance.shutdown() == true) {
				response.put(RESULT, true);
				response.put(VALUE, "The instance is terminated");
			} else {
				response.put(RESULT, false);
				response.put(VALUE, "Fail to terminate the instance");
			}

			//Remove Wiera ID from the list
			m_instanceList.remove(strWieraID);
		}
		return response.toString();
	}
	//End of thrift callback functions

	//Parse execute parameter
	static CommandLine checkParams(String[] args) {
		Options options = new Options();

		//Wiera IP Location option
		//If not specified LocalInstance is running stand-alone mode
		Option input = new Option("w", "wiera", true, "Wiera Server IP address");
		input.setRequired(false);
		options.addOption(input);

		Option policy = new Option("pp", "policypath", true, "LocalInstance policy path for stand-alone mode");
		input.setRequired(false);
		options.addOption(policy);

		Option port = new Option("p", "port", true, "LocalInstance instance port");
		input.setRequired(false);
		options.addOption(port);

		Option selfIP = new Option("e", "externalIP", true, "the interface IP wanted to use");
		input.setRequired(false);
		options.addOption(selfIP);
		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd;

		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			System.out.println(e.getMessage());
			formatter.printHelp("java LocalServer", options);
			return null;
		}

		return cmd;
	}

	//Main function
	public static void main(String[] args) throws NoSuchFieldException, IOException {
		/*
		Check standalone or not for later.
		-w run with Wiera central server.
		-p the listening port of Wiera central server.
		-e an IP address that indicates which interface is going to use.
		*/
		CommandLine cmd = checkParams(args);
		if (cmd == null) {
			return;
		}

		int nWieraPort = WIERA_PORT_FOR_LOCAL_SERVER;
		final LocalServer localServer;

		if (cmd.hasOption('w') == true) {
		    if(cmd.hasOption('p') == true) {
                nWieraPort = Integer.parseInt(cmd.getOptionValue("p"));
            }
			if(cmd.hasOption('e') == true){
				localServer = new LocalServer(cmd.getOptionValue("w"), nWieraPort, cmd.getOptionValue("w"), cmd.getOptionValue("e"));
			}else {
				//Running LocalServer for running multiple instances
				localServer = new LocalServer(cmd.getOptionValue("w"), nWieraPort, cmd.getOptionValue("w"));
			}
			runLocalServer(localServer);
		}else{
			String strPolicyPath = "policy_example/low_latency.json";
			int nPort = 55556;

			//Policy location
			if (cmd.hasOption("pp") == true) {
				strPolicyPath = cmd.getOptionValue("pp");
			}

			File policyFile = new File(strPolicyPath);
			String strPolicy;

			if (policyFile.exists() == true && policyFile.isDirectory() == false) {
				byte[] encoded = Files.readAllBytes(policyFile.toPath());
				strPolicy = new String(encoded);
			} else {
				System.out.println("Failed to find policy file \"" + strPolicyPath + "\"");
				return;
			}

			//Read policy file.
			JSONObject jsonWieraPolicy = new JSONObject(strPolicy);
			JSONObject jsonLocalInstancePolicy;

			//For consistency
			if(jsonWieraPolicy.has(HOSTNAME_LIST) == true) {
				jsonLocalInstancePolicy = jsonWieraPolicy.getJSONObject(HOSTNAME_LIST);
				jsonLocalInstancePolicy.put(ID, jsonWieraPolicy.getString(ID));
			} else {
				jsonLocalInstancePolicy = jsonWieraPolicy;
			}

			//Running LocalInstance without Wiera
			LocalInstance m_instance = new LocalInstance("config.txt", jsonLocalInstancePolicy, true);
			m_instance.runForever(jsonLocalInstancePolicy);
		}
	}

	static void runLocalServer(final LocalServer localServer) {
		/*
		The important function for running Wiera with a central server.
		1. Start a new thread for running a simple CLI tool.
		2. Start this local server .runForever()
		 */
		(new Thread(new Runnable() {
			@Override
			public void run() {
				InputStreamReader isr = new InputStreamReader(System.in);
				BufferedReader in = new BufferedReader(isr);
				String data;
				String[] tokens;
				boolean bResult;
				final String delims = "[ ]+";
				Timer autoReqTimer = new Timer();

				//Set timer with lHowLong
				TimerTask task = new TimerTask() {
					@Override
					public void run() {
						if (localServer.m_isAutoLatency == false) {
							return;
						}

						long lCurTime;
						Map<String, LaunchLocalInstance> instanceList = localServer.getInstanceList();

						lCurTime = System.currentTimeMillis();

						for (LaunchLocalInstance launcher : instanceList.values()) {
							//DataDistributionUtil consistencyPolicy = launcher.m_localInstance.m_peerInstanceManager.getDataDistribution();

							SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
							Date resultDate = new Date(lCurTime);
							String strTime = dateFormat.format(resultDate);

							//System.out.format("[Req][%s] Total: %d(%d), Forwarded: %d(%d), Forwarding: %d(%d) Get: %d(%d)\n", strTime, launcher.m_localInstance.m_localInfo.get.getTotalPutReqCnt(), consistencyPolicy.getTotalPutReqCnt(lCurTime, 30), consistencyPolicy.getForwardedPutReqCnt(), consistencyPolicy.getForwardedPutReqCnt(lCurTime, 30), consistencyPolicy.getForwardingPutReqCnt(), consistencyPolicy.getForwardingPutReqCnt(lCurTime, 30), consistencyPolicy.getTotalGetReqCnt(), consistencyPolicy.getTotalGetReqCnt(true));
							break;
						}
					}
				};

				autoReqTimer.scheduleAtFixedRate(task, 0, 5000);    //Millisecond

				// stay alive until told to quit
				while (true) {
					try {
						if (System.in.available() >= 0) {
							data = in.readLine();
							tokens = data.split(delims);

							// We really don't do a very clean shutdown, there should
							// ideally be a mutex to issue a shutdown!
							if (tokens[0].toUpperCase().equals("QUIT") == true || tokens[0].toUpperCase().equals("Q")) {
								System.out.println("LocalInstance Server exit now");
								System.exit(1);
							} else if (data.equals("p") == true) {
								//instanceLauncher = new LaunchLocalInstance();
								//Thread newLocalInstance = (new Thread(instanceLauncher));
								//newLocalInstance.start();
							} else if (tokens[0].toUpperCase().equals("DELAY") == true || tokens[0].toUpperCase().equals("D") == true) {
								Map<String, LaunchLocalInstance> instanceList = localServer.getInstanceList();

								if (tokens.length == 3) {
									//Now only consider the first instance of list
									for (LaunchLocalInstance launcher : instanceList.values()) {
										long lPeriod = Integer.parseInt(tokens[1]);
										long lHowLong = Integer.parseInt(tokens[2]);
										//launcher.m_localInstance.m_peerInstanceManager.getDataDistribution().setSimulatedDelayPeriod(lPeriod, lHowLong);
										break;
									}
								}
							} else if (tokens[0].toUpperCase().equals("ALL") == true || tokens[0].toUpperCase().equals("A") == true) {
								Map<String, LaunchLocalInstance> instanceList = localServer.getInstanceList();

								//Now only consider the first instance of list
								for (LaunchLocalInstance launcher : instanceList.values()) {
									launcher.m_localInstance.m_localInfo.printAllInfo();
									break;
								}
							} else if (tokens[0].toUpperCase().equals("OPER_LA_CLEAR") == true || tokens[0].toUpperCase().equals("OLC") == true) {
								Map<String, LaunchLocalInstance> instanceList = localServer.getInstanceList();

								for (LaunchLocalInstance launcher : instanceList.values()) {
									HashMap<String, ConcurrentLinkedDeque<OperationLatency>> list = launcher.m_localInstance.m_localInfo.getOperationInfo();
									ConcurrentLinkedDeque<OperationLatency> getList = list.get(GET_LATENCY);
									ConcurrentLinkedDeque<OperationLatency> putList = list.get(PUT_LATENCY);

									getList.clear();
									putList.clear();

									//cost infor clear
									launcher.m_localInstance.m_localInfo.clearAggregatedCostInfo();
									launcher.m_localInstance.m_localInfo.clearRequestsCnt();
									break;
								}

								System.out.println("Operation information is cleared");
							} else if (tokens[0].toUpperCase().equals("OPER_LA") == true || tokens[0].toUpperCase().equals("OL") == true) {
								Map<String, LaunchLocalInstance> instanceList = localServer.getInstanceList();

								for (LaunchLocalInstance launcher : instanceList.values()) {
									HashMap<String, ConcurrentLinkedDeque<OperationLatency>> list = launcher.m_localInstance.m_localInfo.getOperationInfo();
									OperationLatency latency;
									ConcurrentLinkedDeque<OperationLatency> getList = list.get(GET_LATENCY);
									ConcurrentLinkedDeque<OperationLatency> putList = list.get(PUT_LATENCY);

									Iterator<OperationLatency> iter = getList.iterator();

									System.out.println("Get Operation - Time, Target Tier, Latency, Storage, Consistency");

									PrintWriter writer = new PrintWriter("OL.txt", "UTF-8");

									while (iter.hasNext()) {
										latency = iter.next();

										SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
										Date resultDate = new Date(latency.getStartDate());
										String strTime = dateFormat.format(resultDate);

										//String strOutput = String.format("%s %s %.2f %.2f %d", strTime, latency.getLocaleID(), latency.getLatency(), latency.getLocalOperationTime(), latency.getOperationName());
										//System.out.println(strOutput);
										//writer.println(strOutput);
									}

									iter = putList.iterator();

									System.out.println("Put Operation - Time, Target Tier, Latency, Broadcast, Storage, Consistency");

									while (iter.hasNext()) {
										latency = iter.next();

										SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
										Date resultDate = new Date(latency.getStartDate());
										String strTime = dateFormat.format(resultDate);

										//String strOutput = String.format("%s %s %.2f %.2f %.2f %d", strTime, latency.getLocaleID(), latency.getLatency(), latency.getBroadcastTime(), latency.getLocalOperationTime(), latency.getOperationName());
										//System.out.println("");
										//writer.println(strOutput);
									}

									writer.close();

									launcher.m_localInstance.m_localInfo.printOperationLatency();


									break;
								}
							} else if (tokens[0].toUpperCase().equals("REQUEST") == true || tokens[0].toUpperCase().equals("R") == true) {
								localServer.m_isAutoLatency = !localServer.m_isAutoLatency;
							} else if (tokens[0].toUpperCase().equals("LATENCIES") == true || tokens[0].toUpperCase().equals("LA") == true) {
								Map<String, LaunchLocalInstance> instanceList = localServer.getInstanceList();

								//Now only consider the first instance of list
								for (LaunchLocalInstance launcher : instanceList.values()) {
									launcher.m_localInstance.m_localInfo.printLatencyInfo();
									break;
								}
							} else if (tokens[0].toUpperCase().equals("COST") == true) {
								Map<String, LaunchLocalInstance> instanceList = localServer.getInstanceList();

								//Now only consider the first instance of list
								for (LaunchLocalInstance launcher : instanceList.values()) {
									launcher.m_localInstance.m_localInfo.printAggregatedCostInfo();
									break;
								}

							} else if (tokens[0].toUpperCase().equals("OPER") == true) {
								Map<String, LaunchLocalInstance> instanceList = localServer.getInstanceList();

								//Now only consider the first instance of list
								for (LaunchLocalInstance launcher : instanceList.values()) {
									launcher.m_localInstance.m_localInfo.printOperationTime();
									break;
								}

							} else if (tokens[0].toUpperCase().equals("Request") == true || tokens[0].toUpperCase().equals("R") == true) {
								localServer.m_isAutoLatency = !localServer.m_isAutoLatency;
							}
						}
					} catch (IOException io) {
						io.printStackTrace();
					}
				}
			}
		})).start();

		localServer.runForever();
	}

	public static CuratorFramework getCuratorFramework() {
		return m_zkClient;
	}
}