package umn.dcsg.wieralocalserver.test;

//import com.backblaze.erasure.ReedSolomon;
import com.google.common.primitives.Bytes;
import org.apache.commons.cli.*;
import org.apache.commons.math3.distribution.TDistribution;
import org.apache.commons.math3.exception.MathIllegalArgumentException;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
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
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static umn.dcsg.wieralocalserver.Constants.*;

public class WieraClient {
    public static ApplicationToWieraIface.Client getWieraClient(String strIPAddress, int nPort) {
        TTransport transport;

        transport = new TSocket(strIPAddress, nPort);
        TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
        ApplicationToWieraIface.Client client = new ApplicationToWieraIface.Client(protocol);

        try {
            transport.open();
        } catch (TException x) {
            System.out.println(x.getMessage());
            return null;
        }

        return client;
    }

    public static ApplicationToLocalInstanceIface.Client getNewConnection(String strIPAddress, int nPort) {
        TTransport transport;

        transport = new TSocket(strIPAddress, nPort);
        TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport, 1048576 * 200));
        ApplicationToLocalInstanceIface.Client client;

        try {
            transport.open();
            client = new ApplicationToLocalInstanceIface.Client(protocol);
        } catch (TException x) {
            x.printStackTrace();
            return null;
        }

        return client;
    }

    public static class SimulatedClient implements Runnable {
        long m_lStartTime = 0;
        long m_lRegionTime = 0;
        long m_lPeriod = 0; //in Second
        String m_strIP;
        String m_strHost;
        int m_nPort;
        List<Double> m_probability;

        public SimulatedClient(String strHost, String strIP, int nPort, long lStartTime, long lRegionTime, long lPeriod, List<Double> probability) {
            m_strHost = strHost;
            m_strIP = strIP;
            m_nPort = nPort;
            m_lStartTime = lStartTime;
            m_lRegionTime = lRegionTime;
            m_lPeriod = lPeriod;
            m_probability = probability;
        }

        @Override
        public void run() {
            run_clients(m_strHost, m_strIP, m_nPort, m_lStartTime, m_lRegionTime, m_lPeriod, m_probability);
        }
    }

    //Parse execute parameter
    static CommandLine checkParams(String[] args) {
        Options options = new Options();

        //Wiera IP Location option
        //If not specified LocalInstance is running stand-alone mode
        Option input = new Option("w", "wieraip", true, "Wiera Server IP address");
        input.setRequired(false);
        options.addOption(input);

        Option policy = new Option("f", "filepath", true, "Wiera policy file path for stand-alone mode");
        input.setRequired(false);
        options.addOption(policy);

        Option port = new Option("p", "wieraport", true, "Wiera server port");
        input.setRequired(false);
        options.addOption(port);

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            System.out.println(e.getMessage());
            formatter.printHelp("java LocalInstanceClient", options);
            return null;
        }

        return cmd;
    }

    public static void main(String[] args) {
        //Check standalone or not for later.
        //Now it should use Wiera
        CommandLine cmd = checkParams(args);
        if (cmd == null) {
            return;
        }

        String strPolicyPath = "low_latency.json";
        String strWieraIPAddress = "127.0.0.1";
        int nWieraPort = WIERA_PORT_FOR_APPLICATIONS;

        if (cmd.hasOption("w") == true) {
            strWieraIPAddress = cmd.getOptionValue("w");
        }

        //Policy location
        if (cmd.hasOption("f") == true) {
            strPolicyPath = cmd.getOptionValue("f");
        }

        if (cmd.hasOption("p") == true) {
            nWieraPort = Integer.parseInt(cmd.getOptionValue("p"));
        }

        File policyFile = new File(strPolicyPath);

        if (policyFile.exists() == true && policyFile.isDirectory() == false) {
//                byte[] encoded = Files.readAllBytes(policyFile.toPath());
            //              strPolicy = new String(encoded);
        } else {
            System.out.println("Failed to find policy file \"" + strPolicyPath + "\"");
            return;
        }

        //Read policy file.
        //JSONObject jsonPolicy = new JSONObject(strPolicy);

        //Running LocalInstance without Wiera
        //LocalInstance m_instance = new LocalInstance("config.txt", jsonPolicy, true);
        //m_instance.runForever(jsonPolicy);

        //JSONObject jsonPolicy = createPolicy("test", 3, "test");
        //JSONObject policy_example = jsonPolicy.getJSONObject(LOCAL_INSTANCES).getJSONObject("aws-us-east");
        //initEvents(policy_example);

        JSONArray serverList = null;
        ApplicationToWieraIface.Client wieraClient = getWieraClient(strWieraIPAddress, nWieraPort);

        boolean bConnectedToWiera = false;

        if (wieraClient != null) {
            bConnectedToWiera = true;
            try {
                String strResult = wieraClient.getLocalServerList();
                JSONObject res = new JSONObject(strResult);

                //getObject list of local instance.
                boolean bResult = (boolean) res.get(RESULT);

                if (bResult == true) {
                    //Get Server List
                    serverList = (JSONArray) res.get(VALUE);
                    System.out.printf("Current connected LocalInstance Server List (%d)\n", serverList.length());
                    System.out.println(serverList);
                } else {
                    System.out.println("Failed to getObject LocalInstance Server List.");
                }
            } catch (TException e) {
                e.printStackTrace();
            }
        } else {
            //Fail to connect to Wiera
            System.out.println("Failed to connect to Wiera. You will need to connect to Wiera manually.");
        }

        InputStreamReader isr = new InputStreamReader(System.in);
        BufferedReader in = new BufferedReader(isr);
        String data;
        long start_time = 0;
        long end_time = 0;
        double elapsed = 0;
        ApplicationToLocalInstanceIface.Client localInstanceClient = null;
        String strResult;
        String strValue;
        String strHostName = "What the !!!!";
        String[] tokens;
        String strLocalInstanceIP = null;
        int nLocalInstancePort = 0;
        String strLastID = null;

        boolean bResult;
        final String delims = "[ ]+";

        // stay alive until told to quit
        JSONArray instanceList = null;

        while (true) {
            try {
                if (System.in.available() >= 0) {
                    data = in.readLine();
                    tokens = data.split(delims);
                    // We really don't do a very clean shutdown, there should
                    // ideally be a mutex to issue a shutdown!
                    // Command to Wiera
                    if (tokens[0].toUpperCase().equals("CONNECTWIERA") == true || tokens[0].toUpperCase().equals("CW") == true) {
                        wieraClient = getWieraClient(strWieraIPAddress, nWieraPort);

                        if (wieraClient != null) {
                            bConnectedToWiera = true;
                            System.out.println("Connect to Wiera successfully.");
                        } else {
                            bConnectedToWiera = false;
                            System.out.println("Failed to connect to Wiera");
                        }
                    } else if (tokens[0].toUpperCase().equals("START") == true || tokens[0].toUpperCase().equals("S") == true) {
                        int nSize = serverList.length();
                        LinkedList regions = new LinkedList();
                        JSONArray serverInfo;

                        for (int i = 0; i < nSize; i++) {
                            serverInfo = (JSONArray) serverList.get(i);
                            strHostName = (String) serverInfo.get(0);
                            regions.add(strHostName);
                        }

                        if (wieraClient != null) {
                            JSONObject jsonReq = PolicyGenerator.loadPolicy(strPolicyPath);
                            String strReq = jsonReq.toString();

                            strResult = wieraClient.startInstances(strReq);
                            JSONObject res = new JSONObject(strResult);

                            bResult = (boolean) res.get(RESULT);

                            //this will include WieraID or reason if failed
                            strValue = (String) res.get(VALUE);

                            if (bResult == true) {
                                try {
                                    Thread.sleep(3000);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }

                                instanceList = getInstanceList(wieraClient, strValue);
                                System.out.println(instanceList.toString());
                                strLastID = strValue;
                            } else {
                                System.out.println("Failed to start instance reason: " + strValue);
                            }
                        } else {
                            System.out.printf("Wiera Thrift is not running");
                        }
                    } else if (tokens[0].toUpperCase().equals("STOP") == true || tokens[0].toUpperCase().equals("ST") == true) {
                        //Create instance here with global Policy.
                        if (tokens.length > 2) {
                            System.out.println("Usage: stop(st) string:policyID");
                            continue;
                        }

                        if (wieraClient != null) {
                            JSONObject jsonReq = new JSONObject();
                            jsonReq.put(ID, tokens[1]);
                            String strReq = jsonReq.toString();

                            strResult = wieraClient.stopInstances(strReq);
                            JSONObject res = new JSONObject(strResult);

                            bResult = (boolean) res.get(RESULT);

                            //this will include WieraID or reason if failed
                            strValue = (String) res.get(VALUE);

                            if (bResult == true) {
                                System.out.println("Stop instances successfully.");
                            } else {
                                System.out.println("Failed to stop instance reason: " + strValue);
                            }
                        } else {
                            System.out.printf("Wiera Thrift is not running");
                        }
                    } else if (tokens[0].toUpperCase().equals("SHOWLIST") == true || tokens[0].toUpperCase().equals("LIST") == true) {
                        if (instanceList != null) {
                            //Get instance List
                            int lLength = instanceList.length();
                            System.out.println("------------------------  Instance List  ------------------------");

                            for (int i = 0; i < lLength; i++) {
                                JSONArray instance = (JSONArray) instanceList.get(i);

                                System.out.format("Hostname: %s, IP: %s, Port: %d\n", instance.get(0), instance.get(1), instance.get(2));
                            }
                        }
                    } else if (tokens[0].toUpperCase().equals("CONNECT") == true || tokens[0].toUpperCase().equals("C") == true) {
                        if (instanceList != null) {
                            boolean bFound = false;
                            String strParam = tokens[1];
                            long lLength = instanceList.length();

                            for (int i = 0; i < lLength; i++) {
                                JSONArray instance = (JSONArray) instanceList.get(i);

                                if (strParam.equals(instance.get(0)) == true) {
                                    strHostName = (String) instance.get(0);
                                    strLocalInstanceIP = (String) instance.get(1);
                                    nLocalInstancePort = (int) instance.get(2);
                                    bFound = true;
                                    break;
                                }
                            }

                            if (bFound == false) {
                                System.out.println("Invalid hostname.");
                            } else {
                                System.out.println("Done to connect.");
                            }
                        } else {
                            System.out.println("Get instance list first by GIL command.");
                        }
                    } else if (tokens[0].toUpperCase().equals("GETINSTANCELIST") == true || tokens[0].toUpperCase().equals("GIL") == true) {
                        if (bConnectedToWiera == false) {
                            System.out.println("Need to create instances first.");
                            continue;
                        }

                        //Create instance here with global Policy.
                        if (tokens.length != 2) {
                            System.out.println("Usage: GetInstanceList(gil)  string:policyID");
                            continue;
                        }

                        instanceList = getInstanceList(wieraClient, tokens[1]);
                        strLastID = tokens[1];
                    }
                    ////////////////////////////////////////////////////////////////////////////////////////////////
                    // From now on command to LocalInstance
                    else if (tokens[0].toUpperCase().equals("UPDATE") == true || tokens[0].toUpperCase().equals("U") == true) {
                        if (bConnectedToWiera == false || instanceList == null) {
                            System.out.println("Need to connet to Wiera first.");
                            continue;
                        }

                        if (instanceList.length() <= 0) {
                            System.out.println("No available local instance. Need to update instance list with WieraID.");
                            continue;
                        }
                    } else if (tokens[0].toUpperCase().equals("REMOVE") == true || tokens[0].toUpperCase().equals("R") == true) {
                        if (bConnectedToWiera == false || instanceList == null) {
                            System.out.println("Need to connet to Wiera first.");
                            continue;
                        }

                        if (instanceList.length() <= 0) {
                            System.out.println("No available local instance. Need to update instance list with WieraID.");
                            continue;
                        }
                    } else if (tokens[0].toUpperCase().equals("SET") == true) {
                        if (bConnectedToWiera == false || instanceList == null) {
                            System.out.println("Need to connet to Wiera first.");
                            continue;
                        }

                        //If not set find the closest one.
                        if (nLocalInstancePort == 0 || strLocalInstanceIP == null) {
                            if (instanceList.length() <= 0) {
                                System.out.println("No available local instance. Need to update instance list with WieraID.");
                                continue;
                            } else {
                                JSONArray closestInstance = (JSONArray) instanceList.get(0);
                                strHostName = (String) closestInstance.get(0);
                                strLocalInstanceIP = (String) closestInstance.get(1);
                                nLocalInstancePort = (int) closestInstance.get(2);
                            }
                        }

                        if (tokens.length != 3) {
                            System.out.println("Usage: set(s) string:key binary:value");
                            continue;
                        }

                        //start_time = System.nanoTime();
                        localInstanceClient = getNewConnection(strLocalInstanceIP, nLocalInstancePort);

                        if (localInstanceClient != null) {
                            start_time = System.currentTimeMillis();
                            set(localInstanceClient, tokens[1], tokens[2]);
                            //end_time = System.nanoTime();
                            end_time = System.currentTimeMillis();
                            elapsed = end_time - start_time;
                            System.out.printf("Sent to : %s - Elapsed for 'set': %f\n", strHostName, elapsed);
                        } else {
                            System.out.printf("Falied to connection to the server");
                        }
                    } else if (tokens[0].toUpperCase().equals("GET") == true || tokens[0].toUpperCase().equals("G") == true) {
                        if (bConnectedToWiera == false || instanceList == null) {
                            System.out.println("Need to connet to Wiera first.");
                            continue;
                        }

                        if (instanceList.length() <= 0) {
                            System.out.println("No available local instance. Need to update instance list with WieraID.");
                            continue;
                        }

                        if (tokens.length <= 1 || tokens.length >= 4) {
                            System.out.println("Usage: getObject(g) String:key [Integer:version]");
                            continue;
                        }

                        long nVersion = MetaObjectInfo.NO_SUCH_VERSION;

                        if (tokens.length == 3) {
                            try {
                                nVersion = Integer.parseInt(tokens[2]);
                            } catch (NumberFormatException e) {
                                System.out.println("Version should be Integer type.");
                                continue;
                            }
                        }

                        //If not set find the closest one.
                        if (nLocalInstancePort == 0 || strLocalInstanceIP == null) {
                            if (instanceList.length() <= 0) {
                                System.out.println("No available local instance. Need to update instance list with WieraID.");
                                continue;
                            } else {
                                JSONArray closestInstance = (JSONArray) instanceList.get(0);
                                strHostName = (String) closestInstance.get(0);
                                strLocalInstanceIP = (String) closestInstance.get(1);
                                nLocalInstancePort = (int) closestInstance.get(2);
                            }
                        }

                        localInstanceClient = getNewConnection(strLocalInstanceIP, nLocalInstancePort);

                        if (localInstanceClient != null) {
                            start_time = System.currentTimeMillis();
                            ByteBuffer recv = get(localInstanceClient, tokens[1], nVersion);

                            if(recv == null) {
                                System.out.println("Failed to retrieve data for the key: " + tokens[1]);
                                continue;
                            }

                            elapsed = System.currentTimeMillis() - start_time;

                            CharBuffer charBuffer = StandardCharsets.US_ASCII.decode(recv);
                            String r = charBuffer.toString();

                            if (r.length() > 0) {
                                System.out.printf("received value: \"%s\"\n", r);
                            } else {
                                System.out.print("failed to retrieve a value for key: " + tokens[1]);

                                if (tokens.length == 3) {
                                    System.out.println(" ver: " + tokens[2]);
                                } else {
                                    System.out.println("");
                                }
                            }

                            System.out.printf("Sent to %s - Elapsed for 'get': %f\n", strHostName, elapsed);
                        } else {
                            System.out.printf("Failed to connect to the server");
                        }
                    } else if (tokens[0].toUpperCase().equals("GETVL") == true || tokens[0].toUpperCase().equals("GV") == true) {
                        if (bConnectedToWiera == false || instanceList == null) {
                            System.out.println("Need to connect to Wiera first.");
                            continue;
                        }

                        if (instanceList.length() <= 0) {
                            System.out.println("No available local instance. Need to update instance list with WieraID.");
                            continue;
                        }

                        if (tokens.length != 2) {
                            System.out.println("Usage: getvl(gv) String:key");
                            continue;
                        }

                        //If not set find the closest one.
                        if (nLocalInstancePort == 0 || strLocalInstanceIP == null) {
                            if (instanceList.length() <= 0) {
                                System.out.println("No available LocalInstance instance. Need to update instance list with WieraID.");
                                continue;
                            } else {
                                JSONArray closestInstance = (JSONArray) instanceList.get(0);
                                strLocalInstanceIP = (String) closestInstance.get(1);
                                nLocalInstancePort = (int) closestInstance.get(2);
                            }
                        }

                        start_time = System.currentTimeMillis();
                        localInstanceClient = getNewConnection(strLocalInstanceIP, nLocalInstancePort);

                        if (localInstanceClient != null) {
                            try {
                                ByteBuffer recv = localInstanceClient.getVersionList(tokens[1]);

                                CharBuffer charBuffer = StandardCharsets.US_ASCII.decode(recv);
                                String r = charBuffer.toString();

                                System.out.printf("received list for key:%s\n%s", tokens[1], r);
                                elapsed = System.currentTimeMillis() - start_time;
                                System.out.printf("Elapsed for 'getvl': %f\n", elapsed);
                            } catch (TException x) {
                                x.printStackTrace();
                            }
                        } else {
                            System.out.println("Falied to connection to the server");
                        }
                    } else if (tokens[0].toUpperCase().equals("T1")) {
                        if (strLocalInstanceIP == null || nLocalInstancePort < 1024) {
                            System.out.println("Connect to LocalInstance instance first");
                        } else {
                            change_consistency_test(strLocalInstanceIP, nLocalInstancePort);// /instanceList, 4096);
                        }
                    } else if (tokens[0].toUpperCase().equals("T2")) {
                        //0. LocalInstance instance list
                        // 1. Thread per each instance
                        if (strLastID != null) {
                            primary_change_test(getInstanceList(wieraClient, strLastID), 50);// /instanceList, 4096);
                        }
                    } else if (tokens[0].toUpperCase().equals("INIT_RETWIS")) {
                        if (strLastID != null) {
                            RetwisTest.retwis_test_init(getInstanceList(wieraClient, strLastID));// /instanceList, 4096);
                            System.out.println("Done to init retwits data.");
                        }
                    } else if (tokens[0].toUpperCase().equals("RETWIS")) {
                        if (strLastID != null) {
                            RetwisTest.retwis_test(getInstanceList(wieraClient, strLastID));// /instanceList, 4096);
                        }
                    } else if (tokens[0].toUpperCase().equals("T4"))    //Partitioned size test. write and read
                    {
                        if (tokens.length == 3) {
                            strHostName = tokens[1];
                            int lCount = Integer.parseInt(tokens[2]);

                            tier_performance_partitioned(instanceList, strHostName, lCount);
                        } else {
                            System.out.println("t4 Hostname, # of test");
                        }
                    } else if (tokens[0].toUpperCase().equals("EC_TEST")) {
                        if (tokens.length == 4) {
                            long lSize = Integer.parseInt(tokens[1]);
                            int nDataCnt = Integer.parseInt(tokens[2]);
                            int nParityCnt = Integer.parseInt(tokens[3]);

                            byte[] value = Utils.createDummyData(lSize);
                            byte[][] shards = SplitDataResponse.splitData(value, nDataCnt, nParityCnt);

                            start_time = System.currentTimeMillis();

                            // Use Reed-Solomon to calculate the parity.
//                            ReedSolomon reedSolomon = ReedSolomon.create(nDataCnt, nParityCnt);
//                          reedSolomon.encodeParity(shards, 0, shards[0].length);
                            end_time = System.currentTimeMillis();
                            elapsed = end_time - start_time;
                            System.out.printf("Elapsed for encoding: %f ms size: %d\n", elapsed, lSize);

                            final boolean[] shardPresent = new boolean[nDataCnt + nParityCnt];

                            //Test decoding works.
                            //Missing first two always
                            start_time = System.currentTimeMillis();
                            byte[][] decoded = new byte[nDataCnt + nParityCnt][shards[0].length];

                            end_time = System.currentTimeMillis();
                            elapsed = end_time - start_time;
                            System.out.printf("Elapsed for decoding: %f ms\n", elapsed);


                            for (int i = nDataCnt + nParityCnt - 1; i > 1; i--) {
                                System.arraycopy(shards[i], 0, decoded[i], 0, shards[i].length);
                                shardPresent[i] = true;
                            }

                            // Use Reed-Solomon to fill in the missing shards
//                            reedSolomon.decodeMissing(decoded, shardPresent, 0, shards[0].length);

                            byte[] recovered = new byte[(int) lSize];
                            int nCurPos = 0;
                            int nLength = 0;

                            for (int i = 0; i < nDataCnt; i++) {
                                nCurPos = shards[0].length * i;

                                if (lSize - (nCurPos + shards[0].length) < 0) {
                                    nLength = (int) lSize - nCurPos;
                                } else {
                                    nLength = shards[0].length;
                                }

                                System.arraycopy(shards[i], 0, recovered, nCurPos, nLength);
                            }

                            //Data Compare
                            if (Arrays.equals(value, recovered) == false) {
                                System.out.println("Not matched. What the...");
                            } else {
                                System.out.println("Good!!!");
                            }
                        } else {
                            System.out.println("ec_test data_size data_chunk_cnt parity_chunk_cnt");
                        }
                    } else if (tokens[0].toUpperCase().equals("QUIT") == true || tokens[0].toUpperCase().equals("Q") == true) {
                        System.out.println("Wiera client exit now");
                        System.exit(1);
                    } else {
                        System.out.println("Unknown command: " + tokens[0]);
                    }
                }
            } catch (IOException io) {
                io.printStackTrace();
            } catch (TException e) {
                e.printStackTrace();
            }
        }
    }

    private static boolean set(ApplicationToLocalInstanceIface.Client client, String key, String value) {
        //ByteBuffer buffer = ByteBuffer.allocate(1);
        ByteBuffer buf = ByteBuffer.wrap(value.getBytes());
        boolean bRet = false;

        try {
            //MessageDigest md = MessageDigest.getInstance("md5");
            //byte[] digested = md.digest(value.getBytes());

            //buffer.putObject(byte)5)
            bRet = client.put(key, buf);

            if (bRet == true) {
                System.out.printf("Value: %s, md5sum : %s\n", value, new String(""));
            } else {
                System.out.println("false");
            }
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }

        return bRet;
    }

    private static ByteBuffer get(ApplicationToLocalInstanceIface.Client client, String key, long nVersion) {
        ByteBuffer value = null;

        try {
            if (nVersion < 0) {
                value = client.get(key);//ByteBuffer.allocate(1);
            } else {
                value = client.getVersion(key, (int) nVersion);//ByteBuffer.allocate(1);
            }
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }

        return value;
    }

    private static JSONArray getInstanceList(ApplicationToWieraIface.Client wiera_client, String strWieraID) {
        String strResult;
        String strValue;
        JSONArray instanceList;

        try {
            strResult = wiera_client.getInstances(strWieraID);

            //This will contain list of LocalInstance instance as a JsonObject
            JSONObject res = new JSONObject(strResult);

            //getObject list of local instance.
            boolean bResult = (boolean) res.get(RESULT);

            if (bResult == true) {
                //Get instance List
                instanceList = (JSONArray) res.get(VALUE);
                return instanceList;
            } else {
                strValue = (String) res.get(VALUE);
                System.out.println("Failed to getObject instance list reason: " + strValue);
            }
        } catch (TException e) {
            e.printStackTrace();
        }

        return null;
    }

    private static void tier_performance_partitioned(JSONArray instanceList, String strHostname, long lCount) {
        int nCnt = instanceList.length();
        String strKey;
        String strIP;
        int nPort = 0;
        int nSize = 0;
        ApplicationToLocalInstanceIface.Client cli = null;

        List<Integer> sizeList = new ArrayList<Integer>();
//		sizeList.add(1024);
//		sizeList.add(4096);
//		sizeList.add(8192);
//		sizeList.add(16384);
//		sizeList.add(32768);
//		sizeList.add(65536);
//		sizeList.add(131072);
//		sizeList.add(262144);
        sizeList.add(524288);
        sizeList.add(1048576);
//		sizeList.add(1048576 * 2);
        sizeList.add(1048576 * 5);
        sizeList.add(1048576 * 10);
        sizeList.add(1048576 * 50);
        sizeList.add(1048576 * 100);

        Map<Integer, byte[]> dummyDataList = new LinkedHashMap<Integer, byte[]>();

        //Dummy data init
        for (int i = 0; i < sizeList.size(); i++) {
            nSize = sizeList.get(i);
            Path path = Paths.get(String.format("test_data/%d", nSize));
            try {
                byte[] dummyData = Files.readAllBytes(path);
                dummyDataList.put(nSize, dummyData);
            } catch (IOException e) {
                System.out.format("Failed to read the file : %s", String.format("test_data/%d", nSize));
                e.printStackTrace();
            }
        }

        //Find IP and Port based region
        for (int i = 0; i < nCnt; i++) {
            JSONArray region = (JSONArray) instanceList.get(i);
            if (strHostname.equals(region.get(0))) {
                strIP = (String) region.get(1);
                nPort = (int) region.get(2);
                cli = getNewConnection(strIP, nPort);
                break;
            }
        }

        if (cli == null) {
            System.out.format("failed to find client info with hostname: %s\n", strHostname);
            return;
        }

        Iterator<Integer> iter = sizeList.iterator();

        Map<Integer, List> putResults = new LinkedHashMap<Integer, List>();
        Map<Integer, List> getResults = new LinkedHashMap<Integer, List>();

        double max;
        double min;

        while (iter.hasNext()) {
            nSize = iter.next();

            List putResult = new LinkedList<Double>();

            ByteBuffer received;
            ByteBuffer dummyData = ByteBuffer.wrap(dummyDataList.get(nSize));

            max = 0;
            min = 999999999;

            for (int j = 0; j < lCount; j++) {
                strKey = String.format("test%d_%d", j, nSize);

                try {
                    long start_time = System.currentTimeMillis();
                    cli.put(strKey, dummyData);
                    long end_time = System.currentTimeMillis();
                    double elapsed = (double) end_time - (double) start_time;
                    putResult.add(elapsed);
                    System.out.printf("Elapsed for size:%d 'set': %f\n", nSize, elapsed);

                    if (elapsed > max) {
                        max = elapsed;
                    }

                    if (elapsed < min) {
                        min = elapsed;
                    }
                } catch (TException e) {
                    e.printStackTrace();
                }
            }

            putResult.remove(max);
            putResult.remove(min);
            putResults.put(nSize, putResult);

            List getResult = new LinkedList<Double>();
            Random rand = new Random();
            int nRand = 0;

            max = 0;
            min = 999999999;

            //To compare value
            //byte [] retrievedValue = new byte[nSize];
            byte[] value = new byte[nSize];

            //Random test based on Rate
            for (int j = 0; j < lCount; j++) {
                nRand = rand.nextInt((int) lCount);
                strKey = String.format("test%d_%d", j, nSize);

                try {
                    long start_time = System.currentTimeMillis();
                    received = cli.get(strKey);
                    long end_time = System.currentTimeMillis();
                    double elapsed = (double) end_time - (double) start_time;
                    getResult.add(elapsed);
                    System.out.printf("Elapsed for receiving size:%d 'getObject': %f\n", received.array().length, elapsed);

                    if (elapsed > max) {
                        max = elapsed;
                    }

                    if (elapsed < min) {
                        min = elapsed;
                    }

                    //

                    int nBugIndex = Bytes.indexOf(received.array(), dummyData.array());
                    System.arraycopy(received.array(), nBugIndex, value, 0, nSize);

                    //byte[] add = new byte[nBugIndex];
                    //System.arraycopy(received.array(), 0, add, 0, nBugIndex);

                    //CharBuffer charBuffer = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(add));
                    //String r = charBuffer.toString();

                    //System.out.printf("Added Index:%d and string:%s\n", nBugIndex, r);

                    //Data Compare
                    if (Arrays.equals(dummyData.array(), value) == false) {
                        System.out.printf("Not matched. What the... Received size:%d\n", received.array().length);
                    } else {
                        System.out.println("Good!!!");
                    }

                } catch (TException e) {
                    e.printStackTrace();
                }
            }

            //Remove Max, Min
            getResult.remove(max);
            getResult.remove(min);
            getResults.put(nSize, getResult);

            System.out.format("Size: %d Count: %d %d\n", nSize, putResult.size(), getResult.size());
        }

        System.out.println("Put stat");
        SummaryStatistics stat = new SummaryStatistics();

        for (Integer size : putResults.keySet()) {
            stat.clear();
            List<Double> list = putResults.get(size);

            for (int i = 0; i < list.size(); i++) {
                stat.addValue(list.get(i));
            }

            double mean = stat.getMean();
            double ci = calcCI(stat, 0.95);

            System.out.format("%d %f %f\n", size, mean, ci);
        }

        System.out.println("Get stat");

        for (Integer size : getResults.keySet()) {
            stat.clear();
            List<Double> list = getResults.get(size);

            for (int i = 0; i < list.size(); i++) {
                stat.addValue(list.get(i));
            }

            double mean = stat.getMean();
            double ci = calcCI(stat, 0.95);

            System.out.format("%d %f %f\n", size, mean, ci);
        }
    }

    private static double calcCI(SummaryStatistics stats, double level) {
        try {
            // Create T Distribution with N-1 degrees of freedom
            TDistribution tDist = new TDistribution(stats.getN() - 1);
            // Calculate critical value
            double critVal = tDist.inverseCumulativeProbability(1.0 - (1 - level) / 2);
            // Calculate confidence interval
            return critVal * stats.getStandardDeviation() / Math.sqrt(stats.getN());
        } catch (MathIllegalArgumentException e) {
            return Double.NaN;
        }
    }

    private static void primary_change_test(JSONArray instanceList, long lClientCnt) {
        String strHostname;
        long lPeriod = 900; //seconds
        //For now hardcoding.
        List<Double> probability = TestUtils.initProbability(lPeriod, 450, 100);

        //create thread lClientCnt for each regions
        int nCnt = instanceList.length();

        String strIP;
        long lRegionTime = 0;
        long lEachRegion = lPeriod / nCnt;
        int nPort;
        List<Thread> threadList = new LinkedList<Thread>();

        SimulatedClient client;
        Thread clientThread;

        //String[] strOrder = {"ASIA-EAST", "US-WEST", "EU-WEST", };
        //String[] strOrder = {"aws-us-west", "aws-us-west-2", "aws-asia-ne", "aws-asia-se", "aws-eu-west", "aws-us-east", "aws-us-east-2", "aws-ca-central",};
        //String[] strOrder = {"aws-us-west", "aws-us-west-2", "aws-us-east", "aws-us-east-2", "aws-ca-central"};
        //String[] strOrder = {"aws-us-east", "aws-us-east-2", "aws-ca-central"};

        String[] strOrder = {"aws-us-west", "aws-us-west-2", "aws-ca-central", "aws-us-east", "aws-us-east-2", "aws-eu-west", "aws-asia-se", "aws-asia-ne"};
        String strKey;

        //Init Data at first
        //Find IP and Port based region
        for (int i = 0; i < nCnt; i++) {
            JSONArray region = (JSONArray) instanceList.get(i);

            if ("aws-us-east".equals(region.get(0))) {
                strIP = (String) region.get(1);
                nPort = (int) region.get(2);
                ApplicationToLocalInstanceIface.Client cli = getNewConnection(strIP, nPort);

                byte[] data = Utils.createDummyData(DATA_SIZE);
                ByteBuffer dummyData = ByteBuffer.wrap(data);

                for (int j = 0; j < 100; j++) {
                    strKey = String.format("Test%d", j);
                    try {
                        cli.put(strKey, dummyData);
                    } catch (TException e) {
                        e.printStackTrace();
                    }
                }
                break;
            }
        }

        System.out.println("Now wait for 5 seconds for instances to be synced safely.");
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < strOrder.length; i++) {
            //+3 means Asia-east has more request at first.
            lRegionTime = lEachRegion * ((i + 5) % strOrder.length);
            strHostname = strOrder[i];

            System.out.printf("Hostname: %s Region Time:%d\n", strHostname, lRegionTime);

            //Find IP and Port based region
            for (int k = 0; k < nCnt; k++) {
                JSONArray region = (JSONArray) instanceList.get(k);

                if (strHostname.equals(region.get(0))) {
                    strIP = (String) region.get(1);
                    nPort = (int) region.get(2);

                    for (int j = 0; j < lClientCnt; j++) {
                        client = new SimulatedClient(strHostname, strIP, nPort, System.currentTimeMillis(), lRegionTime, lPeriod, probability);
                        clientThread = new Thread(client);
                        clientThread.start();
                        threadList.add(clientThread);
                    }

                    break;
                }
            }
        }

        nCnt = threadList.size();

        //Wait all clients are done
        for (int i = 0; i < nCnt; i++) {
            clientThread = threadList.get(i);

            try {
                clientThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static void change_consistency_test(String strInstanceIP, long lPort) {
        byte[] data = Utils.createDummyData(DATA_SIZE);
        ByteBuffer dummyData = ByteBuffer.wrap(data);

        //int nLength = instanceList.length();
        ByteBuffer received;

/*		for (int i = 0; i < nLength; i++)
		{
			JSONArray closestInstance = (JSONArray) instanceList.getObject(i);
			String strIPAddress = (String) closestInstance.getObject(1);
			int nPort = (int) closestInstance.getObject(2);

			System.out.println("Test to LocalInstance instance (" + closestInstance.getObject(0) + ")");

			//putObject and getObject 5 items. 50/50 read and write.
			//start_time = System.nanoTime();*/
        ApplicationToLocalInstanceIface.Client client = getNewConnection(strInstanceIP, (int) lPort);
        String strTestName;
        for (int i = 0; i < 10; i++) {
            strTestName = String.format("test%d", i + 1);

            try {
                client.put(strTestName, dummyData);
            } catch (TException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Wait 10 second to remove cost info");

        try {
            Thread.sleep(10000);
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (int j = 0; j < 150; j++) {
            if (client != null) {
                try {
                    System.out.printf("Sent cnt: %d\n", j + 1);

                    for (int i = 0; i < 10; i++) {
                        strTestName = String.format("test%d", i + 1);

/*						//5% put
						if ((j * 6 + i) % 18 == 0)
						{
							client.put(strTestName, dummyData);
						}
						else    //95% get
						{
							received = client.get(strTestName);
							if (received.compareTo(dummyData) != 0)
							{
								System.out.println("Sent and received data are not equal");
							}
						}*/

                        if (i % 2 == 0) {
                            client.put(strTestName, dummyData);
                        } else {
                            received = client.get(strTestName);
                            if (received.compareTo(dummyData) != 0) {
                                System.out.println("Sent and received data are not equal");
                            }
                        }
                    }
                } catch (TException e) {
                    e.printStackTrace();
                }

                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void run_clients(String strHost, String strIP, int nPort, long lStartTime, long lRegionTime, long lPeriod, List<Double> probability) {
        byte[] data = Utils.createDummyData(DATA_SIZE);
        ByteBuffer dummyData = ByteBuffer.wrap(data);

        ApplicationToLocalInstanceIface.Client client = getNewConnection(strIP, nPort);
        long lElapse = 0;
        long lCurTime = 0;
        double dProbability = 0D;
        int nRand = 0;

        Random rand = new Random();
        String strKey = new String();

        while (true) {
            lElapse = System.currentTimeMillis() - lStartTime;

            //Do the test for amount of period
            if (lElapse >= lPeriod * 1000) {
                System.out.println("Test is done.");
                break;
            }

            lCurTime = ((lElapse / 1000 + lRegionTime) % lPeriod);
            dProbability = probability.get((int) lCurTime);

            nRand = rand.nextInt(100);

            if (nRand < (dProbability * 100)) {
                if (nRand % 5 == 0) {
                    System.out.format("Elapsed %.2f Secs - %s:cur point %d : %f\n", ((double) lElapse) / 1000.0, strHost, lCurTime, dProbability);
                }

                //YCSB workload (B) as Tuba Does
                //Do the job do read (95%) or write (5%)
                nRand = rand.nextInt(100);

                try {
                    nRand = rand.nextInt(100);
                    strKey = String.format("Test%d", nRand);

                    nRand = rand.nextInt(100);
                    //Do write. 50%
                    if (nRand <= 50) {
                        client.put(strKey, dummyData);
                    } else {
                        //Do read. 50%
                        client.get(strKey);
                    }
                } catch (TException e) {
                    e.printStackTrace();
                }
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}