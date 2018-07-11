package umn.dcsg.wieralocalserver.test;

import com.google.common.primitives.Bytes;
import org.apache.commons.cli.*;
import org.apache.commons.math3.distribution.TDistribution;
import org.apache.commons.math3.exception.MathIllegalArgumentException;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.json.JSONArray;
import umn.dcsg.wieralocalserver.*;
import umn.dcsg.wieralocalserver.clients.LocalInstanceClient;
import umn.dcsg.wieralocalserver.clients.WieraInstanceClient;
import umn.dcsg.wieralocalserver.responses.peers.SplitDataResponse;
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

public class WieraClientCLI {
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
            formatter.printHelp("java LocalInstanceCLI", options);
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

        WieraInstanceClient wieraClient;
        wieraClient = new WieraInstanceClient(strWieraIPAddress, nWieraPort);
        LocalInstanceClient localInstanceClient = null;

        InputStreamReader isr = new InputStreamReader(System.in);
        BufferedReader in = new BufferedReader(isr);
        String data;
        long nStartTime;
        long nEndTime;
        double elapsed;
        //ApplicationToLocalInstanceIface.Client localInstanceClient = null;

        String[] tokens;
        String strLocalInstanceIP = null;
        int nLocalInstancePort = 0;
        String strLastID = null;

        boolean bResult;
        final String delims = "[ ]+";

        // stay alive until told to quit
        while (true) {
            try {
                if (System.in.available() >= 0) {
                    data = in.readLine();
                    tokens = data.split(delims);
                    // We really don't do a very clean shutdown, there should
                    // ideally be a mutex to issue a shutdown!
                    // Command to Wiera
                    if (tokens[0].toUpperCase().equals("CONNECTWIERA") == true || tokens[0].toUpperCase().equals("CW") == true) {
                        if (wieraClient.isConnected() == true) {
                            System.out.println("Already connected to Wiera.");
                        } else {
                            if (tokens.length == 3) {
                                String strIPAddress = tokens[1];
                                int nPort = Integer.parseInt(tokens[2]);
                                wieraClient.connect(strIPAddress, nPort);
                            } else {
                                wieraClient.connect();
                            }

                            if (wieraClient.isConnected() == true) {
                                System.out.println("Connect to Wiera successfully.");
                            } else {
                                System.out.println("Failed to connect to Wiera");
                            }
                        }
                    } else if (tokens[0].toUpperCase().equals("START") == true || tokens[0].toUpperCase().equals("S") == true) {
                        if(wieraClient.startInstance(strPolicyPath) == true) {
                            localInstanceClient = wieraClient.getLocalInstanceClient(0);
                        } else {
                            System.out.println("Failed to start Wiera instance");
                        }
                    } else if (tokens[0].toUpperCase().equals("STOP") == true || tokens[0].toUpperCase().equals("ST") == true) {
                        if (tokens.length > 2) {
                            System.out.println("Usage: stop(st) string:policyID");
                            continue;
                        }

                        wieraClient.stopInstance(tokens[1]);
                    } else if (tokens[0].toUpperCase().equals("SHOWLIST") == true || tokens[0].toUpperCase().equals("LIST") == true) {
                        System.out.println("------------------------  Instance List  ------------------------");

                        if (wieraClient.isConnected() == true && wieraClient.getLocalInstanceList() != null) {
                            //Get instance List
                            int lLength = wieraClient.getLocalInstanceList().length();
                            for (int i = 0; i < lLength; i++) {
                                JSONArray instance = (JSONArray) wieraClient.getLocalInstanceList().get(i);
                                System.out.format("Hostname: %s, IP: %s, Port: %d\n", instance.get(0), instance.get(1), instance.get(2));
                            }
                        } else {
                            System.out.println("- No Available Instance information");
                        }
                    } else if (tokens[0].toUpperCase().equals("CONNECT") == true || tokens[0].toUpperCase().equals("C") == true) {
                        if (wieraClient.isConnected() == true && wieraClient.getLocalInstanceList() != null) {
                            boolean bFound = false;
                            String strParam = tokens[1];
                            long lLength = wieraClient.getLocalInstanceList().length();

                            for (int i = 0; i < lLength; i++) {
                                JSONArray instance = (JSONArray) wieraClient.getLocalInstanceList().get(i);

                                if (strParam.equals(instance.get(0)) == true) {
                                    localInstanceClient = wieraClient.getLocalInstanceClient(i);
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
                        if (wieraClient.isConnected() == false) {
                            System.out.println("Need to create instances first.");
                            continue;
                        }

                        //Create instance here with global Policy.
                        if (tokens.length != 2) {
                            System.out.println("Usage: GetInstanceList(gil)  string:policyID");
                            continue;
                        }

                        wieraClient.updateInstanceClient(tokens[1]);
                    }
                    ////////////////////////////////////////////////////////////////////////////////////////////////
                    // From now on command to LocalInstance
                    else if (tokens[0].toUpperCase().equals("UPDATE") == true || tokens[0].toUpperCase().equals("U") == true) {
                        //Not supported Yet.
                        if (localInstanceClient == null || localInstanceClient.isConnected() == false) {
                            System.out.println("Need to connet to local Instance.");
                            continue;
                        }
                    } else if (tokens[0].toUpperCase().equals("REMOVE") == true || tokens[0].toUpperCase().equals("R") == true) {
                        //Not supported Yet.
                        if (localInstanceClient == null || localInstanceClient.isConnected() == false) {
                            System.out.println("Need to connet to local Instance.");
                            continue;
                        }
                    } else if (tokens[0].toUpperCase().equals("SET") == true) {
                        if (localInstanceClient == null || localInstanceClient.isConnected() == false) {
                            System.out.println("Need to connet to local Instance.");
                            continue;
                        }

                        if (tokens.length <= 1 || tokens.length >= 4) {
                            System.out.println("Usage: put String:key String:value");
                            continue;
                        }

                        nStartTime = System.currentTimeMillis();
                        localInstanceClient.put(tokens[1], tokens[2]);
                        nEndTime = System.currentTimeMillis();
                        elapsed = nEndTime - nStartTime;
                        System.out.printf("Sent to : %s - Elapsed for 'put': %f\n", localInstanceClient.getHostName(), elapsed);
                    } else if (tokens[0].toUpperCase().equals("GET") == true || tokens[0].toUpperCase().equals("G") == true) {
                        if (localInstanceClient == null || localInstanceClient.isConnected() == false) {
                            System.out.println("Need to connet to local Instance.");
                            continue;
                        }

                        if (tokens.length <= 1 || tokens.length >= 4) {
                            System.out.println("Usage: get String:key [Integer:version]");
                            continue;
                        }

                        int nVersion = MetaObjectInfo.NO_SUCH_VERSION;

                        if (tokens.length == 3) {
                            try {
                                nVersion = Integer.parseInt(tokens[2]);
                            } catch (NumberFormatException e) {
                                System.out.println("Version should be Integer type.");
                                continue;
                            }
                        }

                        nStartTime = System.currentTimeMillis();
                        byte[] result = localInstanceClient.get(tokens[1], nVersion);


                        if (result == null) {
                            System.out.println("Failed to find the key: " + tokens[1]);
                            continue;
                        }

                        elapsed = System.currentTimeMillis() - nStartTime;

                        CharBuffer charBuffer = StandardCharsets.US_ASCII.decode(ByteBuffer.wrap(result));
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

                        System.out.printf("Sent to %s - Elapsed for 'get': %f\n", localInstanceClient.getHostName(), elapsed);
                    } else if (tokens[0].toUpperCase().equals("GETVL") == true || tokens[0].toUpperCase().equals("GV") == true) {
                        if (localInstanceClient == null || localInstanceClient.isConnected() == false) {
                            System.out.println("Need to connet to local Instance.");
                            continue;
                        }

                        if (tokens.length != 2) {
                            System.out.println("Usage: getvl(gv) String:key");
                            continue;
                        }

                        nStartTime = System.currentTimeMillis();

                        List<Integer> list = localInstanceClient.getVersionList(tokens[1]);

                        System.out.printf("received list for key:%s\n%s", tokens[1], list.toString());
                        elapsed = System.currentTimeMillis() - nStartTime;
                        System.out.printf("Elapsed for 'getvl': %f\n", elapsed);
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
                            // primary_change_test(getInstanceList(wieraClient, strLastID), 50);// /instanceList, 4096);
                        }
                    } else if (tokens[0].toUpperCase().equals("INIT_RETWIS")) {
                        if (strLastID != null) {
                            //  RetwisTest.retwis_test_init(getInstanceList(wieraClient, strLastID));// /instanceList, 4096);
                            System.out.println("Done to init retwits data.");
                        }
                    } else if (tokens[0].toUpperCase().equals("RETWIS")) {
                        if (strLastID != null) {
                            //  RetwisTest.retwis_test(getInstanceList(wieraClient, strLastID));// /instanceList, 4096);
                        }
                    } else if (tokens[0].toUpperCase().equals("T4"))    //Partitioned size test. write and read
                    {
                        if (tokens.length == 3) {
                            int lCount = Integer.parseInt(tokens[2]);

                            //  tier_performance_partitioned(instanceList, strHostName, lCount);
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

                            nStartTime = System.currentTimeMillis();

                            // Use Reed-Solomon to calculate the parity.
//                            ReedSolomon reedSolomon = ReedSolomon.create(nDataCnt, nParityCnt);
//                          reedSolomon.encodeParity(shards, 0, shards[0].length);
                            nEndTime = System.currentTimeMillis();
                            elapsed = nEndTime - nStartTime;
                            System.out.printf("Elapsed for encoding: %f ms size: %d\n", elapsed, lSize);

                            final boolean[] shardPresent = new boolean[nDataCnt + nParityCnt];

                            //Test decoding works.
                            //Missing first two always
                            nStartTime = System.currentTimeMillis();
                            byte[][] decoded = new byte[nDataCnt + nParityCnt][shards[0].length];

                            nEndTime = System.currentTimeMillis();
                            elapsed = nEndTime - nStartTime;
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
            }
        }
    }

    private static void tier_performance_partitioned(JSONArray instanceList, String strHostname, long lCount) {
        int nCnt = instanceList.length();
        String strKey;
        String strIP;
        int nPort = 0;
        int nSize = 0;
        LocalInstanceClient client = null;

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
                client = new LocalInstanceClient(strHostname, strIP, nPort, 1);
                break;
            }
        }

        if (client == null) {
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
                long start_time = System.currentTimeMillis();
                client.put(strKey, dummyData);
                long end_time = System.currentTimeMillis();
                double elapsed = (double) end_time - (double) start_time;
                putResult.add(elapsed);
                System.out.printf("Elapsed for size:%d 'put': %f\n", nSize, elapsed);

                if (elapsed > max) {
                    max = elapsed;
                }

                if (elapsed < min) {
                    min = elapsed;
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

               long start_time = System.currentTimeMillis();
                byte[] result = client.get(strKey);
                long end_time = System.currentTimeMillis();
                double elapsed = (double) end_time - (double) start_time;
                getResult.add(elapsed);
                System.out.printf("Elapsed for receiving size:%d 'getObject': %f\n", result.length, elapsed);

                if (elapsed > max) {
                    max = elapsed;
                }

                if (elapsed < min) {
                    min = elapsed;
                }

                //

                int nBugIndex = Bytes.indexOf(result, dummyData.array());
                System.arraycopy(result, nBugIndex, value, 0, nSize);

                //byte[] add = new byte[nBugIndex];
                //System.arraycopy(received.array(), 0, add, 0, nBugIndex);

                //CharBuffer charBuffer = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(add));
                //String r = charBuffer.toString();

                //System.out.printf("Added Index:%d and string:%s\n", nBugIndex, r);

                //Data Compare
                if (Arrays.equals(dummyData.array(), value) == false) {
                    System.out.printf("Not matched. What the... Received size:%d\n", result.length);
                } else {
                    System.out.println("Good!!!");
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
                LocalInstanceClient cli = new LocalInstanceClient("aws-us-east", strIP, nPort, 1);

                byte[] data = Utils.createDummyData(DATA_SIZE);
                ByteBuffer dummyData = ByteBuffer.wrap(data);

                for (int j = 0; j < 100; j++) {
                    strKey = String.format("Test%d", j);
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

    private static void change_consistency_test(String strInstanceIP, int nPort) {
        byte[] dummyData = Utils.createDummyData(DATA_SIZE);

        //int nLength = instanceList.length();
        byte[] received;

/*		for (int i = 0; i < nLength; i++)
		{
			JSONArray closestInstance = (JSONArray) instanceList.getObject(i);
			String strIPAddress = (String) closestInstance.getObject(1);
			int nPort = (int) closestInstance.getObject(2);

			System.out.println("Test to LocalInstance instance (" + closestInstance.getObject(0) + ")");

			//putObject and getObject 5 items. 50/50 read and write.
			//start_time = System.nanoTime();*/
        LocalInstanceClient client = new LocalInstanceClient("localhost", strInstanceIP, nPort, 1);

        String strTestName;
        for (int i = 0; i < 10; i++) {
            strTestName = String.format("test%d", i + 1);
            client.put(strTestName, dummyData);
        }

        System.out.println("Wait 10 second to remove cost info");

        try {
            Thread.sleep(10000);
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (int j = 0; j < 150; j++) {
            if (client != null) {
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
                        client.put(strTestName, ByteBuffer.wrap(dummyData));
                    } else {
                        received = client.get(strTestName);
                        if (received.equals(dummyData) == false) {
                            System.out.println("Sent and received data are not equal");
                        }
                    }
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

        LocalInstanceClient client = new LocalInstanceClient(strHost, strIP, nPort, 1);
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
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}