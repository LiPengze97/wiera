package umn.dcsg.wieralocalserver.client;

//import com.backblaze.erasure.ReedSolomon;
import com.amazonaws.services.dynamodbv2.xspec.NULL;
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
import umn.dcsg.wieralocalserver.test.PolicyGenerator;
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

public class WieraClientCLI {
    //Wiera client (local instance -> wiera client)

    protected static WieraLocalInstanceClient w_localInstanceC = null;
    protected static WieraCentralClient w_centralC = null;

    //Parse execute parameter
    protected static CommandLine checkParams(String[] args) {
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

        String strPolicyPath = "";
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

        JSONArray localServerList = null;

        w_centralC = new WieraCentralClient(strWieraIPAddress, nWieraPort);
        if (w_centralC != null) {


           w_centralC.printLocalServers();

        } else {
            //Fail to connect to Wiera
            System.out.println("[Debug] Failed to connect to Wiera Central Server.");
            System.exit(1);
        }

        // Read commands
        String delims = "[ ]+";
        String data = null;
        String[] tokens;
        InputStreamReader isr = new InputStreamReader(System.in);
        BufferedReader in = new BufferedReader(isr);
        while(true){
            try {
                System.out.print("#");
                if (System.in.available() >= 0) {
                    data = in.readLine();
                    tokens = data.split(delims);

                    if (tokens[0].toUpperCase().equals("START") == true || tokens[0].toUpperCase().equals("S") == true) {
                        if(tokens.length == 2){
                            strPolicyPath = tokens[1];
                        }
                        try {
                            boolean rs = w_centralC.startPolicy(strPolicyPath);
                            if (rs == true) {
                                w_centralC.printLocalStorageInstances();
                            } else {
                                System.out.println("[debug] Failed to start policy.");
                            }
                        }catch(NullPointerException e){
                            System.out.println("[debug] Failed to start policy.");
                        }
                    }else if(tokens[0].toUpperCase().equals("SHOWLIST") == true || tokens[0].toUpperCase().equals("LIST") == true) {

                        w_centralC.printLocalStorageInstances();


                    }else if (tokens[0].toUpperCase().equals("CONNECT") == true || tokens[0].toUpperCase().equals("C") == true) {
                        if (w_centralC.isSetpolicy() == true){
                            JSONArray instance = null;
                            if(tokens.length  == 1){
                                instance = w_centralC.getDefaultLocalStorageInstance();
                            }else if(tokens.length == 2){
                                instance = w_centralC.getLocalStorageInstances(tokens[1]);
                            }else{
                                System.out.println("[Input Error] Please supply at most one argument.");
                                continue;
                            }
                            String strHostName = (String) instance.get(0);
                            String strLocalInstanceIP = (String) instance.get(1);
                            int nLocalInstancePort = (int) instance.get(2);

                            w_localInstanceC = w_centralC.getLocalInstance(strWieraIPAddress, nLocalInstancePort);
                            System.out.println("[Log] Successfully connect to "+ strHostName);
                        }else{
                            System.out.println("[Error] Please first setup policy");
                        }
                    }else if(tokens[0].toUpperCase().equals("SET") == true){
                        if(w_localInstanceC != null){
                            if(tokens.length != 3){
                                System.out.println("[Input Error] Please follow the rule: SET key value");
                                continue;
                            }
                            ByteBuffer buf = ByteBuffer.wrap(tokens[2].getBytes());
                            w_localInstanceC.set(tokens[1], buf);
                            System.out.println("Set done.");
                        }else{
                            System.out.println("[Error] Please first connect to a local storage instance.");
                        }
                    }else if(tokens[0].toUpperCase().equals("GET") == true){
                        if(w_localInstanceC != null){
                            if(tokens.length != 2){
                                System.out.println("[Input Error] Please follow the rule: GET key");
                                continue;
                            }
                            ByteBuffer buf = w_localInstanceC.get(tokens[1]);
                            CharBuffer charBuffer = StandardCharsets.US_ASCII.decode(buf);
                            String r = charBuffer.toString();
                            System.out.println("received value: " + r);
                        }else{
                            System.out.println("[Error] Please first connect to a local storage instance.");
                        }
                    }else{
                        if(tokens[0].equals("") == false ){
                            System.out.println("[Input Error] Unknown command: " + tokens[0]);
                        }
                    }
                }
            }catch (IOException io) {
                io.printStackTrace();
            }
        }

    }
}
