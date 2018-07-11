package umn.dcsg.wieralocalserver.test;

import java.io.*;
import java.nio.ByteBuffer;

import org.apache.commons.cli.*;
import umn.dcsg.wieralocalserver.MetaObjectInfo;
import umn.dcsg.wieralocalserver.clients.LocalInstanceClient;

import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class LocalInstanceCLI {
    //Main function starts here!
    //Parse execute parameter
    static CommandLine checkParams(String[] args) {
        Options options = new Options();

        //Wiera IP Location option
        //If not specified LocalInstance is running stand-alone mode
        Option input = new Option("i", "ip", true, "LocalInstance Server IP address");
        input.setRequired(false);
        options.addOption(input);

        Option port = new Option("p", "port", true, "LocalInstance instance port");
        input.setRequired(false);
        options.addOption(port);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("java LocalInstanceCLI", options);
            return null;
        }

        return cmd;
    }

    public static void main(String[] args) {
        //Default to connect
        String strIPAddress = "127.0.0.1";
        int nPort = 55555;

        //Get ip and port
        CommandLine cmd = checkParams(args);
        if (cmd == null) {
            return;
        }

        //Check whether LocalInstance needs to connect to Wiera
        //
        if (cmd.hasOption('i') == true) {
            strIPAddress = cmd.getOptionValue('i');
        }

        if (cmd.hasOption('p') == true) {
            nPort = Integer.parseInt(cmd.getOptionValue('p'));
        }

        LocalInstanceClient localInstanceClient = new LocalInstanceClient("localhost", strIPAddress, nPort);

        //Now connect to LocalInstance
        InputStreamReader isr = new InputStreamReader(System.in);
        BufferedReader in = new BufferedReader(isr);
        long start_time = 0;
        long end_time = 0;
        double elapsed = 0;

        String strInput;
        String[] tokens;
        final String delims = "[ ]+";

        while (true) {
            try {
                if (System.in.available() >= 0) {
                    strInput = in.readLine();
                    tokens = strInput.split(delims);

                    if (tokens[0].toUpperCase().equals("SET") == true) {
                        if (tokens.length != 3) {
                            System.out.println("Usage: put(s) string:key binary:value");
                            continue;
                        }

                        if (localInstanceClient != null) {
                            start_time = System.currentTimeMillis();
                            localInstanceClient.put(tokens[1], tokens[2]);
                            end_time = System.currentTimeMillis();
                            elapsed = end_time - start_time;
                            System.out.printf("Elapsed for 'put': %f\n", elapsed);
                        } else {
                            System.out.printf("Failed to connect to LocalInstance Server");
                        }
                    } else if (tokens[0].toUpperCase().equals("GET") == true || tokens[0].toUpperCase().equals("G") == true) {
                        if (tokens.length <= 1 || tokens.length >= 4) {
                            System.out.println("Usage: getObject(g) String:key [Integer:version]");
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

                        start_time = System.currentTimeMillis();
                        byte[] recv = localInstanceClient.get(tokens[1], nVersion);
                        elapsed = System.currentTimeMillis() - start_time;

                        CharBuffer charBuffer = StandardCharsets.US_ASCII.decode(ByteBuffer.wrap(recv));
                        String r = charBuffer.toString();

                        if (r.length() > 0) {
                            System.out.printf("received value: \"%s\"\n", r);
                            System.out.printf("Elapsed for 'get': %f\n", elapsed);
                        } else {
                            System.out.print("failed to retrieve a value for key: " + tokens[1]);
                        }
                    } else if (tokens[0].toUpperCase().equals("GETVL") == true || tokens[0].toUpperCase().equals("GV") == true) {
                        if (tokens.length != 2) {
                            System.out.println("Usage: getvl(gv) String:key");
                            continue;
                        }

                        start_time = System.currentTimeMillis();
                        List versionList = localInstanceClient.getVersionList(tokens[1]);

                        System.out.printf("received list for key:%s\n", tokens[1], versionList.toString());
                        elapsed = System.currentTimeMillis() - start_time;
                        System.out.printf("Elapsed for 'getvl': %f\n", elapsed);
                    } else if (tokens[0].toUpperCase().equals("QUIT") == true || tokens[0].toUpperCase().equals("Q") == true) {
                        System.out.println("LocalInstance WieraClient exit now");
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
}