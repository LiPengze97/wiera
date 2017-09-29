package umn.dcsg.wieralocalserver.test;

import java.io.*;
import java.nio.ByteBuffer;

import org.apache.commons.cli.*;
import umn.dcsg.wieralocalserver.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import umn.dcsg.wieralocalserver.thriftinterfaces.ApplicationToLocalInstanceIface;

import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;

public class LocalInstanceClient {
    private int m_nPort;
    private String m_strIPAddress;
    ApplicationToLocalInstanceIface.Client m_client;

    LocalInstanceClient(String strIPAddress, int nPort) {
        TTransport transport;

        transport = new TSocket(strIPAddress, nPort);
        TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport, 1048576 * 200));

        try {
            transport.open();
            m_client = new ApplicationToLocalInstanceIface.Client(protocol);
        } catch (TException x) {
            x.printStackTrace();
        }
    }

    public boolean set(String key, String value) {
        if (m_client == null) {
            return false;
        }

        ByteBuffer buf = ByteBuffer.wrap(value.getBytes());
        boolean bRet = false;

        try {
            bRet = m_client.put(key, buf);

            if (bRet == false) {
                System.out.println("Fail to put!!!");
            }
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }

        return bRet;
    }

    public ByteBuffer get(String key, long nVersion) {
        ByteBuffer value = null;

        try {
            if (nVersion < 0) {
                value = m_client.get(key);
            } else {
                value = m_client.getVersion(key, (int) nVersion);
            }
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }

        return value;
    }

    public ByteBuffer getVersionList(String key) {
        ByteBuffer value = null;

        try {
            value = m_client.getVersionList(key);
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }

        return value;
    }

    public ApplicationToLocalInstanceIface.Client getClient() {
        return m_client;
    }

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
            formatter.printHelp("java LocalInstanceClient", options);
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

        LocalInstanceClient localInstanceClient = new LocalInstanceClient(strIPAddress, nPort);

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
                            System.out.println("Usage: set(s) string:key binary:value");
                            continue;
                        }

                        if (localInstanceClient != null) {
                            start_time = System.currentTimeMillis();
                            localInstanceClient.set(tokens[1], tokens[2]);
                            end_time = System.currentTimeMillis();
                            elapsed = end_time - start_time;
                            System.out.printf("Elapsed for 'set': %f\n", elapsed);
                        } else {
                            System.out.printf("Failed to connect to LocalInstance Server");
                        }
                    } else if (tokens[0].toUpperCase().equals("GET") == true || tokens[0].toUpperCase().equals("G") == true) {
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

                        start_time = System.currentTimeMillis();
                        ByteBuffer recv = localInstanceClient.get(tokens[1], nVersion);
                        elapsed = System.currentTimeMillis() - start_time;

                        CharBuffer charBuffer = StandardCharsets.US_ASCII.decode(recv);
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
                        ByteBuffer recv = localInstanceClient.getVersionList(tokens[1]);

                        CharBuffer charBuffer = StandardCharsets.US_ASCII.decode(recv);
                        String r = charBuffer.toString();

                        System.out.printf("received list for key:%s\n%s", tokens[1], r);
                        elapsed = System.currentTimeMillis() - start_time;
                        System.out.printf("Elapsed for 'getvl': %f\n", elapsed);
                    } else if (tokens[0].toUpperCase().equals("QUIT") == true || tokens[0].toUpperCase().equals("Q") == true) {
                        System.out.println("LocalInstance Client exit now");
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