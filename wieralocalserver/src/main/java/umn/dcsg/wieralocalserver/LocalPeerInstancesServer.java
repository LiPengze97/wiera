package umn.dcsg.wieralocalserver;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;
import umn.dcsg.wieralocalserver.thriftinterfaces.LocalInstanceToPeerIface;

public class LocalPeerInstancesServer implements Runnable {
	PeerInstancesManager m_peerInstanceManager;
	TServer m_thriftServer;
	int m_port;

	public LocalPeerInstancesServer(PeerInstancesManager manager) {
		m_peerInstanceManager = manager;

		try {
			//For now hardcoded port for Azure.
			//TServerSocket client_socket = new TServerSocket(9097);
			TServerSocket serverSocket = new TServerSocket(0);
			TServerTransport serverTransport = serverSocket;
			TProtocolFactory tProtocolFactory = new TBinaryProtocol.Factory(true, true);
			TTransportFactory transportFactory = new TFramedTransport.Factory();
			LocalInstanceToPeerInterface peerInterface = new LocalInstanceToPeerInterface(m_peerInstanceManager.m_instance);
			LocalInstanceToPeerIface.Processor processor = new LocalInstanceToPeerIface.Processor(peerInterface);
			m_thriftServer = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport)
					.minWorkerThreads(128)
					.maxWorkerThreads(512)
					.inputTransportFactory(transportFactory)
					.outputTransportFactory(transportFactory)
					.inputProtocolFactory(tProtocolFactory)
					.outputProtocolFactory(tProtocolFactory)
					.processor(processor));

			m_port = serverSocket.getServerSocket().getLocalPort();
		} catch (TTransportException e) {
			e.printStackTrace();
		}
	}

	public int getPort() {
		return m_port;
	}

	public void run() {
		System.out.format("LocalInstance Peer Server starts waiting peers connections from port: %d\n", getPort());
		m_thriftServer.serve();
	}
}