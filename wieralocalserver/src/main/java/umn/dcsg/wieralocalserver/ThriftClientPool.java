package umn.dcsg.wieralocalserver;

import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Kwangsung on 1/31/2017.
 */
public class ThriftClientPool {
	String m_strIP;
	long m_lPort;
	Class m_clientClass;
	LinkedBlockingQueue<TServiceClient> m_peerClientList;

	ThriftClientPool(String strIP, long lPort, long lPoolCount, Class clientClass) {
		m_strIP = strIP;
		m_lPort = lPort;
		m_clientClass = clientClass;

		m_peerClientList = new LinkedBlockingQueue<>();

		for (int i = 0; i < lPoolCount; i++) {
			TServiceClient peerClient = null;
			try {
				peerClient = createLocalInstancePeerClient();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (NoSuchMethodException e) {
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				e.printStackTrace();
			}

			if (peerClient != null) {
				m_peerClientList.add(peerClient);
			} else {
				System.out.println("Failed create connection to peer.");
			}
		}
	}

	public String getIP() {
		return m_strIP;
	}

	TServiceClient getClient() {
		try {
			return m_peerClientList.take();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return null;
	}

	void releasePeerClient(TServiceClient peerClient) {
		try {
			m_peerClientList.put(peerClient);
			//System.out.format("[debug] Available clients # %d\n", m_peerClientList.size());
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	TServiceClient createLocalInstancePeerClient() throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
		TTransport transport;
		transport = new TSocket(m_strIP, (int) m_lPort);
		TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
		//PeerInstanceIface.LocalInstanceClient client = new PeerInstanceIface.LocalInstanceClient(protocol);
		TServiceClient client = (TServiceClient) m_clientClass.getConstructor(TProtocol.class).newInstance(protocol);

		//10 seconds timeout
		((TSocket) transport).setTimeout(10000);

		try {
			transport.open();
		} catch (TException x) {
			System.out.printf("Failed to connect to IP:%s port:%d\n", m_strIP, m_lPort);
			return null;
		}

		return client;
	}
}