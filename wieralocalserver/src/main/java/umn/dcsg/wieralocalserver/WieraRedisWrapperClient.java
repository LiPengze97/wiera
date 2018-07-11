package umn.dcsg.wieralocalserver;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import umn.dcsg.wieralocalserver.thriftinterfaces.ApplicationToWieraIface;
import umn.dcsg.wieralocalserver.thriftinterfaces.RedisWrapperApplicationIface;
import umn.dcsg.wieralocalserver.utils.Utils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.json.JSONArray;
import org.json.JSONObject;

import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by Kwangsung on 10/17/2016.
 */
public class WieraRedisWrapperClient {
	String m_strWieraID;
	String m_strWieraIP;
	int m_nWieraPort;
	boolean m_bInitialized = false;
	JSONArray m_wieraInstanceList = null;
	//RedisWrapperApplicationIface.LocalInstanceCLI m_wieraRedisClient;
	ThriftClientPool m_clientPool;// = new ThriftClientPool(strPeerIP, nPeerPort, 6,PeerInstanceIface.LocalInstanceCLI.class);

	public WieraRedisWrapperClient(String strWieraIP, int nWieraPort, String strWieraID) {
		m_strWieraIP = strWieraIP;
		m_nWieraPort = nWieraPort;
		m_strWieraID = strWieraID;
		m_bInitialized = initWieraInstance();

		if (m_bInitialized == true) {
			//Get closest one
			m_clientPool = initClientPool(10);
		}
	}

	//Parameter can be used for handling fault for later
	public RedisWrapperApplicationIface.Client getLocalInstanceClient() {
		return (RedisWrapperApplicationIface.Client) m_clientPool.getClient();
	}

	private boolean initWieraInstance() {
		ApplicationToWieraIface.Client wiera_client = getWieraClient(m_strWieraIP, m_nWieraPort);

		if (wiera_client != null) {
			String strResult = null;
			try {
				strResult = wiera_client.getWieraInstance(m_strWieraID);

				//This will contain list of LocalInstance instance as a JsonObject
				JSONObject res = new JSONObject(strResult);

				//getObject list of local instance.
				boolean bResult = (boolean) res.get(Constants.RESULT);

				if (bResult == true) {
					//Get instance List
					m_wieraInstanceList = (JSONArray) res.get(Constants.VALUE);

					if (m_wieraInstanceList != null) {
						return true;
					}
				}
			} catch (TException x) {
				System.out.printf("Failed to retrieve LocalInstance instance list with WieraID: %s\n", m_strWieraID);
				x.printStackTrace();
			}
		}

		return false;
	}

	//Need to create connection pool
	public ThriftClientPool initClientPool(int nPool) {
		String strHostName;
		RedisWrapperApplicationIface.Client client = null;

		try {
			strHostName = InetAddress.getLocalHost().getHostName();

			if (m_wieraInstanceList != null) {
				for (int i = 0; i < m_wieraInstanceList.length(); i++) {
					JSONArray instance = (JSONArray) m_wieraInstanceList.get(i);

					if (strHostName.compareTo((String) instance.get(0)) == 0) {
						String strLocalInstanceIP = (String) instance.get(1);
						int nLocalInstancePort = 6378;//(int) closestInstance.get(2);

						ThriftClientPool pool = new ThriftClientPool(strLocalInstanceIP, nLocalInstancePort, nPool, RedisWrapperApplicationIface.Client.class);
						return pool;
					}
				}
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

		return null;
	}

	private ApplicationToWieraIface.Client getWieraClient(String strIPAddress, int nPort) {
		TTransport transport;

		transport = new TSocket(strIPAddress, nPort);
		TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
		ApplicationToWieraIface.Client client = new ApplicationToWieraIface.Client(protocol);

		try {
			transport.open();
		} catch (TException x) {
			x.printStackTrace();
			return null;
		}

		return client;
	}

	public Map<String, String> hgetAll(final String strKey) throws TException {
		//Reqeust
		JSONObject req = new JSONObject();
		req.put(Constants.KEY, strKey);

		//Result
		RedisWrapperApplicationIface.Client client = (RedisWrapperApplicationIface.Client) m_clientPool.getClient();

		try {
			String strRet = client.hgetAll(req.toString());
			JSONObject ret = new JSONObject(strRet);

			if ((boolean) ret.get(Constants.RESULT) == true) {
				//Make a Map with ret
				Type type = new TypeToken<Map<String, String>>() {
				}.getType();
				Gson gson = new Gson();

				String strMap = (String) ret.get(Constants.VALUE);

				if (strMap.length() > 0) {
					return gson.fromJson(strMap, type);
				}
			}
		} catch (TException e) {
			e.printStackTrace();
		} finally {
			m_clientPool.releasePeerClient(client);
		}

		return new HashMap<String, String>();
	}

	// @return Return OK or Exception if hash is empty
	public String hmset(final String strKey, final Map<String, String> hash) throws TException {
		//Make a Map with ret
		Type type = new TypeToken<Map<String, String>>() {
		}.getType();
		Gson gson = new Gson();

		//Reqeust
		JSONObject req = new JSONObject();
		req.put(Constants.KEY, strKey);
		req.put(Constants.VALUE, gson.toJson(hash, type));

		//Result
		RedisWrapperApplicationIface.Client client = (RedisWrapperApplicationIface.Client) m_clientPool.getClient();

		try {
			String strRet = client.hmset(req.toString());
			JSONObject ret = new JSONObject(strRet);

			if ((boolean) ret.get(Constants.RESULT) == true) {
				String strResult = (String) ret.get(Constants.VALUE);
				return strResult;
			}
		} catch (TException e) {
			e.printStackTrace();
		} finally {
			m_clientPool.releasePeerClient(client);
		}

		//Return empty Map
		return "Failed";
	}

	public List<String> hmget(final String strKey, final String... fields) {
		//Make a Map with ret
		Type type = new TypeToken<ArrayList<String>>() {
		}.getType();
		Gson gson = new Gson();

		//Reqeust
		JSONObject req = new JSONObject();
		req.put(Constants.KEY, strKey);
		req.put(Constants.VALUE, gson.toJson(Arrays.asList(fields), type));

		//Result
		RedisWrapperApplicationIface.Client client = (RedisWrapperApplicationIface.Client) m_clientPool.getClient();

		try {
			String strRet = client.hmget(req.toString());

			JSONObject ret = new JSONObject(strRet);

			if ((boolean) ret.get(Constants.RESULT) == true) {
				ArrayList retList = gson.fromJson((String) ret.get(Constants.VALUE), type);
				return retList;
			}
		} catch (TException e) {
			e.printStackTrace();
		} finally {
			m_clientPool.releasePeerClient(client);
		}

		//Return empty List
		return new ArrayList();
	}

	public Long zadd(final String strKey, final double dbScore, final String strMember) {
		//Reqeust
		JSONObject req = new JSONObject();
		req.put(Constants.KEY, strKey);
		req.put(Constants.VALUE, dbScore);
		req.put(Constants.VALUE2, strMember);

		RedisWrapperApplicationIface.Client client = (RedisWrapperApplicationIface.Client) m_clientPool.getClient();

		//Result
		try {
			String strRet = client.zadd(req.toString());
			JSONObject ret = new JSONObject(strRet);

			if ((boolean) ret.get(Constants.RESULT) == true) {
				return Utils.convertToLong(ret.get(Constants.VALUE));
			}
		} catch (TException e) {
			e.printStackTrace();
		} finally {
			m_clientPool.releasePeerClient(client);
		}

		return 0l;
	}

	public Long zrem(final String strKey, final String... members) {
		//Make a Map with ret
		Type type = new TypeToken<ArrayList<String>>() {
		}.getType();
		Gson gson = new Gson();

		//Reqeust
		JSONObject req = new JSONObject();
		req.put(Constants.KEY, strKey);
		req.put(Constants.VALUE, gson.toJson(Arrays.asList(members), type));

		//Result
		RedisWrapperApplicationIface.Client client = (RedisWrapperApplicationIface.Client) m_clientPool.getClient();

		try {
			String strRet = client.zrem(req.toString());
			JSONObject ret = new JSONObject(strRet);

			if ((boolean) ret.get(Constants.RESULT) == true) {
				return Utils.convertToLong((ret.get(Constants.VALUE)));
			}
		} catch (TException e) {
			e.printStackTrace();
		} finally {
			m_clientPool.releasePeerClient(client);
		}

		return 0l;
	}

	public Set<String> zrangeByScore(final String strKey, final double lStart, final double lEnd, final double nOffset, final int nCount) {
		//Make a Map with ret
		Type type = new TypeToken<Set<String>>() {
		}.getType();
		Gson gson = new Gson();

		//Reqeust
		JSONObject req = new JSONObject();
		req.put(Constants.KEY, strKey);
		req.put(Constants.VALUE, lStart);
		req.put(Constants.VALUE2, lEnd);
		req.put(Constants.VALUE3, nOffset);
		req.put(Constants.VALUE4, nCount);

		//Result
		RedisWrapperApplicationIface.Client client = (RedisWrapperApplicationIface.Client) m_clientPool.getClient();

		try {
			String strRet = client.zrangeByScore(req.toString());
			JSONObject ret = new JSONObject(strRet);
			Set<String> retSet;

			if ((boolean) ret.get(Constants.RESULT) == true) {
				retSet = gson.fromJson((String) ret.get(Constants.VALUE), type);
				return retSet;
			}
		} catch (TException e) {
			e.printStackTrace();
		} finally {
			m_clientPool.releasePeerClient(client);
		}

		return Collections.emptySet();
	}

	public Long del(final String strKey) {
		//Make a Map with ret
		Type type = new TypeToken<ArrayList<String>>() {
		}.getType();
		Gson gson = new Gson();

		//Reqeust
		JSONObject req = new JSONObject();
		req.put(Constants.KEY, strKey);

		//Result
		RedisWrapperApplicationIface.Client client = (RedisWrapperApplicationIface.Client) m_clientPool.getClient();

		try {
			String strRet = client.remove(req.toString());
			JSONObject ret = new JSONObject(strRet);

			if ((boolean) ret.get(Constants.RESULT) == true) {
				return Utils.convertToLong(ret.get(Constants.VALUE));
			}
		} catch (TException e) {
			e.printStackTrace();
		} finally {
			m_clientPool.releasePeerClient(client);
		}

		return 0l;
	}
}