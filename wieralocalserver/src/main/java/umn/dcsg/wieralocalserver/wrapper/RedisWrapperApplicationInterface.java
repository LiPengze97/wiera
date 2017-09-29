package umn.dcsg.wieralocalserver.wrapper;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import umn.dcsg.wieralocalserver.Constants;
import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.info.Latency;
import umn.dcsg.wieralocalserver.thriftinterfaces.RedisWrapperApplicationIface;
import umn.dcsg.wieralocalserver.utils.Utils;
import org.apache.commons.collections4.bidimap.DualTreeBidiMap;
import org.apache.thrift.TException;
import org.json.JSONObject;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by Kwangsung on 1/30/2017.
 */
//LocalInstanceClient library for wrapping class
//If only function can be done with single communication, it is handled only with get/put
//If not, Wiera side fuction provided.
public class RedisWrapperApplicationInterface implements RedisWrapperApplicationIface.Iface {
	LocalInstance m_localInstance;

	public RedisWrapperApplicationInterface(LocalInstance instance) {
		m_localInstance = instance;
	}

	private ArrayList<String> getListByKey(String strKey) throws TException {
		ArrayList list = null;

		try {
			//For reqeust
			Gson gson = new Gson();
			Type setType = new TypeToken<ArrayList>() {
			}.getType();

			ByteBuffer received = m_localInstance.m_applicationToLocalInstanceInterface.get(strKey);

			//Return
			byte[] result = received.array();

			if (result.length > 0) {
				String strJson = new String(result);
				list = gson.fromJson(strJson, setType);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return list;
	}

	private Set<String> getSetByKey(String strKey) throws TException {
		Set set = null;

		try {
			Gson gson = new Gson();
			Type setType = new TypeToken<Set>() {
			}.getType();

			ByteBuffer received = m_localInstance.m_applicationToLocalInstanceInterface.get(strKey);

			//Return
			byte[] result = received.array();

			if (result.length > 0) {
				String strJson = new String(result);
				set = gson.fromJson(strJson, setType);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return set;
	}

	@Override
	public String get(String strRequest) throws TException {
		JSONObject req = new JSONObject(strRequest);
		String strKey = (String) req.get(Constants.KEY);

		//Return
		JSONObject ret = new JSONObject();
		ByteBuffer received = m_localInstance.m_applicationToLocalInstanceInterface.get(strKey);
		byte[] result = received.array();

		if (result.length > 0) {
			ret.put(Constants.RESULT, true);
			ret.put(Constants.VALUE, new String(result));
		} else {
			ret.put(Constants.RESULT, false);
			ret.put(Constants.VALUE, "Failed.");
		}

		return ret.toString();
	}

	@Override
	public String put(String strRequest) throws TException {
		JSONObject req = new JSONObject(strRequest);
		String strKey = (String) req.get(Constants.KEY);
		String strValue = (String) req.get(Constants.VALUE);

		//Return
		JSONObject ret = new JSONObject();
		boolean bRet = m_localInstance.m_applicationToLocalInstanceInterface.put(strKey, ByteBuffer.wrap(strValue.getBytes()));

		if (bRet == true) {
			ret.put(Constants.RESULT, true);
			ret.put(Constants.VALUE, "OK");
		} else {
			ret.put(Constants.RESULT, false);
			ret.put(Constants.VALUE, "Failed");
		}

		return ret.toString();
	}

	@Override
	public String lpush(String strRequest) throws TException {
		JSONObject ret = new JSONObject();

		try {
			JSONObject req = new JSONObject(strRequest);
			String strKey = (String) req.get(Constants.KEY);
			String strValue = (String) req.get(Constants.VALUE);

			//Return
			//JSONObject ret = new JSONObject();
			ret.put(Constants.RESULT, true);

			ArrayList list = getListByKey(strKey);

			if (list == null) {
				list = new ArrayList();
			}

			list.add(0, strValue);

			//Write Back
			Gson gson = new Gson();
			m_localInstance.m_applicationToLocalInstanceInterface.put(strKey, ByteBuffer.wrap(gson.toJson(list).getBytes()));
			ret.put(Constants.VALUE, list.size());

		} catch (Exception e) {
			e.printStackTrace();
		}

		return ret.toString();
	}

	@Override
	public String lrange(String strRequest) throws TException {
		//Return
		JSONObject ret = new JSONObject();
		ret.put(Constants.RESULT, true);

		try {
			//For reqeust
			Gson gson = new Gson();
			Type listType = new TypeToken<ArrayList>() {
			}.getType();

			//Reqeust
			JSONObject req = new JSONObject(strRequest);
			String strKey = (String) req.get(Constants.KEY);
			long lFrom = Utils.convertToLong(req.get(Constants.VALUE));
			long lTo = Utils.convertToLong(req.get(Constants.VALUE2));

			ArrayList oriList = getListByKey(strKey);
			ArrayList list;

			if (oriList != null) {
				try {
					list = (ArrayList) oriList.subList((int) lFrom, (int) lTo);
				} catch (Exception e) {
					list = oriList;
				}
			} else {
				list = new ArrayList();
			}

			ret.put(Constants.VALUE, gson.toJson(list));
		} catch (Exception e) {
			e.printStackTrace();
		}

		return ret.toString();
	}

	@Override
	public String llen(String strRequest) throws TException {
		Gson gson = new Gson();
		Type listType = new TypeToken<ArrayList>() {
		}.getType();

		//Reqeust
		JSONObject req = new JSONObject(strRequest);
		String strKey = (String) req.get(Constants.KEY);

		//Return
		JSONObject ret = new JSONObject();
		ret.put(Constants.RESULT, true);
		long lCnt = 0;

		ArrayList list = getListByKey(strKey);

		if (list != null) {
			lCnt = list.size();
		}

		ret.put(Constants.VALUE, lCnt);
		return ret.toString();
	}

	@Override
	public String sadd(String strRequest) throws TException {
		JSONObject ret = new JSONObject();
		ret.put(Constants.RESULT, true);

		try {
			//Reqeust
			JSONObject req = new JSONObject(strRequest);
			String strKey = (String) req.get(Constants.KEY);
			String strValue = (String) req.get(Constants.VALUE);

			//Find the Set
			Set<String> set = getSetByKey(strKey);

			if (set == null) {
				set = new HashSet<String>();
			}

			set.add(strValue);

			//Write Back
			Gson gson = new Gson();
			boolean bRet = m_localInstance.m_applicationToLocalInstanceInterface.put(strKey, ByteBuffer.wrap(gson.toJson(set).getBytes()));
			ret.put(Constants.VALUE, set.size());
		} catch (Exception e) {
			e.printStackTrace();
		}

		return ret.toString();
	}

	@Override
	public String srem(String strRequest) throws TException {
		JSONObject req = new JSONObject(strRequest);
		String strKey = (String) req.get(Constants.KEY);
		String strValue = (String) req.get(Constants.VALUE);

		//Return
		JSONObject ret = new JSONObject();
		ret.put(Constants.RESULT, true);

		//Find the Set
		Set<String> set = getSetByKey(strKey);

		if (set != null) {
			set.remove(strValue);

			//Write Back
			Gson gson = new Gson();
			boolean bRet = m_localInstance.m_applicationToLocalInstanceInterface.put(strKey, ByteBuffer.wrap(gson.toJson(set).getBytes()));
			ret.put(Constants.VALUE, set.size());
		} else {
			ret.put(Constants.VALUE, 0);
		}

		return ret.toString();
	}

	@Override
	public String smembers(String strRequest) throws TException {
		JSONObject req = new JSONObject(strRequest);
		String strKey = (String) req.get(Constants.KEY);

		//Return
		JSONObject ret = new JSONObject();
		ret.put(Constants.RESULT, true);

		//Find the Set
		Set<String> set = getSetByKey(strKey);

		if (set == null) {
			set = new HashSet<String>();
		}

		Gson gson = new Gson();
		ret.put(Constants.VALUE, gson.toJson(set));

		return ret.toString();
	}

	@Override
	public String sismember(String strRequest) throws TException {
		//	System.out.println("sismember");

		//Return
		JSONObject ret = new JSONObject();
		ret.put(Constants.RESULT, true);

		try {
			JSONObject req = new JSONObject(strRequest);
			String strKey = (String) req.get(Constants.KEY);
			String strValue = (String) req.get(Constants.VALUE);

			//Find the Set
			Set<String> set = getSetByKey(strKey);

			if (set != null && set.contains(strValue) == true) {
				ret.put(Constants.VALUE, 1);
			} else {
				ret.put(Constants.VALUE, 0);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return ret.toString();
	}

	@Override
	public String scard(String strRequest) throws TException {
		JSONObject req = new JSONObject(strRequest);
		String strKey = (String) req.get(Constants.KEY);

		//Return
		JSONObject ret = new JSONObject();
		ret.put(Constants.RESULT, true);

		//Find the Set
		Set<String> set = getSetByKey(strKey);

		if (set != null) {
			ret.put(Constants.VALUE, set.size());
		} else {
			ret.put(Constants.VALUE, 0);
		}

		return ret.toString();
	}

	@Override
	public String incr(String strRequest) throws TException {
		//	System.out.println("incr");
		//Reqeust
		JSONObject req = new JSONObject(strRequest);
		String strKey = (String) req.get(Constants.KEY);

		//Return
		JSONObject ret = new JSONObject();
		ret.put(Constants.RESULT, true);
		ByteBuffer received = m_localInstance.m_applicationToLocalInstanceInterface.get(strKey);
		byte[] result = received.array();
		long lNum = 0;

		if (result.length > 0) {
			lNum = Long.parseLong(new String(result));
		} else {
			lNum = 0;
		}

		lNum++;

		//Write Back
		m_localInstance.m_applicationToLocalInstanceInterface.put(strKey, ByteBuffer.wrap(new Long(lNum).toString().getBytes()));
		ret.put(Constants.VALUE, lNum);
		return ret.toString();
	}

	@Override
	public String exists(String strRequest) throws TException {
		JSONObject ret = new JSONObject();
		ret.put(Constants.RESULT, true);

		try {
			//Reqeust
			JSONObject req = new JSONObject(strRequest);
			String strKey = (String) req.get(Constants.KEY);

			//Return
			ByteBuffer received = m_localInstance.m_applicationToLocalInstanceInterface.get(strKey);
			byte[] result = received.array();

			if (result.length > 0) {
				ret.put(Constants.VALUE, 1);
			} else {
				ret.put(Constants.VALUE, 0);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return ret.toString();
	}

	@Override
	public String expire(String strRequest) throws TException {
		JSONObject ret = new JSONObject();
		ret.put(Constants.RESULT, false);
		ret.put(Constants.VALUE, "Not supported yet");
		return ret.toString();
	}

	@Override
	public String hgetAll(final String strRequest) throws TException {
		Latency latency = new Latency();
		latency.start();

		//Reqeust
		JSONObject req = new JSONObject(strRequest);
		String strKey = (String) req.get(Constants.KEY);

		Latency opLat = new Latency();
		opLat.start();
		ByteBuffer received = m_localInstance.m_applicationToLocalInstanceInterface.get(strKey);
		opLat.stop();

		//Return
		JSONObject ret = new JSONObject();
		ret.put(Constants.RESULT, true);
		ret.put(Constants.VALUE, new String(received.array()));

		latency.stop();
		System.out.printf("hgetAll : %.2f\n", latency.getLatencyInMills() - opLat.getLatencyInMills());

		return ret.toString();
	}

	// @return Return OK or Exception if hash is empty
	public String hmset(final String strRequest) throws TException {
		Latency latency = new Latency();
		latency.start();

		//Reqeust
		JSONObject req = new JSONObject(strRequest);
		String strKey = (String) req.get(Constants.KEY);
		String strMap = (String) req.get(Constants.VALUE);

		//Return
		JSONObject ret = new JSONObject();

		Latency opLat = new Latency();
		opLat.start();

		if (m_localInstance.m_applicationToLocalInstanceInterface.put(strKey, ByteBuffer.wrap(strMap.getBytes())) == true) {
			ret.put(Constants.RESULT, true);
			ret.put(Constants.VALUE, Constants.OK);
		} else {
			ret.put(Constants.RESULT, false);
			ret.put(Constants.VALUE, "Exception occurs while put request.");
		}
		opLat.stop();

		latency.stop();
		System.out.printf("hmset : %.2f\n", latency.getLatencyInMills() - opLat.getLatencyInMills());
		return ret.toString();
	}

	@Override
	public String hmget(String strRequest) throws TException //final String strKey, final String... fields)
	{
		Latency latency = new Latency();
		latency.start();

		//Reqeust
		JSONObject req = new JSONObject(strRequest);
		String strKey = (String) req.get(Constants.KEY);
		String strFields = (String) req.get(Constants.VALUE);

		Gson gson = new Gson();
		Type type = new TypeToken<ArrayList<String>>() {
		}.getType();
		ArrayList<String> fields = gson.fromJson(strFields, type);

		//Get Hashmap and find fields
		List<String> ret_list = new ArrayList<>();
		Map<String, String> hash;

		Type mapType = new TypeToken<Map<String, String>>() {
		}.getType();

		Latency opLat = new Latency();
		opLat.start();
		ByteBuffer received = m_localInstance.m_applicationToLocalInstanceInterface.get(strKey);
		opLat.stop();

		//Return
		JSONObject ret = new JSONObject();
		byte[] result = received.array();
		ret.put(Constants.RESULT, true);

		if (result.length > 0) {
			String strJson = new String(result);
			hash = gson.fromJson(strJson, mapType);

			for (String field : fields) {
				ret_list.add(hash.get(field));
			}
		}

		ret.put(Constants.VALUE, gson.toJson(ret_list));

		latency.stop();
		System.out.printf("hmget : %.2f\n", latency.getLatencyInMills() - opLat.getLatencyInMills());
		return ret.toString();
	}

	@Override
	public String zadd(String strRequest) throws TException {
		Latency opLat = new Latency();
		Latency op2Lat = new Latency();
		Latency latency = new Latency();
		latency.start();

		JSONObject ret = new JSONObject();
		long lRet = 0;

		try {
			//Reqeust
			JSONObject req = new JSONObject(strRequest);
			String strKey = (String) req.get(Constants.KEY);
			double dbScore = Utils.convertToDouble(req.get(Constants.VALUE));
			String strMember = (String) req.get(Constants.VALUE2);

			//Get sortedMap First

			opLat.start();
			ByteBuffer received = m_localInstance.m_applicationToLocalInstanceInterface.get(strKey);
			opLat.stop();

			//Return
			byte[] result = received.array();
			ret.put(Constants.RESULT, true);

			Gson gson = new Gson();
			Type type = new TypeToken<DualTreeBidiMap<Double, String>>() {
			}.getType();
			DualTreeBidiMap map;

			if (result.length > 0) {
				String strJson = new String(result);
				map = gson.fromJson(strJson, type);

				if (map.containsValue(strMember) == true) {
					lRet = 1;
					map.removeValue(strMember);
				}
			} else {
				//If not exist, create
				map = new DualTreeBidiMap<Double, String>();
			}

			//need to update member's score.
			map.put(dbScore, strMember);

			op2Lat.start();
			m_localInstance.m_applicationToLocalInstanceInterface.put(strKey, ByteBuffer.wrap(gson.toJson(map).getBytes()));
			op2Lat.stop();

		} catch (Exception e) {
			e.printStackTrace();
		}

		ret.put(Constants.VALUE, lRet);

		latency.stop();
		System.out.printf("zadd : %.2f\n", latency.getLatencyInMills() - op2Lat.getLatencyInMills() - opLat.getLatencyInMills());
		return ret.toString();
	}

	@Override
	public String zrem(String strRequest) throws TException {
		Latency opLat = new Latency();
		Latency op2Lat = new Latency();
		Latency latency = new Latency();
		latency.start();

		//For reqeust
		Gson gson = new Gson();
		Type listType = new TypeToken<ArrayList>() {
		}.getType();

		//Reqeust
		JSONObject req = new JSONObject(strRequest);
		String strKey = (String) req.get(Constants.KEY);
		String strMembers = (String) req.get(Constants.VALUE2);
		ArrayList<String> memberList = gson.fromJson(strMembers, listType);

		//Get sortedMap First
		opLat.start();
		ByteBuffer received = m_localInstance.m_applicationToLocalInstanceInterface.get(strKey);
		opLat.stop();

		//Return
		JSONObject ret = new JSONObject();
		byte[] result = received.array();
		ret.put(Constants.RESULT, true);

		Type mapType = new TypeToken<DualTreeBidiMap<Double, String>>() {
		}.getType();
		DualTreeBidiMap map;
		long lRet = 0;

		if (result.length > 0) {
			String strJson = new String(result);
			map = gson.fromJson(strJson, mapType);

			for (String strMember : memberList) {
				if (map.containsValue(strMember) == true) {
					map.removeValue(strMember);
					lRet++;
				}
			}

			op2Lat.start();
			m_localInstance.m_applicationToLocalInstanceInterface.put(strKey, ByteBuffer.wrap(gson.toJson(map).getBytes()));
			op2Lat.stop();
		}

		ret.put(Constants.VALUE, lRet);

		latency.stop();
		System.out.printf("zrem : %.2f\n", latency.getLatencyInMills() - op2Lat.getLatencyInMills() - opLat.getLatencyInMills());
		return ret.toString();
	}

	@Override
	public String zrangeByScore(String strRequest) throws TException {
		Latency latency = new Latency();
		latency.start();

		//Reqeust
		JSONObject req = new JSONObject(strRequest);
		String strKey = (String) req.get(Constants.KEY);
		double dbMin = Utils.convertToDouble(req.get(Constants.VALUE2));
		double dbMax = Utils.convertToDouble(req.get(Constants.VALUE3));
		long lOffset = Utils.convertToLong(req.get(Constants.VALUE4));
		long lCount = Utils.convertToLong(req.get(Constants.VALUE5));

		//Get sortedMap First
		Latency opLat = new Latency();
		opLat.start();
		ByteBuffer received = m_localInstance.m_applicationToLocalInstanceInterface.get(strKey);
		opLat.stop();

		//Return
		JSONObject ret = new JSONObject();
		byte[] result = received.array();
		ret.put(Constants.RESULT, true);

		Gson gson = new Gson();
		Type type = new TypeToken<DualTreeBidiMap<Double, String>>() {
		}.getType();
		DualTreeBidiMap map;
		long lRet = 0;
		Set<String> retSet;

		if (result.length > 0) {
			String strJson = new String(result);
			map = gson.fromJson(strJson, type);
			retSet = map.subMap(dbMin, dbMax).keySet();
		} else {
			retSet = Collections.emptySet();
		}

		ret.put(Constants.VALUE, gson.toJson(retSet));

		latency.stop();
		System.out.printf("zrangeByScore : %.2f\n", latency.getLatencyInMills() - opLat.getLatencyInMills());
		return ret.toString();
	}

	@Override
	public String remove(String strRequest) throws TException {
		//System.out.println("remove");

		//Reqeust
		JSONObject req = new JSONObject(strRequest);
		String strKey = (String) req.get(Constants.KEY);

		//Return
		JSONObject ret = new JSONObject();

		try {
			if (m_localInstance.m_applicationToLocalInstanceInterface.remove(strKey) == true) {
				ret.put(Constants.RESULT, true);
				ret.put(Constants.VALUE, 1);
			} else {
				ret.put(Constants.RESULT, false);
				ret.put(Constants.VALUE, 0);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return ret.toString();
	}
}