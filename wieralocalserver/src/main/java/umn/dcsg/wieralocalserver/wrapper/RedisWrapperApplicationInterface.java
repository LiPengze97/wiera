package umn.dcsg.wieralocalserver.wrapper;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.codec.binary.Base64;
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

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by Kwangsung on 1/30/2017.
 */
//LocalInstanceCLI library for wrapping class
//If only function can be done with single communication, it is handled only with get/put
//If not, Wiera side fuction provided.
public class RedisWrapperApplicationInterface implements RedisWrapperApplicationIface.Iface {
	LocalInstance m_localInstance;

	public RedisWrapperApplicationInterface(LocalInstance instance) {
		m_localInstance = instance;
	}

	private ArrayList<String> getListByKey(String strKey) throws TException {
		ArrayList list;

		try {
			//For reqeust
			Gson gson = new Gson();
			Type setType = new TypeToken<ArrayList>() {
			}.getType();

			//Result
			ByteBuffer result = m_localInstance.m_applicationToLocalInstanceInterface.get(strKey);
			JSONObject obj = new JSONObject(new String(result.array(), result.position(), result.remaining()));

			boolean bRet = obj.getBoolean(RESULT);

			if(bRet == true) {
				//Return
				byte[] ret = Base64.decodeBase64((String) obj.get(VALUE));;
				String strJson = new String(ret);
				list = gson.fromJson(strJson, setType);
				return list;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	private Set<String> getSetByKey(String strKey) throws TException {
		Set set = null;

		try {
			Gson gson = new Gson();
			Type setType = new TypeToken<Set>() {
			}.getType();

			ByteBuffer result = m_localInstance.m_applicationToLocalInstanceInterface.get(strKey);
			JSONObject obj = new JSONObject(new String(result.array(), result.position(), result.remaining()));

			boolean bRet = obj.getBoolean(RESULT);
			byte[] value = Base64.decodeBase64((String) obj.get(VALUE));;

			if (bRet == true && value.length > 0) {
				String strJson = new String(value);
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
		String strKey = (String) req.get(KEY);

		//Return
		JSONObject ret = new JSONObject();
		ByteBuffer received = m_localInstance.m_applicationToLocalInstanceInterface.get(strKey);
		byte[] result = received.array();

		if (result.length > 0) {
			ret.put(RESULT, true);
			ret.put(VALUE, new String(result));
		} else {
			ret.put(RESULT, false);
			ret.put(VALUE, "Failed.");
		}

		return ret.toString();
	}

	@Override
	public String put(String strRequest) throws TException {
		JSONObject req = new JSONObject(strRequest);
		String strKey = (String) req.get(KEY);
		String strValue = (String) req.get(VALUE);

		//Result
		ByteBuffer result = m_localInstance.m_applicationToLocalInstanceInterface.put(strKey, ByteBuffer.wrap(strValue.getBytes()));
		JSONObject obj = new JSONObject(new String(result.array(), result.position(), result.remaining()));

		//Return
		JSONObject ret = new JSONObject();

		boolean bRet = obj.getBoolean(RESULT);

		if (bRet == true) {
			ret.put(RESULT, true);
			ret.put(VALUE, "OK");
		} else {
			ret.put(RESULT, false);
			ret.put(VALUE, "Failed");
		}

		return ret.toString();
	}

	@Override
	public String lpush(String strRequest) throws TException {
		JSONObject ret = new JSONObject();

		try {
			JSONObject req = new JSONObject(strRequest);
			String strKey = (String) req.get(KEY);
			String strValue = (String) req.get(VALUE);

			//Return
			//JSONObject ret = new JSONObject();
			ArrayList list = getListByKey(strKey);

			if (list == null) {
				list = new ArrayList();
			}

			list.add(0, strValue);

			//Write Back
			Gson gson = new Gson();

			//Result
			ByteBuffer result = m_localInstance.m_applicationToLocalInstanceInterface.put(strKey, ByteBuffer.wrap(gson.toJson(list).getBytes()));
			JSONObject obj = new JSONObject(new String(result.array(), result.position(), result.remaining()));

			boolean bRet = obj.getBoolean(RESULT);

			if (bRet == true) {
				ret.put(RESULT, true);
				ret.put(VALUE, list.size());
			} else {
				ret.put(RESULT, false);
				ret.put(VALUE, 0);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return ret.toString();
	}

	@Override
	public String lrange(String strRequest) throws TException {
		//Return
		JSONObject ret = new JSONObject();
		ret.put(RESULT, true);

		try {
			//For reqeust
			Gson gson = new Gson();
			Type listType = new TypeToken<ArrayList>() {
			}.getType();

			//Reqeust
			JSONObject req = new JSONObject(strRequest);
			String strKey = (String) req.get(KEY);
			long lFrom = Utils.convertToLong(req.get(VALUE));
			long lTo = Utils.convertToLong(req.get(VALUE2));

			ArrayList oriList = getListByKey(strKey);
			ArrayList list;
			boolean bRet;

			if (oriList != null) {
				try {
					list = (ArrayList) oriList.subList((int) lFrom, (int) lTo);
				} catch (Exception e) {
					list = oriList;
				}
				bRet = true;
			} else {
				list = new ArrayList();
				bRet = false;
			}

			if (bRet == true) {
				ret.put(RESULT, true);
			} else {
				ret.put(RESULT, false);
			}

			ret.put(VALUE, gson.toJson(list));
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
		String strKey = (String) req.get(KEY);

		//Return
		JSONObject ret = new JSONObject();
		long lCnt = 0;

		ArrayList list = getListByKey(strKey);
		boolean bRet;

		if (list != null) {
			lCnt = list.size();
			bRet = true;
		} else {
			bRet = false;
		}

		if (bRet == true) {
			ret.put(RESULT, true);
		} else {
			ret.put(RESULT, false);
		}

		ret.put(VALUE, lCnt);
		return ret.toString();
	}

	@Override
	public String sadd(String strRequest) throws TException {
		JSONObject ret = new JSONObject();

		try {
			//Reqeust
			JSONObject req = new JSONObject(strRequest);
			String strKey = (String) req.get(KEY);
			String strValue = (String) req.get(VALUE);

			//Find the Set
			Set<String> set = getSetByKey(strKey);

			if (set == null) {
				set = new HashSet<String>();
			}

			set.add(strValue);

			//Write Back
			//Result
			Gson gson = new Gson();
			ByteBuffer result = m_localInstance.m_applicationToLocalInstanceInterface.put(strKey, ByteBuffer.wrap(gson.toJson(set).getBytes()));
			JSONObject obj = new JSONObject(new String(result.array(), result.position(), result.remaining()));

			boolean bRet = obj.getBoolean(RESULT);

			if (bRet == true) {
				ret.put(RESULT, true);
			} else {
				ret.put(RESULT, false);
			}
			ret.put(VALUE, set.size());
		} catch (Exception e) {
			e.printStackTrace();
		}

		return ret.toString();
	}

	@Override
	public String srem(String strRequest) throws TException {
		JSONObject req = new JSONObject(strRequest);
		String strKey = (String) req.get(KEY);
		String strValue = (String) req.get(VALUE);

		//Return
		JSONObject ret = new JSONObject();
		ret.put(RESULT, true);

		//Find the Set
		Set<String> set = getSetByKey(strKey);

		if (set != null) {
			set.remove(strValue);

			//Write Back
			Gson gson = new Gson();
			ByteBuffer result = m_localInstance.m_applicationToLocalInstanceInterface.put(strKey, ByteBuffer.wrap(gson.toJson(set).getBytes()));
			JSONObject obj = new JSONObject(new String(result.array(), result.position(), result.remaining()));

			boolean bRet = obj.getBoolean(RESULT);

			if (bRet == true) {
				ret.put(RESULT, true);
			} else {
				ret.put(RESULT, false);
			}

			ret.put(VALUE, set.size());
		} else {
			ret.put(VALUE, 0);
		}

		return ret.toString();
	}

	@Override
	public String smembers(String strRequest) throws TException {
		JSONObject req = new JSONObject(strRequest);
		String strKey = (String) req.get(KEY);

		//Return
		JSONObject ret = new JSONObject();
		ret.put(RESULT, true);

		//Find the Set
		Set<String> set = getSetByKey(strKey);

		if (set == null) {
			set = new HashSet<String>();
		}

		Gson gson = new Gson();
		ret.put(VALUE, gson.toJson(set));

		return ret.toString();
	}

	@Override
	public String sismember(String strRequest) throws TException {
		//	System.out.println("sismember");

		//Return
		JSONObject ret = new JSONObject();
		ret.put(RESULT, true);

		try {
			JSONObject req = new JSONObject(strRequest);
			String strKey = (String) req.get(KEY);
			String strValue = (String) req.get(VALUE);

			//Find the Set
			Set<String> set = getSetByKey(strKey);

			if (set != null && set.contains(strValue) == true) {
				ret.put(VALUE, 1);
			} else {
				ret.put(VALUE, 0);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return ret.toString();
	}

	@Override
	public String scard(String strRequest) throws TException {
		JSONObject req = new JSONObject(strRequest);
		String strKey = (String) req.get(KEY);

		//Return
		JSONObject ret = new JSONObject();
		ret.put(RESULT, true);

		//Find the Set
		Set<String> set = getSetByKey(strKey);

		if (set != null) {
			ret.put(VALUE, set.size());
		} else {
			ret.put(VALUE, 0);
		}

		return ret.toString();
	}

	@Override
	public String incr(String strRequest) throws TException {
		//	System.out.println("incr");
		//Reqeust
		JSONObject req = new JSONObject(strRequest);
		String strKey = (String) req.get(KEY);

		//Return
		JSONObject ret = new JSONObject();
		ret.put(RESULT, true);

		ByteBuffer result = m_localInstance.m_applicationToLocalInstanceInterface.get(strKey);
		JSONObject obj = new JSONObject(new String(result.array(), result.position(), result.remaining()));

		boolean bRet = obj.getBoolean(RESULT);
		byte[] value = Base64.decodeBase64((String) obj.get(VALUE));
		long lNum = 0;

		if (bRet == true && value.length > 0) {
			lNum = Long.parseLong(new String(value));
		} else {
			lNum = 0;
		}

		lNum++;

		//Write Back
		result = m_localInstance.m_applicationToLocalInstanceInterface.put(strKey, ByteBuffer.wrap(new Long(lNum).toString().getBytes()));
		ret.put(VALUE, lNum);
		return ret.toString();
	}

	@Override
	public String exists(String strRequest) throws TException {
		JSONObject ret = new JSONObject();
		ret.put(RESULT, true);

		try {
			//Reqeust
			JSONObject req = new JSONObject(strRequest);
			String strKey = (String) req.get(KEY);

			//Return
			ByteBuffer result = m_localInstance.m_applicationToLocalInstanceInterface.get(strKey);
			JSONObject obj = new JSONObject(new String(result.array(), result.position(), result.remaining()));

			boolean bRet = obj.getBoolean(RESULT);

			if (bRet == true) {
				ret.put(VALUE, 1);
			} else {
				ret.put(VALUE, 0);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return ret.toString();
	}

	@Override
	public String expire(String strRequest) throws TException {
		JSONObject ret = new JSONObject();
		ret.put(RESULT, false);
		ret.put(VALUE, "Not supported yet");
		return ret.toString();
	}

	@Override
	public String hgetAll(final String strRequest) throws TException {
		Latency latency = new Latency();
		latency.start();

		//Reqeust
		JSONObject req = new JSONObject(strRequest);
		String strKey = (String) req.get(KEY);

		Latency opLat = new Latency();
		opLat.start();

		ByteBuffer result = m_localInstance.m_applicationToLocalInstanceInterface.get(strKey);
		JSONObject obj = new JSONObject(new String(result.array(), result.position(), result.remaining()));

		opLat.stop();

		//Return
		JSONObject ret = new JSONObject();
		ret.put(RESULT, true);
		ret.put(VALUE, new String(Base64.decodeBase64((String) obj.get(VALUE))));

		System.out.printf("hgetAll : %.2f\n", latency.getLatencyInMills() - opLat.getLatencyInMills());

		return ret.toString();
	}

	// @return Return OK or Exception if hash is empty
	public String hmset(final String strRequest) throws TException {
		Latency latency = new Latency();
		latency.start();

		//Reqeust
		JSONObject req = new JSONObject(strRequest);
		String strKey = (String) req.get(KEY);
		String strMap = (String) req.get(VALUE);

		//Return
		JSONObject ret = new JSONObject();

		Latency opLat = new Latency();
		opLat.start();

		ByteBuffer result = m_localInstance.m_applicationToLocalInstanceInterface.put(strKey, ByteBuffer.wrap(strMap.getBytes()));
		JSONObject obj = new JSONObject(new String(result.array(), result.position(), result.remaining()));

		boolean bRet = obj.getBoolean(RESULT);

		if (bRet == true) {
			ret.put(RESULT, true);
			ret.put(VALUE, OK);
		} else {
			ret.put(RESULT, false);
			ret.put(VALUE, "Exception occurs while put request.");
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
		String strKey = (String) req.get(KEY);
		String strFields = (String) req.get(VALUE);

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

		ByteBuffer result = m_localInstance.m_applicationToLocalInstanceInterface.get(strKey);
		JSONObject obj = new JSONObject(new String(result.array(), result.position(), result.remaining()));

		opLat.stop();

		boolean bRet = obj.getBoolean(RESULT);
		byte[] value = Base64.decodeBase64((String) obj.get(VALUE));

		//Return
		JSONObject ret = new JSONObject();
		ret.put(RESULT, true);

		if (bRet == true && value.length > 0) {
			String strJson = new String(value);
			hash = gson.fromJson(strJson, mapType);

			for (String field : fields) {
				ret_list.add(hash.get(field));
			}
		}

		ret.put(VALUE, gson.toJson(ret_list));

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
			String strKey = (String) req.get(KEY);
			double dbScore = Utils.convertToDouble(req.get(VALUE));
			String strMember = (String) req.get(VALUE2);

			//Get sortedMap First
			opLat.start();

			ByteBuffer result = m_localInstance.m_applicationToLocalInstanceInterface.get(strKey);
			JSONObject obj = new JSONObject(new String(result.array(), result.position(), result.remaining()));
			opLat.stop();

			boolean bRet = obj.getBoolean(RESULT);
			byte[] value = Base64.decodeBase64((String) obj.get(VALUE));

			//Return
			ret.put(RESULT, true);

			Gson gson = new Gson();
			Type type = new TypeToken<DualTreeBidiMap<Double, String>>() {
			}.getType();
			DualTreeBidiMap map;

			if (bRet == true && value.length > 0) {
				String strJson = new String(value);
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

		ret.put(VALUE, lRet);

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
		String strKey = (String) req.get(KEY);
		String strMembers = (String) req.get(VALUE2);
		ArrayList<String> memberList = gson.fromJson(strMembers, listType);

		//Get sortedMap First
		opLat.start();
		ByteBuffer result = m_localInstance.m_applicationToLocalInstanceInterface.get(strKey);
		JSONObject obj = new JSONObject(new String(result.array(), result.position(), result.remaining()));
		opLat.stop();

		//Return
		JSONObject ret = new JSONObject();
		boolean bRet = obj.getBoolean(RESULT);
		byte[] value = Base64.decodeBase64((String) obj.get(VALUE));

		ret.put(RESULT, true);

		Type mapType = new TypeToken<DualTreeBidiMap<Double, String>>() {
		}.getType();
		DualTreeBidiMap map;
		long lRet = 0;

		if (bRet == true && value.length > 0) {
			String strJson = new String(value);
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

		ret.put(VALUE, lRet);

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
		String strKey = (String) req.get(KEY);
		double dbMin = Utils.convertToDouble(req.get(VALUE2));
		double dbMax = Utils.convertToDouble(req.get(VALUE3));
		long lOffset = Utils.convertToLong(req.get(VALUE4));
		long lCount = Utils.convertToLong(req.get(VALUE5));

		//Get sortedMap First
		Latency opLat = new Latency();
		opLat.start();

		ByteBuffer result = m_localInstance.m_applicationToLocalInstanceInterface.get(strKey);
		JSONObject obj = new JSONObject(new String(result.array(), result.position(), result.remaining()));
		opLat.stop();

		boolean bRet = obj.getBoolean(RESULT);
		byte[] value = Base64.decodeBase64((String) obj.get(VALUE));

		//Return
		JSONObject ret = new JSONObject();

		ret.put(RESULT, true);

		Gson gson = new Gson();
		Type type = new TypeToken<DualTreeBidiMap<Double, String>>() {
		}.getType();
		DualTreeBidiMap map;
		Set<String> retSet;

		if (bRet == true && value.length > 0) {
			String strJson = new String(value);
			map = gson.fromJson(strJson, type);
			retSet = map.subMap(dbMin, dbMax).keySet();
		} else {
			retSet = Collections.emptySet();
		}

		ret.put(VALUE, gson.toJson(retSet));

		latency.stop();
		System.out.printf("zrangeByScore : %.2f\n", latency.getLatencyInMills() - opLat.getLatencyInMills());
		return ret.toString();
	}

	@Override
	public String remove(String strRequest) throws TException {
		//System.out.println("remove");

		//Reqeust
		JSONObject req = new JSONObject(strRequest);
		String strKey = (String) req.get(KEY);

		//Return
		JSONObject ret = new JSONObject();

		try {
			ByteBuffer result = m_localInstance.m_applicationToLocalInstanceInterface.remove(strKey);
			JSONObject obj = new JSONObject(new String(result.array(), result.position(), result.remaining()));
			boolean bRet = obj.getBoolean(RESULT);

			if (bRet == true) {
				ret.put(RESULT, true);
				ret.put(VALUE, 1);
			} else {
				ret.put(RESULT, false);
				ret.put(VALUE, 0);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return ret.toString();
	}
}