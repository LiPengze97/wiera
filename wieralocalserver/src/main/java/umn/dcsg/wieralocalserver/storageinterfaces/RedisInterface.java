package umn.dcsg.wieralocalserver.storageinterfaces;

/**
 * Created by Kwangsung on 1/15/2016.
 */

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisInterface extends StorageInterface {
	private JedisPool m_pool;
	private String m_serverAddress = "localhost";

	RedisInterface() {
		m_pool = new JedisPool(m_serverAddress);
	}

	public RedisInterface(String strServerAddress) {
		this();
		m_serverAddress = strServerAddress;
	}

	public boolean put(String key, byte[] value) {
		try (Jedis jedis = m_pool.getResource()) {
			jedis.set(key.getBytes(), value);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}

		return false;
	}

	public byte[] get(String key) {
		try (Jedis jedis = m_pool.getResource()) {
			/// ... do stuff here ... for example
			return jedis.get(key.getBytes());
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	@Override
	protected boolean delete(String key) {
		try (Jedis jedis = m_pool.getResource()) {
			return jedis.del(key) > 0;
		} catch (Exception e) {
			e.printStackTrace();
		}

		return false;
	}

	@Override
	protected boolean growTier(int byPercent) {
		return true;
	}

	@Override
	protected boolean shrinkTier(int byPercent) {
		return true;
	}
}