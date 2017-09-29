package umn.dcsg.wieralocalserver.storageinterfaces;

/**
 * Created by Kwangsung on 1/15/2016.
 */
//Not yet.
public class WieraInstanceTierInterface extends StorageInterface {
//	WieraRedisWrapperClient m_wieraClient;

	public WieraInstanceTierInterface(String strWieraIP, int nWieraPort, String strWieraID) {
//		m_wieraClient = new WieraRedisWrapperClient(strWieraIP, nWieraPort, strWieraID);
	}

	@Override
	public boolean put(String key, byte[] value) {
		/*ByteBuffer buf = ByteBuffer.wrap(value);
		try
		{
			return m_wieraClient.getLocalInstanceClient(0).put(key, buf);
		}
		catch (TException e)
		{
			e.printStackTrace();
		}*/

		return false;
	}

	@Override
	public byte[] get(String key) {
	/*	ByteBuffer value = null;

		try
		{
			value = m_wieraClient.getLocalInstanceClient(0).get(key);
		}
		catch (TTransportException e)
		{
			e.printStackTrace();
		}
		catch (TException e)
		{
			e.printStackTrace();
		}*/

		return null;
	}

	@Override
	public boolean delete(String key) {
/*
		try
		{
			return m_wieraClient.getLocalInstanceClient(0).remove(key);
		}
		catch (TException e)
		{
			e.printStackTrace();
		}
*/

		return false;
	}

	@Override
	protected boolean growTier(int byPercent) {
		return false;
	}

	@Override
	protected boolean shrinkTier(int byPercent) {
		return false;
	}

}