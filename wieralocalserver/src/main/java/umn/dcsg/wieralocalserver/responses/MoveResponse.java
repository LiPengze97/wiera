package umn.dcsg.wieralocalserver.responses;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.util.concurrent.RateLimiter;
import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.Locale;
import umn.dcsg.wieralocalserver.MetaObjectInfo;
import umn.dcsg.wieralocalserver.responses.peers.ForwardGetResponse;
import umn.dcsg.wieralocalserver.responses.peers.ForwardPutResponse;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by ajay on 7/13/13.
 */
public class MoveResponse extends Response {
	public MoveResponse(LocalInstance instance, String strEventName, Map<String, Object> params) {
		super(instance, strEventName, params);
	}

	@Override
	protected void InitRequiredParams() {
		m_lstRequiredParams.add(KEY_LIST);
		m_lstRequiredParams.add(FROM);
		m_lstRequiredParams.add(TO);
		m_lstRequiredParams.add(RATE);
	}

	//Need to check this works.
	@Override
	public boolean respond(Map<String, Object> responseParams) {
		boolean bRet = true;

		//Locale (from) : MetaObject : versions
		HashMap<Locale, Map<MetaObjectInfo, Vector<Integer>>> keyList = (HashMap<Locale, Map<MetaObjectInfo, Vector<Integer>>>) responseParams.get(KEY_LIST);
		Locale fromLocale = m_localInstance.getLocaleWithID((String) responseParams.get(FROM));
		Locale targetLocale = m_localInstance.getLocaleWithID((String) responseParams.get(TO));
		Double rate = (Double) responseParams.get(RATE);
		RateLimiter rateLimiter = RateLimiter.create(rate);
		String strKey;
		String strVersionedKey;
		int nVer;
		Vector verList;
		String strReason = NOT_HANDLED  + " in " + getClass().getSimpleName();

		for (Locale locale: keyList.keySet()) {
			System.out.println("[debug] - Locale : " + locale.getLocaleID());

			for(MetaObjectInfo obj: keyList.get(locale).keySet()) {
				System.out.println("[debug] --- Key List: " + obj.getKey());
			}
		}

		//Locale now override equals()
		if(keyList.containsKey(fromLocale) == true && keyList.get(fromLocale).size() > 0) {
			Map<MetaObjectInfo, Vector<Integer>> keys = keyList.get(fromLocale);

			for (Map.Entry<MetaObjectInfo, Vector<Integer>> entry : keys.entrySet()) {
				rateLimiter.acquire();

				MetaObjectInfo obj = entry.getKey();
				strKey = obj.getKey();
				verList = entry.getValue();

				ReentrantReadWriteLock lock = m_localInstance.m_keyLocker.getLock(strKey);

				try {
					int nVerCnt = verList.size();

					lock.writeLock().lock();

					for (int i = 0; i < nVerCnt; i++) {
						nVer = (int) verList.get(i);
						strVersionedKey = obj.getVersionedKey(nVer);

						//Check whether version is in required tier.
						//Need to be optimized to avoid multiple search
						//Todo, We might simply ask to targetInstance to move data into other rather than retrieving it to local
						if (fromLocale.equals(obj.getLocale(nVer, false)) == false) {
							//System.out.format("Key: %s From tier: %s, tier in meta: %s\n", strVersionedKey, strFromTier, obj.getLocale(lVer));
							continue;
						}

						byte[] value;

						//Check from locale
						if (fromLocale.isLocalLocale() == true) {
							value = m_localInstance.getInternal(strVersionedKey, fromLocale.getTierName());
						} else {
							//forward get operation if not local locale
							bRet = Response.respondAtRuntimeWithClass(m_localInstance, ForwardGetResponse.class, responseParams);

							if (bRet == false) {
								strReason = "Failed to forward get in MoveResponse: " + responseParams.get(REASON);
								break;
							} else {
								value = (byte[]) responseParams.get(VALUE);
							}
						}

						//Check target locale
						if (targetLocale.isLocalLocale() == true) {
							bRet = m_localInstance.putInternal(strVersionedKey, value, targetLocale.getTierName());
						} else {
							bRet = Response.respondAtRuntimeWithClass(m_localInstance, ForwardPutResponse.class, responseParams);

							if (bRet == false) {
								strReason = "Failed to forward put in MoveResponse: " + responseParams.get(REASON);
								break;
							}
						}

						if (bRet == true) {
							System.out.format("[debug] move key \"%s\" from %s to %s\n", strVersionedKey, fromLocale.getTierName(), targetLocale.getTierName());
							obj.addLocale(nVer, targetLocale);
							obj.setLAT();

							if (fromLocale.isLocalLocale() == true) {
								if (m_localInstance.deleteInternal(obj, fromLocale.getTierName())) {
									if (obj.removeLocale(nVer, fromLocale) == false) {
										System.out.println("[debug] Failed to remove Locale: " + fromLocale.getLocaleID());
									}

								} else {
									//Failed so recover
									m_localInstance.deleteInternal(obj, targetLocale.getTierName());
									strReason = String.format("Failed to remove the Key: %s from: %s in MoveReponse", strKey, fromLocale.getTierName());
									bRet = false;
									break;
								}
							}
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					lock.writeLock().unlock();

					if (!bRet) {
						break;
					} else {
						//Store
						addMetaToUpdate(obj, responseParams);
					}
				}
			}
		}

		//Put the reason
		responseParams.put(REASON, strReason);
		return bRet;
	}

	@Override
	public void doPrepareResponseParams(Map<String, Object> responseParams) {
		if (responseParams.containsKey(FROM) == false) {
			responseParams.put(FROM, m_initParams.get(FROM));
		}

		if (responseParams.containsKey(TO) == false) {
			responseParams.put(TO, m_initParams.get(TO));
		}

		if (responseParams.containsKey(RATE) == false) {
			if(m_initParams.containsKey(RATE) == true) {
				responseParams.put(RATE, m_initParams.get(RATE));
			} else {
				responseParams.put(RATE, 5.0);
			}
		}
	}

	@Override
	public boolean doCheckResponseConditions(Map<String, Object> responseParams) {
		return true;
	}
}