package umn.dcsg.wieralocalserver.events;

import umn.dcsg.wieralocalserver.responses.Response;
import umn.dcsg.wieralocalserver.LocalInstance;

import java.util.Map;

/**
 * Created by Kwangsung on 12/21/2015.
 */

public class MonitoringRequestCntEvent extends MonitoringEvent {
	private static final long serialVersionUID = 2556678386039177452L;
	private long m_nPeriodThreshold; //How many seconds are latency violated allowed.
	private long m_timeViolateStart = 0;
	private String m_strPotentialHostname = m_localInstance.m_peerInstanceManager.getPrimaryPeerHostname();

//	private long m_timeViolateReqestCntStart = 0;
//	private long m_timeViolateForwaredReqestCntStart = 0;

	public MonitoringRequestCntEvent(LocalInstance instance, Response response, int nPeriodThreshold) {
		super(instance, response);
		this.m_nPeriodThreshold = nPeriodThreshold;
	}

    @Override
    Map<String, Object> doCheckMonitoringEventCondition(Map<String, Object> eventParams, String strTriggerName) {
    	//Check last 30 seconds.
		String strPotentialPrimaryHostname = findInstanceForwardingMore(30);

		if (strPotentialPrimaryHostname != null) {
			if (m_timeViolateStart == 0 || strPotentialPrimaryHostname.equals(m_strPotentialHostname) == false) {
				m_strPotentialHostname = strPotentialPrimaryHostname;
				m_timeViolateStart = System.currentTimeMillis();
			} else {    //Check if
				long elapsed = System.currentTimeMillis() - m_timeViolateStart;

				if (elapsed > m_nPeriodThreshold) {
					System.out.format("Now time period also Change primary to : %s\n", strPotentialPrimaryHostname);
					m_timeViolateStart = 0;
					m_strPotentialHostname = "";

					return eventParams;
				}
			}
		} else {
			m_timeViolateStart = 0;
			m_strPotentialHostname = "";
		}

        return null;
	}

	private String findInstanceForwardingMore(long lPeriodInSec) {
		//check forwarded message
		//This can condisier put or get or both. (in here only put for simplicity)
		long lCurTime = System.currentTimeMillis();
		long lDirectRequestCnt = m_localInstance.m_localInfo.getPutReqCntInPeriod(lCurTime, lPeriodInSec);
		long lDifference;
		long lMaxDifference = 0;
		long lForwardedRequestCnt;
		String strCurMostPopularHostName = m_strPotentialHostname;

		for (String strHostname : m_localInstance.m_peerInstanceManager.getPeersHostnameList()) {
			lForwardedRequestCnt = m_localInstance.m_localInfo.getForwardedPutReqCntInPeriod(strHostname, lCurTime, lPeriodInSec);

			//Compare cnt between direct request from application and forwarded
			if (lDirectRequestCnt < lForwardedRequestCnt) {
				lDifference = lForwardedRequestCnt - lDirectRequestCnt;

				if (lDifference > lMaxDifference) {
					lMaxDifference = lDifference;
					strCurMostPopularHostName = strHostname;
				}
			}
		}

		return strCurMostPopularHostName;
	}
}