package umn.dcsg.wieralocalserver.events;

import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.info.OperationLatency;
import umn.dcsg.wieralocalserver.utils.Utils;

import java.util.List;
import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by Kwangsung on 12/17/2015.
 */
public class MonitoringOperationLatencyEvent extends MonitoringEvent {
    private static final long serialVersionUID = 2550078386039177453L;
    private long m_lPeriodViolateStart;
    private int m_nPeriodThreshold;
    private int m_nThreshold;
    private String m_strOperation;
    private String m_strOPLatencyType;
    private String m_strRelationalOperator;

    public MonitoringOperationLatencyEvent(LocalInstance instance, Map <String, Object> params, List lstResponse) {
        super(instance, params, lstResponse);
        m_nThreshold = Utils.convertToInteger(params.get(LATENCY_THRESHOLD));
        m_nPeriodThreshold = Utils.convertToInteger(params.get(PERIOD_THRESHOLD));
        m_strOperation = (String)params.get(OPERATION);
        m_strRelationalOperator = (String)params.get(RELATIONAL_OPERATION);

        if(m_strOperation.equals(ACTION_PUT_EVENT) == true) {
            m_strOPLatencyType = PUT_LATENCY;
        } else {
            m_strOPLatencyType = GET_LATENCY;
        }
    }

    protected Map<String, Object> doCheckMonitoringEventCondition(Map<String, Object> eventParams, String strEventTrigger) {
        //Todo need to check last 30 seconds
        OperationLatency operationLatency = m_localInstance.m_localInfo.getLatestOperationInfo(m_strOPLatencyType);

        if(operationLatency == null) {
            System.out.println("[debug] No operation latency information found. Stop responding");
            return null;
        }

        double dbOperationLat = operationLatency.getLatencyInMills();
        boolean bViolated = checkCondition(m_strRelationalOperator, dbOperationLat, m_nThreshold);

        //violated
        if (bViolated == true) {
            System.out.format("[debug] %s Latency violated for now : %f ms threshold: %d ms\n", m_strOPLatencyType, dbOperationLat, m_nPeriodThreshold);

            if (m_lPeriodViolateStart == 0) {
                m_lPeriodViolateStart = System.currentTimeMillis();
            } else {
                long elapsed = System.currentTimeMillis() - m_lPeriodViolateStart;

                if (elapsed > m_nPeriodThreshold) {
                    System.out.format("[debug] Period violated for now, change data distribution\n");
                    m_lPeriodViolateStart = 0;
                    return eventParams;
                }
            }
        } else {
            m_lPeriodViolateStart = 0;
        }

        return null;
    }
}