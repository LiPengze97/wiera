package umn.dcsg.wieralocalserver.events;

import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.responses.Response;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Kwangsung on 12/17/2015.
 */
public class MonitoringTierLatencyEvent extends MonitoringEvent {
    private static final long serialVersionUID = 2550078386039177451L;
    private long m_nPeriodThreshold; //How many seconds are latency violated allowed.
    private long m_nLatencyThreshold;
    private LocalInstance m_instance;
    private String m_strOPType;
    private Boolean lock = false;

    HashMap<String, Long> m_violationList = new HashMap<>();

    MonitoringTierLatencyEvent(LocalInstance instance, Response response, int nLatencyThreshold, int nPeriodThreshold, String strOPType) {
        super(instance, response);
        m_instance = instance;
        this.m_nPeriodThreshold = nPeriodThreshold;
        this.m_nLatencyThreshold = nLatencyThreshold;
        this.m_strOPType = strOPType;
    }

    public MonitoringTierLatencyEvent(LocalInstance instance, Map <String, Object> params, List lstResponse) {
        super(instance, params, lstResponse);
    }

    protected Map<String, Object> doCheckMonitoringEventCondition(Map<String, Object> eventParams, String strEventTrigger) {
        //DataDistributionUtil consistecy = m_instance.m_peerInstanceManager.getDataDistribution();
        //long lBrocastLatency = 0;

        /*if (consistecy.getDistributionType() == DataDistributionUtil.DATA_DISTRIBUTION_TYPE.TRIPS) {
            //Check sla violation current data placement.
            /*OperationLatency distributionLatency = m_instance.m_staticInfo.getLastOperationLatency(m_strOPType);
            String strTierName = distributionLatency.getLocale();

            if(distributionLatency.getLatency() >= m_nLatencyThreshold)
            {
                //This means this tier violates sla before.
                //check time
                if(m_violationList.containsKey(strTierName) == true)
                {

                }
                else
                {
                    //m_violationList.
                }

                }
        }*/

        return null;
    }
}