package umn.dcsg.wieralocalserver.events;

import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.utils.Utils;

import java.util.List;
import java.util.Map;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created with IntelliJ IDEA.
 * User: ajay
 * Date: 28/03/13
 * Time: 11:10 PM
 * To change this template use File | Settings | File Templates.
 */

// This event is associated with an attribute in a particular object
// Events should be maintained with objects so that in actions that
// affect them we can access events
    // Threshold events seem to be very tricky to really realize, I believe each threshold event is associated with an
    // action, we can probably run them in the background
public class MonitoringTierCapacityEvent extends MonitoringEvent {
    private static final long serialVersionUID = 2330078386039177453L;
    // Set the threshold, which when reached will
    // trigger an event and may a corresponding response
    private long m_lThreshold;
    private String m_strTargetTier;

    public MonitoringTierCapacityEvent(LocalInstance instance, Map <String, Object> params, List lstResponse) {
        super(instance, params, lstResponse);

        m_strTargetTier = (String)params.get(TIER_NAME);
        m_lThreshold = Utils.convertToLong(params.get(CAPACITY_THRESHOLD));
    }

    protected Map<String, Object> doCheckMonitoringEventCondition(Map<String, Object> eventParams, String strEventTrigger) {
        System.out.printf("[debug] Used space %f%s, and threshold %d%s\n", m_localInstance.m_tiers.getTier(m_strTargetTier).getUsedRateInPercent(), "%", m_lThreshold, "%");

        if(m_localInstance.m_tiers.getTier(m_strTargetTier).getUsedRateInPercent() > m_lThreshold)
        {
            eventParams.put(TIER_NAME, m_strTargetTier);
            return eventParams;
        }

        return null;
    }
}