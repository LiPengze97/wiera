package umn.dcsg.wieralocalserver.events;

import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.MetaObjectInfo;
import umn.dcsg.wieralocalserver.Locale;
import umn.dcsg.wieralocalserver.utils.Utils;

import java.util.*;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by Kwangsung on 12/23/2015.
 */
public class MonitoringColdDataEvent extends MonitoringEvent {
    private static final long serialVersionUID = 2550078386039177453L;
    private long m_lColdDataThreshold;
    private LocalInstance m_instance;

    public MonitoringColdDataEvent(LocalInstance instance, Map<String, Object> params, List lstResponse) {
        super(instance, params, lstResponse);
        m_instance = instance;
        m_lColdDataThreshold = Utils.convertToLong(params.get(PERIOD_THRESHOLD));
    }

    @Override
    Map<String, Object> doCheckMonitoringEventCondition(Map<String, Object> eventParams, String strTriggerName) {
        /*
        Locale targetLocale = m_localInstance.getLocaleWithID((String) eventParams.get(TARGET_LOCALE));

        Map<MetaObjectInfo, Vector<Integer>> keys = m_instance.m_metadataStore.searchColdObject(m_lColdDataThreshold,
                targetLocale);

        if (keys != null && keys.isEmpty() == false) {
            System.out.println("[debug] I Found some cold data");

            //Which keys in which locale and version?
            Map<Locale, Map<MetaObjectInfo, Vector<Integer>>> keyList = new HashMap<>();
            keyList.put(targetLocale, keys);

            //Fill-out params for the next response if exists
            eventParams.put(KEY_LIST, keyList);
            return eventParams;
        }
*/
        return null;
    }
}