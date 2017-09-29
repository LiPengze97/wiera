package umn.dcsg.wieralocalserver.events;

import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.utils.Utils;

import java.util.*;

import static umn.dcsg.wieralocalserver.Constants.TIMER_EVENT;
import static umn.dcsg.wieralocalserver.Constants.TIMER_PERIOD;

/**
 * Created by ajay on 7/13/14.
 * 
 * Allows us to create Timer Events in LocalInstance. A timer event is when
 * objects are moved/stored at regular time-intervals. 
 */

public class TimerEvent extends Event {
    private static final long serialVersionUID = -1656798057303779148L;
    Timer t = null;

    public TimerEvent(LocalInstance instance, Map <String, Object> params, List lstResponse) {
        super(instance, params, lstResponse);
        long lTimer = Utils.convertToLong(params.get(TIMER_PERIOD));
        startTimer(lTimer);
    }

    private void startTimer(long lPeriod) {
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                //OperationLatency operationLatency = m_localInstance.m_localInfo.addOperationLatency(strKey, Locale.getLocaleID(LocalServer.getHostName(), ""), ACTION_PUT_EVENT, Constants.PUT_LATENCY);
                evaluateEvent(new HashMap<String, Object>(), TIMER_EVENT);
            }
        };

        t = new Timer();
        t.scheduleAtFixedRate(task, 0, lPeriod);
    }

    @Override
    protected Map<String, Object> doCheckEventCondition(Map<String, Object> eventParams, String strEventTrigger) {
        return eventParams;
    }
}