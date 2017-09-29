package umn.dcsg.wieralocalserver.events;

import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.responses.Response;
import umn.dcsg.wieralocalserver.utils.Utils;

import java.util.*;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created by Kwangsung on 7/20/2017.
 */
public abstract class MonitoringEvent extends Event {
    private boolean m_bTimerInitialized = false;
    private Thread m_timerThread = null;
    private Boolean lock = false;

    public MonitoringEvent(LocalInstance instance, Response response) {
        super(instance, response);
    }

    public MonitoringEvent(LocalInstance instance, Map<String, Object> params, List lstResponse) {
        super(instance, params, lstResponse);
    }

    synchronized public void initTimer() {
        if (m_bTimerInitialized == true) {
            return;
        }

        //For getting input for LocalInstance Server
        m_timerThread = (new Thread(new Runnable() {
            @Override
            public void run() {
                long lPeriod = Utils.convertToLong(m_eventParams.get(PERIOD));

                while (true) {
                    try {
                        Thread.sleep(lPeriod);
                        evaluateEvent(null, TIMER_EVENT);
                    } catch (InterruptedException e) {
                        //Will happen when get the signal
                    }
                }
            }
        }));

        m_timerThread.start();

        //timer init only once.
        m_bTimerInitialized = true;
    }

    abstract Map<String, Object> doCheckMonitoringEventCondition(Map<String, Object> eventParams, String strTriggerName);

    @Override
    protected Map<String, Object> doCheckEventCondition(Map<String, Object> eventParams, String strTriggerName) {
        //Each subclass need to check the condition whether event should be raised.
        //Interrupting only when this monitoring event happen by put and get action
        synchronized (lock) {
            if (m_bTimerInitialized == true && eventParams != null) {
                //Reset Timer
                m_timerThread.interrupt();
            }

            return doCheckMonitoringEventCondition(eventParams, strTriggerName);
        }
    }

    protected static boolean checkCondition(String strType, double dbValue, double dbThreshold) {
        if (strType.equals(LT) == true) {
            return dbValue < dbThreshold;
        } else if (strType.equals(LTOE) == true) {
            return dbValue <= dbThreshold;
        } else if (strType.equals(EQUAL) == true) {
            return dbValue == dbThreshold;
        } else if (strType.equals(GT) == true) {
            return dbValue > dbThreshold;
        } else if (strType.equals(GTOE) == true) {
            return dbValue >= dbThreshold;
        } else {
            return false;
        }
    }
}