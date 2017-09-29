package umn.dcsg.wieralocalserver.events;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.Condition;
import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: ajay
 * Date: 15/05/13
 * Time: 10:49 PM
 * To change this template use File | Settings | File Templates.
 */
//Not used for now. 8/1/2017
public class EventDispatch extends Thread {
	Lock lock = null;
	Condition condition = null;
	long m_lTimeout;
	// Ideally we would like to have a list of events, the rationale is that we
	// use these EventDispatch
	// threads primarily to run a bunch of thresholds that can be affected by an
	// action or by the expiration
	// of a timer
	ArrayList<UUID> eventList = null;
	EventRegistry eventRegistry = null;

	public EventDispatch(ArrayList<UUID> events, Lock lock, Condition condition, EventRegistry registry, long lTimeout) {
		this.eventList = events;
		this.lock = lock;
		this.condition = condition;
		this.eventRegistry = registry;
		m_lTimeout = lTimeout;
	}

	public void run() {
		while (true) {
			try {
				lock.lock();
				try {
					condition.await(m_lTimeout, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				for (UUID eventID : eventList) {
					Map<String, Object> eventParams = new HashMap<>();
					eventRegistry.evaluateEvent(eventID, eventParams, null );
				}
			} finally {
				lock.unlock();
			}
		}
	}
}