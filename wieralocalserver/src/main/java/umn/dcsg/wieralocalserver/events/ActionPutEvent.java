package umn.dcsg.wieralocalserver.events;

import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.responses.Response;

import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA. User: ajay Date: 01/04/13 Time: 12:47 AM To
 * change this template use File | Settings | File Templates.
 * 
 * This program allows the user to implement an on-call Action event.  This is the simplest of the three
 * LocalInstance code types.
 */

public class ActionPutEvent extends Event {
	private static final long serialVersionUID = 1L;

	public ActionPutEvent(LocalInstance instance) {
		super(instance);
	}

	public ActionPutEvent(LocalInstance instance, Response response) {
		super(instance, response);
	}

	public ActionPutEvent(LocalInstance instance, Map <String, Object> params, List lstResponse) {
		super(instance, params, lstResponse);
	}

	@Override
	protected Map<String, Object> doCheckEventCondition(Map<String, Object> eventParams, String strEventTrigger) {
		return eventParams;
	}
}