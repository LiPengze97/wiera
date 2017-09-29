package umn.dcsg.wieralocalserver.events;

import umn.dcsg.wieralocalserver.LocalInstance;
import umn.dcsg.wieralocalserver.MetaObjectInfo;
import umn.dcsg.wieralocalserver.info.Latency;
import umn.dcsg.wieralocalserver.info.OperationLatency;
import umn.dcsg.wieralocalserver.responses.Response;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static umn.dcsg.wieralocalserver.Constants.*;


/**
 * Created with IntelliJ IDEA.
 * User: ajay
 * Date: 28/03/13
 * Time: 11:05 PM
 * To change this template use File | Settings | File Templates.
 */

// We support three kinds of events - Timer, Threshold, and Action Events.
// Events are named entities since they can be added and removed

public abstract class Event implements Serializable {
    // TODO: Add an event registry, this should not just be a reference but the actual
    // event, can this be done in JAVA?? Maybe every where else we store the UUID of the
    // event and then do a getObject from the registry

    private static final long serialVersionUID = -7746564383811239079L;
    UUID eventID = null;
    LocalInstance m_localInstance;

    protected List<Response> m_lstResponse;
    protected Map<String, Object> m_eventParams;
    private ReentrantReadWriteLock m_eventLock;
    private boolean m_bActive = false;

    Event(LocalInstance instance) {
        m_localInstance = instance;
        m_lstResponse = new ArrayList<>();
        m_eventLock = new ReentrantReadWriteLock();
        m_bActive = true;
    }

    Event(LocalInstance instance, Response response) {
        this(instance);
        addResponse(response);
    }

    Event(LocalInstance instance, List responseList) {
        this(instance);

        for (int i = 0; i < responseList.size(); i++) {
            addResponse((Response) responseList.get(i));
        }
    }

    Event(LocalInstance instance, Map<String, Object> eventParams, List responseList) {
        this(instance, responseList);
        m_eventParams = eventParams;
    }

    Object eventOccured(Map<String, Object> eventParams, String strEventTrigger) {
        Object result = null;
        try {
            m_eventLock.readLock().lock();
            if (m_bActive) {
                result = evaluateEvent(eventParams, strEventTrigger);
            }
        } finally {
            m_eventLock.readLock().unlock();
        }
        return result;
    }

    public UUID getEventID() {
        return eventID;
    }

    public void setEventID(UUID EventID) {
        eventID = EventID;
    }

    public void disable() {
        try {
            m_eventLock.writeLock().lock();
            m_bActive = false;
        } finally {
            m_eventLock.writeLock().unlock();
        }
    }

    public boolean addResponse(Response response) {
        m_eventLock.writeLock().lock();
        boolean ret = m_lstResponse.add(response);
        m_eventLock.writeLock().unlock();

        return ret;
    }

    public boolean removeResponse(Response response) {
        m_eventLock.writeLock().lock();
        boolean ret = m_lstResponse.remove(response);
        m_eventLock.writeLock().unlock();

        return ret;
    }

    public void removeAllResponse() {
        m_eventLock.writeLock().lock();
        m_lstResponse.clear();
        m_eventLock.writeLock().unlock();
    }

    protected Map<String, Object> respondsToEvent(Map<String, Object> responseParams) {
        try {
            m_eventLock.readLock().lock();

            //Set latency timer
            OperationLatency operationLatency = null;

            //Timing for only Get and Put operations
            if(responseParams.containsKey(OPERATION_LATENCY) == true) {
                operationLatency = (OperationLatency) responseParams.get(OPERATION_LATENCY);
            }

            Latency latency = null;
            boolean bRet;

            for (Response response : m_lstResponse) {
                //Check reponse can support the event
                //Each reponse will have a change to make params here
                response.doPrepareResponseParams(responseParams);

                if (response.doCheckResponseParams(responseParams.keySet()) == true) {
                    //Set timer for each response with its name for action event
                    if(operationLatency != null) {
                        latency = operationLatency.addTimer(response.getClass().getSimpleName());
                        latency.start();
                    }

                    bRet = response.respond(responseParams);

                    //Only for Get and Put operations
                    if(latency != null) {
                        latency.stop();
                    }

                    if (bRet == false) {
                        //Response failed
                        System.out.println(responseParams.get(REASON));
                        break;
                    }
                } else {
                    String strReason = String.format("Response: %s cannot response for the event: %s because of lack of params\n", response.getClass().getSimpleName(), getClass().getSimpleName());
                    responseParams.put(RESULT, false);
                    responseParams.put(REASON, strReason);
                    System.out.printf(strReason);
                }
            }

            //Reaching here mean there was no false return
            //Save metadata - if there is object meta updates, store them
            if(responseParams.containsKey(OBJS_LIST) == true) {
                Map<String, MetaObjectInfo> objsList = (Map) responseParams.get(OBJS_LIST);

                for (MetaObjectInfo obj : objsList.values()) {
                    m_localInstance.commitMeta(obj);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            m_eventLock.readLock().unlock();
        }

        return responseParams;
    }

    protected Map<String, Object> evaluateEvent(Map<String, Object> eventParams, String strEventTrigger) {
        //doCheckEventCondition return inputs for response. (input may changed) or null
        Map<String, Object> responseParams = doCheckEventCondition(eventParams, strEventTrigger);

        if (responseParams != null) {
            return respondsToEvent(responseParams);
        } else {
            return null;
        }
    }

    //To pass the args to response as is, return false.
    //If the event needs to be ignored, return null
    protected abstract Map<String, Object> doCheckEventCondition(Map<String, Object> eventParams, String strEventTrigger);

    public static Event createEvent(LocalInstance localInstance, String strEventType, Map<String, Object> eventParams, List lstResponse)
    {
        Class<?> foundClass;
        Constructor<?> eventConstructor;

        try {
            foundClass = Class.forName(EVENT_PACKAGE_PATH + strEventType + EVENT_FOR_CLASS);
            eventConstructor = foundClass.getConstructor(LocalInstance.class, Map.class, List.class);
            return (Event) eventConstructor.newInstance(localInstance, eventParams, lstResponse);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }

        return null;
    }
}