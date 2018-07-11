package umn.dcsg.wieralocalserver.responses;


import umn.dcsg.wieralocalserver.*;
import umn.dcsg.wieralocalserver.info.Latency;
import umn.dcsg.wieralocalserver.info.OperationLatency;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import static umn.dcsg.wieralocalserver.Constants.*;

/**
 * Created with IntelliJ IDEA.
 * User: ajay
 * Date: 28/03/13
 * Time: 11:07 PM
 * To change this template use File | Settings | File Templates.
 */

// A response only needs to know how to respond
public abstract class Response {
    protected LocalInstance m_localInstance = null;
    protected Map<String, Object> m_initParams;
    protected String m_strEventName;    //Which event invoke this response

    //Need to be set by developers (you) when they (you) want to create a new response
    protected List<String> m_lstRequiredParams;

    /**
     * Constructor with response param
     * Response can be generated with policy or other response dynamically
     *
     * @param localInstance LocalInstance localInstance
     * @param params        Reponse class to handle the request (or event)
     * @return void
     * @see Response
     */
    public Response(LocalInstance localInstance, String strEventName, Map<String, Object> params) {
        m_localInstance = localInstance;
        m_strEventName = strEventName;
        m_initParams = params;
        m_lstRequiredParams = new LinkedList<>();
        InitRequiredParams();
    }

    protected abstract void InitRequiredParams();


    // What all does a response need to execute? Many things
    // hence keep the signature very generic -> for named parameters
    public abstract void doPrepareResponseParams(Map<String, Object> responseParams);

    //This will check static paramters
    public boolean doCheckResponse(Map<String, Object> responseParams) {
        Set<String> keySet = responseParams.keySet();
        String strMissing = new String();
        boolean bCheck = true;

        for (String strKey : m_lstRequiredParams) {
            if (keySet.contains(strKey) == false) {
                strMissing += strKey;
                strMissing += " ";
                bCheck = false;
            }
        }

        if(bCheck == false) {
            String strReason = String.format("Response: %s cannot response for the event: %s - missing params: %s\n", getClass().getSimpleName(), getClass().getSimpleName(), strMissing);
            responseParams.put(REASON, strReason);
            return false;
        }

        return doCheckResponseConditions(responseParams);
    }

    //This function will check the response is met the condition to response
    public abstract boolean doCheckResponseConditions(Map<String, Object> responseParams);

    //The responseparams will contain RESULT (boolean) and
    //VALUE if response success
    //REASON if response failed

    public abstract boolean respond(Map<String, Object> responseParams);

    public static Response CreateResponse(LocalInstance localInstance, Class responseClass, Map<String, Object> responseParams) {
        Constructor<?> responseConstructor;
        Response response;
        try {
            responseConstructor = responseClass.getConstructor(LocalInstance.class, String.class, Map.class);
            response = (Response) responseConstructor.newInstance(localInstance, responseClass.getName(), responseParams);
            return response;
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
            responseParams.put(REASON, e.getMessage());
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            responseParams.put(REASON, e.getMessage());
        } catch (InstantiationException e) {
            e.printStackTrace();
            responseParams.put(REASON, e.getMessage());
        } catch (InvocationTargetException e) {
            e.printStackTrace();
            responseParams.put(REASON, e.getMessage());
        }

        return null;
    }

    public static void addMetaToUpdate(MetaObjectInfo meta, Map<String, Object> responseParams) {
        Map<String, MetaObjectInfo> metaList;
        if (responseParams.containsKey(META_NOT_COMMITED_LIST) == false) {
            metaList = new HashMap<>();
            responseParams.put(META_NOT_COMMITED_LIST, metaList);
        } else {
            metaList = (Map)responseParams.get(META_NOT_COMMITED_LIST);
        }

        if(metaList.containsKey(meta.getKey()) == false) {
            metaList.put(meta.getKey(), meta);
        }
    }

    public static boolean checkMetaInResponse(String strKey, Map<String, Object> responseParams) {
        Map<String, MetaObjectInfo> metaList;
        if (responseParams.containsKey(META_NOT_COMMITED_LIST) == false) {
            return false;
        } else {
            metaList = (Map)responseParams.get(META_NOT_COMMITED_LIST);
            return metaList.containsKey(strKey);
        }
    }

    /**
     * Create and do response at run-time.
     *
     * @param localInstance  LocalInstance localInstance
     * @param responseClass  Reponse class to handle the request (or event)
     * @param responseParams Dynamic parameters for responding. This should include the result (and value) and reason if failed.
     * @return void
     * @see Response
     */
    public static boolean respondAtRuntimeWithClass(LocalInstance localInstance, Class responseClass, Map<String, Object> responseParams) {
        Response response = CreateResponse(localInstance, responseClass, responseParams);

        if (response != null) {
            return respondAtRuntimeWithInstance(localInstance, response, responseParams);
        } else {
            responseParams.put(RESULT, false);
            return false;
        }
    }

    //This is the final function for responding
    public static boolean respondAtRuntimeWithInstance(LocalInstance localInstance, Response response, Map<String, Object> responseParams) {
        boolean bRet;
        Latency latency;

        //Set timer
        OperationLatency operationLatency = (OperationLatency) responseParams.get(OPERATION_LATENCY);
        latency = operationLatency.addTimer(response.getClass().getSimpleName());
        latency.start();
        bRet = response.respond(responseParams);
        latency.stop();

        responseParams.put(RESULT, bRet);
        return bRet;
    }

    public static boolean respondSequentiallyWithClass(LocalInstance localInstance, List<Class> responseClassList, Map<String, Object> responseParams) {
        Constructor<?> responseConstructor;
        Response response;
        List responseList = new LinkedList<Response>();

        for (Class responseClass : responseClassList) {
            try {
                responseConstructor = responseClass.getConstructor(LocalInstance.class, String.class, Map.class);
                response = (Response) responseConstructor.newInstance(localInstance, responseClass.getName(), responseParams);
                responseList.add(response);
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
        }

        return respondSequentiallyWithInstance(localInstance, responseList, responseParams);
    }

    //final sequentially function to be called.
    public static boolean respondSequentiallyWithInstance(LocalInstance localInstance, List<Response> responseList, Map<String, Object> responseParams) {
        for (Response response : responseList) {
            response.doPrepareResponseParams(responseParams);

            if (response.doCheckResponse(responseParams) == false ||
                respondAtRuntimeWithInstance(localInstance, response, responseParams) == false) {
                //If response is not ready do be executed, or fails
                return false;
            }
        }

        return true;
    }

    public static Response createResponse(LocalInstance localInstance, String strResponseType, String strParentEventName, Map<String, Object> responseParam) {
        Class<?> foundClass;
        Constructor<?> responseConstructor;

        try {
            foundClass = Class.forName(RESPONSE_PACKAGE_PATH + strResponseType + RESPONSE_FOR_CLASS);

            if(responseParam != null) {
                responseConstructor = foundClass.getConstructor(LocalInstance.class, String.class, Map.class);
                return (Response) responseConstructor.newInstance(localInstance, strParentEventName, responseParam);
            } else {
                responseConstructor = foundClass.getConstructor(LocalInstance.class, String.class);
                return (Response) responseConstructor.newInstance(localInstance, strParentEventName);
            }

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