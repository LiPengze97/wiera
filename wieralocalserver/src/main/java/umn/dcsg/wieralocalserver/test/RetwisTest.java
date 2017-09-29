package umn.dcsg.wieralocalserver.test;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * Created by Kwangsung on 7/28/2017.
 */
public class RetwisTest {
    public static void retwis_test_init(JSONArray instanceList) {
        //Register first.
        //Find IP and Port based region
        HttpClient httpclient = HttpClients.createDefault();

        for (int k = 0; k < instanceList.length(); k++) {
            JSONArray region = (JSONArray) instanceList.get(k);

            if ("aws-us-east".equals(region.get(0))) {
                for (int i = 0; i < 1000; i++) {
                    int nErrorCode = signup(httpclient, (String) region.get(1), String.format("%d", i), String.format("%d", i));
                    if (nErrorCode != HttpStatus.SC_OK) {
                        System.out.printf("Signup Failed. error code: %d\n", nErrorCode);
                    }
                }

                break;
            }
        }
    }

    public static void retwis_test(JSONArray instanceList) {
        String strHostname;
        long lPeriod = 900; //seconds
        long lThreadCnt = 8;//
        long lClientCnt = 125; //Total 1000 people

        //For now hardcoding.
        List<Double> probability = TestUtils.initProbability(lPeriod, 450, 100);

        //create thread lClientCnt for each regions
        int nCnt = instanceList.length();

        String strIP;
        long lRegionTime = 0;
        long lEachRegion = lPeriod / nCnt;
        int nPort;
        List<Thread> threadList = new LinkedList<Thread>();

        SimulatedRetwisClient client;
        Thread clientThread;
        String[] strOrder = {"aws-us-west", "aws-us-west-2", "aws-ca-central", "aws-us-east", "aws-us-east-2", "aws-eu-west", "aws-asia-se", "aws-asia-ne"};
        //String[] strOrder = {"aws-us-east"};

        //Find IP and Port based region
        for (int i = 0; i < strOrder.length; i++) {
            //+3 means Asia-east has more request at first.
            lRegionTime = lEachRegion * ((i + 5) % strOrder.length);
            strHostname = strOrder[i];

            //Find IP and Port based region
            //This is not actual loop. (what a poor;;;)
            for (int k = 0; k < nCnt; k++) {
                JSONArray region = (JSONArray) instanceList.get(k);

                if (strHostname.equals(region.get(0))) {
                    strIP = (String) region.get(1);
                    nPort = (int) region.get(2);

                    for (int j = 0; j < lThreadCnt; j++) {
                        long lID = lClientCnt * i + j;
                        client = new SimulatedRetwisClient((String) region.get(0), (String) region.get(1), (int) region.get(2), String.format("%d", lID), System.currentTimeMillis(), lRegionTime, lPeriod, probability, lClientCnt * strOrder.length);
                        clientThread = new Thread(client);
                        clientThread.start();
                        threadList.add(clientThread);
                    }

                    break;
                }
            }
        }

        nCnt = threadList.size();

        //Wait all clients are done
        for (int i = 0; i < nCnt; i++) {
            clientThread = threadList.get(i);

            try {
                clientThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    //Will be called by thread for each
    public static void run_retwis_clients(SimulatedRetwisClient client) {
        Random rand = new Random();
        RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(60 * 1000).build();
        HttpClient httpClient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
        CookieStore cookieStore = new BasicCookieStore();
        HttpContext httpContext = new BasicHttpContext();
        httpContext.setAttribute(HttpClientContext.COOKIE_STORE, cookieStore);
        int nCntForOperation = 0; //each thread can login at most 8

        while (true) {
            long lElapse = System.currentTimeMillis() - client.m_lStartTime;

            if (lElapse >= client.m_lPeriod * 1000 && nCntForOperation >= 16) {
                System.out.println("Test is done.");
                break;
            }

            //Do the test for amount of period
            if (nCntForOperation >= 16) {
                System.out.println("I'm sending all reqeuts.");
                break;
            }

            long lCurTime = ((lElapse / 1000 + client.m_lRegionTime) % client.m_lPeriod);
            double dProbability = client.m_probability.get((int) lCurTime);

            //Can this user execute something?
            int nRand = rand.nextInt(100);

            if (nRand < (dProbability * 100))
            //if (nRand < 100)
            {
                nCntForOperation++;
                //125 means number of users.
                int nRandomSelctedID = (int) client.m_lRegionTime + rand.nextInt(125);

                client.m_strID = String.format("%d", nRandomSelctedID);
                //Login first with random ID
                int nErrorCode = login(httpClient, httpContext, client.m_strIP, client.m_strID, client.m_strID);

                if (nErrorCode != HttpStatus.SC_OK && nErrorCode != HttpStatus.SC_SEE_OTHER) {
                    System.out.printf("Failed to login. Skip ID: %s error code:%d\n", client.m_strID, nErrorCode);
                    continue;
                } else {
                    System.out.printf("Hostname: %s Login ID %s - %f\n", client.m_strHost, client.m_strID, dProbability);
                }

                //each login do two operations
                for (int i = 0; i < 3; i++) {
                    //What will this user do?
                    nRand = rand.nextInt(100);

                    if (nRand < 75) {
                        //System.out.printf("ID: %s readtimeline\n", client.m_strPolicyID);
                        nErrorCode = readTimeLine(httpClient, httpContext, client.m_strIP);
                        if (nErrorCode != HttpStatus.SC_OK) {
                            System.out.printf("Fail happens in ID: %s readtimeline. error code: %d\n", client.m_strID, nErrorCode);
                        }
                    } else if (nRand < 90) {
                        //post
                        //System.out.printf("ID: %s post\n", client.m_strPolicyID);
                        nErrorCode = post(httpClient, httpContext, client.m_strIP, client.m_strID);
                        if (nErrorCode != HttpStatus.SC_OK) {
                            System.out.printf("Fail happens in ID: %s post. error code: %d\n", client.m_strID, nErrorCode);
                        }
                    } else if (nRand < 96) {
                        //follow
                        int nFollow = rand.nextInt((int) client.m_lTotal);
                        //System.out.printf("ID: %s follow :%d\n", client.m_strPolicyID, nFollow);
                        nErrorCode = follow(httpClient, httpContext, client.m_strIP, String.format("%d", nFollow));
                        if (nErrorCode != HttpStatus.SC_OK) {
                            System.out.printf("Fail happens in ID: %s follow. error code: %d\n", client.m_strID, nErrorCode);
                        }
                    } else if (nRand < 100) {
                        //unfollow
                        int nUnfollow = rand.nextInt((int) client.m_lTotal);
                        //System.out.printf("ID: %s unfollow :%d\n", client.m_strPolicyID, nUnfollow);
                        nErrorCode = unfollow(httpClient, httpContext, client.m_strIP, String.format("%d", nUnfollow));
                        if (nErrorCode != HttpStatus.SC_OK) {
                            System.out.printf("Fail happens in ID: %s unfollow. error code: %d\n", client.m_strID, nErrorCode);
                        }
                    }

                    //take a rest 5000ms
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                logout(httpClient, httpContext, client.m_strIP);
            } else {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static int signup(HttpClient httpclient, String strIP, String strID, String strPassword) {
        HttpPost httppost = new HttpPost("http://" + strIP + ":8080/signup");

        // Request parameters and other properties.
        List<NameValuePair> params = new ArrayList<NameValuePair>(2);
        params.add(new BasicNameValuePair("name", strID));
        params.add(new BasicNameValuePair("password", strPassword));
        HttpResponse response = null;

        try {
            httppost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
            response = httpclient.execute(httppost);
            //Execute and get the response.
            HttpEntity entity = response.getEntity();

            if (entity != null) {
                EntityUtils.consume(entity);
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            httppost.releaseConnection();
        }

        if (response != null) {
            return response.getStatusLine().getStatusCode();
        } else {
            return HttpStatus.SC_BAD_REQUEST;
        }
    }

    private static int readTimeLine(HttpClient httpclient, HttpContext httpContext, String strIP) {
        //Read time line
        HttpGet httpget = new HttpGet("http://" + strIP + ":8080/home");
        HttpResponse response = null;

        try {
            //Execute and get the response.
            response = httpclient.execute(httpget, httpContext);
            HttpEntity entity = response.getEntity();

            if (entity != null) {
                EntityUtils.consume(entity);
            }
        } catch (Exception e) {
            //e.printStackTrace();
            //System.out.printf("Exception in readTimeLine(): %s, %s ", strIP);
        } finally {
            httpget.releaseConnection();
        }

        if (response != null) {
            return response.getStatusLine().getStatusCode();
        } else {
            return HttpStatus.SC_BAD_REQUEST;
        }
    }

    private static int post(HttpClient httpclient, HttpContext httpContext, String strIP, String strContents) {
        HttpResponse response = null;

        //Read time line
        HttpPost httppost = new HttpPost("http://" + strIP + ":8080/post");

        // Request parameters and other properties.
        List<NameValuePair> params = new ArrayList<NameValuePair>(2);
        params.add(new BasicNameValuePair("content", strContents));

        try {
            //Execute and get the response.
            httppost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
            //Execute and get the response.
            response = httpclient.execute(httppost, httpContext);
            HttpEntity entity = response.getEntity();

            if (entity != null) {
                EntityUtils.consume(entity);
            }
        } catch (Exception e) {
            //e.printStackTrace();
            //System.out.printf("Exception in post(): %s, %s ", strIP, strContents);
        } finally {
            httppost.releaseConnection();
        }

        if (response != null) {
            return response.getStatusLine().getStatusCode();
        } else {
            return HttpStatus.SC_BAD_REQUEST;
        }
    }

    private static int follow(HttpClient httpclient, HttpContext httpContext, String strIP, String strName) {
        //Read time line
        HttpPost httppost = new HttpPost("http://" + strIP + ":8080/follow/" + strName);

        // Request parameters and other properties.
        List<NameValuePair> params = new ArrayList<NameValuePair>(0);
        HttpResponse response = null;

        try {
            //Execute and get the response.
            httppost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
            //Execute and get the response.
            response = httpclient.execute(httppost, httpContext);
            HttpEntity entity = response.getEntity();

            if (entity != null) {
                EntityUtils.consume(entity);
            }
        } catch (Exception e) {
            //e.printStackTrace();
            //System.out.printf("Exception in follow(): %s, %s ", strIP, strName);
        } finally {
            httppost.releaseConnection();
        }

        if (response != null) {
            return response.getStatusLine().getStatusCode();
        } else {
            return HttpStatus.SC_BAD_REQUEST;
        }
    }

    private static int unfollow(HttpClient httpclient, HttpContext httpContext, String strIP, String strName) {
        HttpResponse response = null;

        //Read time line
        HttpPost httppost = new HttpPost("http://" + strIP + ":8080/unfollow/" + strName);

        // Request parameters and other properties.
        List<NameValuePair> params = new ArrayList<NameValuePair>(0);

        try {
            //Execute and get the response.
            httppost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
            //Execute and get the response.
            response = httpclient.execute(httppost, httpContext);
            HttpEntity entity = response.getEntity();

            if (entity != null) {
                EntityUtils.consume(entity);
            }
        } catch (Exception e) {
            //e.printStackTrace();
            //System.out.printf("Exception in unfollow(): %s, %s ", strIP, strName);
        } finally {
            httppost.releaseConnection();
        }

        if (response != null) {
            return response.getStatusLine().getStatusCode();
        } else {
            return HttpStatus.SC_BAD_REQUEST;
        }
    }

    private static int login(HttpClient httpclient, HttpContext httpContext, String strIP, String strID, String strPassword) {
        HttpPost httppost = new HttpPost("http://" + strIP + ":8080/login");
        HttpResponse response = null;

        // Request parameters and other properties.
        List<NameValuePair> params = new ArrayList<NameValuePair>(2);
        params.add(new BasicNameValuePair("name", strID));
        params.add(new BasicNameValuePair("password", strPassword));

        try {
            httppost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
            //Execute and get the response.
            response = httpclient.execute(httppost, httpContext);
            HttpEntity entity = response.getEntity();

            if (entity != null) {
                EntityUtils.consume(entity);
            }
        } catch (IOException e) {
            //e.printStackTrace();
        } finally {
            //	httppost.releaseConnection();
        }

        if (response != null) {
            return response.getStatusLine().getStatusCode();
        } else {
            return HttpStatus.SC_BAD_REQUEST;
        }
    }

    private static int logout(HttpClient httpclient, HttpContext httpContext, String strIP) {
        HttpGet httpGet = new HttpGet("http://" + strIP + ":8080/logout");
        HttpResponse response = null;

        try {
            //Execute and get the response.
            response = httpclient.execute(httpGet, httpContext);
            HttpEntity entity = response.getEntity();

            if (entity != null) {
                EntityUtils.consume(entity);
            }
        } catch (Exception e) {
            //e.printStackTrace();
            System.out.printf("Exception in logout(): %s\n", strIP);
        } finally {
            httpGet.releaseConnection();
        }

        if (response != null) {
            return response.getStatusLine().getStatusCode();
        } else {
            return HttpStatus.SC_BAD_REQUEST;
        }
    }

    public static class SimulatedRetwisClient implements Runnable {
        long m_lStartTime = 0;
        long m_lRegionTime = 0;
        long m_lPeriod = 0; //in Second
        String m_strIP;
        String m_strHost;
        String m_strID;
        int m_nPort;
        List<Double> m_probability;
        long m_lTotal = 0;

        public SimulatedRetwisClient(String strHost, String strIP, int nPort, String strID, long lStartTime, long lRegionTime, long lPeriod, List<Double> probability, long lTotal) {
            m_strHost = strHost;
            m_strIP = strIP;
            m_strID = strID;
            m_nPort = nPort;
            m_lStartTime = lStartTime;
            m_lRegionTime = lRegionTime;
            m_lPeriod = lPeriod;
            m_probability = probability;
            m_lTotal = lTotal;
        }

        @Override
        public void run() {
            RetwisTest.run_retwis_clients(this);
        }
    }
}
