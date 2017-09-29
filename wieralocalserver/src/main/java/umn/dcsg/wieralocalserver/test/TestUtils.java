package umn.dcsg.wieralocalserver.test;

import org.apache.commons.math3.distribution.NormalDistribution;

import java.util.*;

/**
 * Created by Kwangsung on 7/18/2017.
 */
public class TestUtils {
    public static void simluatedDelay(long lDelayedPeriod) {
        if (lDelayedPeriod > 0) {
            //10% variation
            long lVariation = (long) (lDelayedPeriod * 0.1);

            if (lVariation == 0) {
                lVariation = 1;
            }

            Random rand = new Random();
            int n = rand.nextInt((int) (lVariation + lVariation)) - (int) lVariation;

            if (lDelayedPeriod > 0) {
                try {
                    Thread.sleep(lDelayedPeriod + n);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static List<Double> initProbability(long lPeriod, double mean, double stdev) {
        double x1;
        double x2;

        NormalDistribution nd = new NormalDistribution(mean, stdev);
        List<Double> pro = new LinkedList<Double>();
        List<Double> probability = new LinkedList<Double>();

        for (int i = 0; i < lPeriod; i++) {
            x1 = nd.cumulativeProbability(i);
            x2 = nd.cumulativeProbability(i + 1);
            x2 = x2 - x1;
            pro.add(x2);
        }

        int nMedIndex = pro.size() / 2;
        double med = pro.get(nMedIndex);

        for (int i = 0; i < lPeriod; i++) {
            x1 = pro.get(i) / med;
            probability.add(x1);
        }

        return probability;
    }

    //This is for simulating delay
    public static class SimulatedDelay {
        private static long m_lSimulatedDelayPeriod = 0;
        private Timer m_timer; //For delay

        public long getSimulatedDelayPeriod() {
            return m_lSimulatedDelayPeriod;
        }

        //example
        //lPeriod 500ms
        //lHowLong 30000ms -> 30 seconds
        public void setSimulatedDelayPeriod(long lPeriod, long lHowLong) {
            m_lSimulatedDelayPeriod = lPeriod;

            //Set timer with lHowLong
            TimerTask task = new TimerTask() {
                @Override
                public void run() {
                    m_lSimulatedDelayPeriod = 0;
                    m_timer.cancel();

                    System.out.println("\nSimulated Delay is now deactivated\n");
                }
            };

            m_timer = new Timer();
            m_timer.scheduleAtFixedRate(task, lHowLong, lHowLong);    //Millisecond
            System.out.format("\nSimulated Delay is activated %d ms for %d ms\n", lPeriod, lHowLong);
        }
    }
}