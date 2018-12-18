package com.johnlpage.pocdriver;


import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;


public class POCTestResults {

    /**
     * The time this LoadRunner started
     */
    private Date startTime;
    private Date lastIntervalTime;
    long initialCount;

    public static String[] opTypes = {"inserts", "keyqueries", "updates", "rangequeries"};
    private ConcurrentHashMap<String, POCopStats> opStats;


    POCTestResults(POCTestOptions testOptions) {
        startTime = new Date();
        lastIntervalTime = new Date();
        opStats = new ConcurrentHashMap<String, POCopStats>();

        for (String s : opTypes) {
            POCopStats stats = new POCopStats();
            stats.slowOps = new AtomicLong[ testOptions.slowThresholds.length];            
            for(int i =0; i< testOptions.slowThresholds.length; i++){
                stats.slowOps[i] = new AtomicLong();
            }
            opStats.put(s, stats);
        }
    }

    //This returns inserts per second since we last called it
    //Rather than us keeping an overall figure

    HashMap<String, Long> GetOpsPerSecondLastInterval() {

        HashMap<String, Long> rval = new HashMap<String, Long>();

        Date now = new Date();
        Long milliSecondsSinceLastCheck = now.getTime() - lastIntervalTime.getTime();

        for (String s : opTypes) {
            Long opsNow = GetOpsDone(s);
            Long opsPrev = GetPrevOpsDone(s);
            Long opsPerInterval = ((opsNow - opsPrev) * 1000) / milliSecondsSinceLastCheck;
            rval.put(s, opsPerInterval);
            SetPrevOpsDone(s, opsNow);
        }

        lastIntervalTime = now;

        return rval;
    }

    public Long GetSecondsElapsed() {
        Date now = new Date();
        return (now.getTime() - startTime.getTime()) / 1000;
    }


    private Long GetPrevOpsDone(String opType) {
        POCopStats os = opStats.get(opType);
        return os.intervalCount.get();
    }

    private void SetPrevOpsDone(String opType, Long numOps) {
        POCopStats os = opStats.get(opType);
        os.intervalCount.set(numOps);
    }

    public Long GetOpsDone(String opType) {
        POCopStats os = opStats.get(opType);
        return os.totalOpsDone.get();
    }

    
    public Long GetSlowOps(String opType, int thresholdIndex) {
        POCopStats os = opStats.get(opType);
        return os.slowOps[thresholdIndex].get();
    }

    public void RecordSlowOp(String opType, int number, int thresholdIndex) {
        POCopStats os = opStats.get(opType);        
        os.slowOps[thresholdIndex].addAndGet(number);
        
    }

    public void RecordOpsDone(String opType, int howmany) {
        POCopStats os = opStats.get(opType);
        if (os == null) {
            System.out.println("Cannot fetch opstats for " + opType);
        } else {
            os.totalOpsDone.addAndGet(howmany);
        }
    }
}
