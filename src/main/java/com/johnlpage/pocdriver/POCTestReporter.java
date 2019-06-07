package com.johnlpage.pocdriver;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;


public class POCTestReporter implements Runnable {
    private POCTestResults testResults;
    private MongoClient mongoClient;
    private POCTestOptions testOpts;

    private static final DateFormat DF_FULL = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final DateFormat DF_TIME = new SimpleDateFormat("HH:mm:ss");

    POCTestReporter(POCTestResults r, MongoClient mc, POCTestOptions t) {
        mongoClient = mc;
        testResults = r;
        testOpts = t;

    }


    private void logData() {
        PrintWriter outfile = null;

        if (testOpts.logfile != null) {

            try {
                outfile = new PrintWriter(new BufferedWriter(new FileWriter(testOpts.logfile, true)));
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }

        Long insertsDone = testResults.GetOpsDone("inserts");
        if (testResults.GetSecondsElapsed() < testOpts.reportTime)
            return;
        System.out.println("------------------------");
        if (testOpts.sharded && !testOpts.singleserver) {
            MongoDatabase configdb = mongoClient.getDatabase("config");
            MongoCollection<Document> shards = configdb.getCollection("shards");
            testOpts.numShards = (int) shards.count();
        }
        Date todaysdate = new Date();
        System.out.format("After %d seconds (%s), %,d new documents inserted - collection has %,d in total \n",
                testResults.GetSecondsElapsed(), DF_TIME.format(todaysdate), insertsDone, testResults.initialCount + insertsDone);

        if (outfile != null) {
            outfile.format("%d,%d", testResults.GetSecondsElapsed(), insertsDone);
        }

        HashMap<String, Long> results = testResults
                .GetOpsPerSecondLastInterval();
        String[] opTypes = POCTestResults.opTypes;

        for (String o : opTypes) {
            System.out.format("%,d %s per second since last report ",
                    results.get(o), o);

            if (outfile != null) {
                String str = DF_FULL.format(todaysdate);
                String mydate = str.replaceAll("\\s+", "T");
                outfile.format("%s,%d,%d", mydate, testResults.GetSecondsElapsed(), insertsDone);
            }

            Long opsDone = testResults.GetOpsDone(o);
            if (opsDone > 0) {
                Double fastops = 100 - (testResults.GetSlowOps(o) * 100.0)
                        / opsDone;
                System.out.format("%.2f %% in under %d milliseconds", fastops,
                        testOpts.slowThreshold);
                if (outfile != null) {
                    outfile.format(",%.2f", fastops);
                }
            } else {
                System.out.format("%.2f %% in under %d milliseconds", (float) 100, testOpts.slowThreshold);
                if (outfile != null) {
                    outfile.format(",%d", 100);
                }
            }
            System.out.println();

        }
        if (outfile != null) {
            outfile.println();
            outfile.close();
        }
        System.out.println();
    }

    public void run() {

        logData();

    }

    /**
     * Output a final summary
     */
    public void finalReport() {

        Long insertsDone = testResults.GetOpsDone("inserts");

        Long secondsElapsed = testResults.GetSecondsElapsed();

        System.out.println("------------------------");
        System.out.println("Insert operations: "+testResults.getInsertDocs()+" - Update Operations: "+testResults.getUpdateDocs() +" - Read operations: "+testResults.getReadDocs());
        System.out.format("After %d seconds, %d new documents inserted - collection has %d in total \n",
                secondsElapsed, insertsDone, testResults.initialCount + insertsDone);

        String[] opTypes = POCTestResults.opTypes;

        for (String o : opTypes) {

            Long opsDone = testResults.GetOpsDone(o);

            System.out.format("%d %s per second on average", (int)(1f * opsDone / secondsElapsed), o);
            System.out.println();

        }
        System.out.println();

    }
}
