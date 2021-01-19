package com.johnlpage.pocdriver;

import com.mongodb.BasicDBObject;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;

import org.bson.Document;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.mongodb.client.model.Filters.eq;

public class LoadRunner {

    private MongoClient mongoClient;
    Logger logger;

    private void PrepareSystem(POCTestOptions testOpts, POCTestResults results) {
        MongoDatabase db;
        MongoCollection<Document> coll;
        // Create indexes and suchlike
        db = mongoClient.getDatabase(testOpts.databaseName);
        coll = db.getCollection(testOpts.collectionName);
        if (testOpts.emptyFirst) {
            coll.drop();
        }

        TestRecord testRecord = new TestRecord(testOpts);
        List<String> fields = testRecord.listFields();
        for (int x = 0; x < testOpts.secondaryidx; x++) {
            coll.createIndex(new Document(fields.get(x), 1));
        }
        if (testOpts.fulltext) {
            IndexOptions options = new IndexOptions();
            options.background(true);
            BasicDBObject weights = new BasicDBObject();
            weights.put("lorem", 15);
            weights.put("_fulltext.text", 5);
            options.weights(weights);
            Document index = new Document();
            index.put("$**", "text");
            coll.createIndex(index, options);
        }

        results.initialCount = coll.estimatedDocumentCount();
        // Now have a look and see if we are sharded
        // And how many shards and make sure that the collection is sharded
        if (!testOpts.singleserver) {
            ConfigureSharding(testOpts);
        }

    }

    private void ConfigureSharding(POCTestOptions testOpts) {
        MongoDatabase admindb = mongoClient.getDatabase("admin");
        Document cr = admindb.runCommand(new Document("serverStatus", 1));
        if (cr.getDouble("ok") == 0) {
            logger.warn(cr.toJson());
            return;
        }

        String procname = (String) cr.get("process");
        if (procname != null && procname.contains("mongos")) {
            testOpts.sharded = true;
            // Turn the auto balancer off - good code rarely needs it running constantly
            MongoDatabase configdb = mongoClient.getDatabase("config");
            MongoCollection<Document> settings = configdb.getCollection("settings");
            UpdateResult rval = settings.updateOne(eq("_id", "balancer"),
                    new Document("$set", new Document("stopped", true)), new UpdateOptions().upsert(true));
            logger.info(rval.toString());
            logger.info("Balancer disabled");
            try {
                logger.info("Enabling Sharding on Database");
                admindb.runCommand(new Document("enableSharding", testOpts.databaseName));
            } catch (Exception e) {
                if (!e.getMessage().contains("already enabled"))
                    logger.warn(e.getMessage());
            }

            try {
                logger.info("Sharding Collection");
                admindb.runCommand(
                        new Document("shardCollection", testOpts.databaseName + "." + testOpts.collectionName)
                                .append("key", new Document("_id", 1)));
            } catch (Exception e) {
                if (!e.getMessage().contains("already"))
                    logger.warn(e.getMessage());
            }

            // See how many shards we have in the system - and get a list of their names
            logger.info("Counting Shards");
            MongoCollection<Document> shards = configdb.getCollection("shards");
            MongoCursor<Document> shardc = shards.find().iterator();
            testOpts.numShards = 0;
            while (shardc.hasNext()) {
                logger.info("Found a shard");
                shardc.next();
                testOpts.numShards++;

            }

            logger.info("System has " + testOpts.numShards + " shards");
        }
    }

    public void RunLoad(POCTestOptions testOpts, POCTestResults testResults) {

        PrepareSystem(testOpts, testResults);
        // Report on progress by looking at testResults
        POCTestReporter reporter = new POCTestReporter(testResults, mongoClient, testOpts);

        // Using a thread pool we keep filled
        ExecutorService testexec = Executors.newFixedThreadPool(testOpts.numThreads);

        // Allow for multiple clients to run -
        // Check for testOpts.threadIdStart - this should be an integer to start
        // the 'workerID' for each set of threads.
        int threadIdStart = testOpts.threadIdStart;
        logger.info("threadIdStart=" + threadIdStart);
        ArrayList<MongoWorker> workforce = new ArrayList<MongoWorker>();
        logger.info("Launching worker threads");
        for (int i = threadIdStart; i < (testOpts.numThreads + threadIdStart); i++) {
            logger.info("Creating worker " + i);
            workforce.add(new MongoWorker(mongoClient, testOpts, testResults, i));
        }
        logger.info("Worker threads all started");

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(reporter, 0, testOpts.reportTime, TimeUnit.SECONDS);

        for (MongoWorker w : workforce) {
            testexec.execute(w);
        }

        testexec.shutdown();

        try {
            testexec.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            logger.info("All Threads Complete");
            executor.shutdown();
        } catch (InterruptedException e) {
            logger.error(e.getMessage());

        }

        // do final report
        reporter.finalReport();
    }

    LoadRunner(POCTestOptions testOpts) {
        logger = LoggerFactory.getLogger(LoadRunner.class);

        try {
            // For not authentication via connection string passing of user/pass only
            mongoClient = MongoClients.create(testOpts.connectionDetails);
        } catch (Exception ex) {
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            logger.error(errors.toString());
        }
    }
}
