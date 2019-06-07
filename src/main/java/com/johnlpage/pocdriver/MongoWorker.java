package com.johnlpage.pocdriver;

import com.mongodb.client.MongoClient;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.WriteModel;
import static java.lang.Math.toIntExact;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.descending;

public class MongoWorker implements Runnable {

    private MongoClient mongoClient;
    private MongoCollection<Document> coll;
    private ArrayList<MongoCollection<Document>> colls;
    private POCTestOptions testOpts;
    private POCTestResults testResults;
    private int workerID;
    private int sequence;
    private int numShards = 0;
    private Random rng;
    private ZipfDistribution zipf;
    private boolean workflowed = false;
    private boolean zipfian = false;
    private String workflow;
    private int workflowStep = 0;
    private ArrayList<Document> keyStack;
    private int lastCollection;
    private int maxCollections;
    private int readCount = 0;
    private int insertCount = 0;
    private int updateCount = 0;
    private long readOpLimit = 0;

    private void ReviewShards() {
        // System.out.println("Reviewing chunk distribution");
        if (testOpts.sharded && !testOpts.singleserver) {
            // I'd like to pick a shard and write there - it's going to be
            // faster and
            // We can ensure we distribute our workers over out shards
            // So we will tell mongo that's where we want our records to go
            // System.out.println("Sharded and not a single server");
            MongoDatabase admindb = mongoClient.getDatabase("admin");
            Boolean split = false;

            while (!split) {

                try {
                    // System.out.println("Splitting a chunk");
                    admindb.runCommand(new Document("split", testOpts.databaseName + "." + testOpts.collectionName)
                            .append("middle",
                                    new Document("_id", new Document("w", workerID).append("i", sequence + 1))));
                    split = true;
                } catch (Exception e) {

                    if (e.getMessage().contains("is a boundary key of existing")) {
                        split = true;
                    } else {
                        if (!e.getMessage().contains("could not aquire collection lock"))
                            System.out.println(e.getMessage());
                        try {
                            Thread.sleep(1000);
                        } catch (Exception ignored) {
                        }
                    }
                }

            }
            // And move that to a shard - which shard? take my workerid and mod
            // it with the number of shards
            int shardno = workerID % testOpts.numShards;
            // Get the name of the shard

            MongoCursor<Document> shardlist = mongoClient.getDatabase("config").getCollection("shards").find()
                    .skip(shardno).limit(1).iterator();
            // System.out.println("Getting shard name");
            String shardName = "";
            while (shardlist.hasNext()) {
                Document obj = shardlist.next();

                shardName = obj.getString("_id");
                // System.out.println(shardName);
            }

            boolean move = false;
            while (!move) {
                try {
                    admindb.runCommand(new Document("moveChunk", testOpts.databaseName + "." + testOpts.collectionName)
                            .append("find", new Document("_id", new Document("w", workerID).append("i", sequence + 1)))
                            .append("to", shardName));
                    move = true;
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    if (e.getMessage().contains("that chunk is already on that shard")) {
                        move = true;
                    } else {
                        if (!e.getMessage().contains("could not aquire collection lock"))
                            System.out.println(e.getMessage());
                        try {
                            Thread.sleep(1000);
                        } catch (Exception ignored) {
                        }
                    }
                }

            }

            // System.out.println("Moved {w:" + workerID + ",i:" + (sequence + 1)
            // + "} to " + shardName);
            numShards = testOpts.numShards;
        }
    }

    MongoWorker(MongoClient c, POCTestOptions t, POCTestResults r, int id) {
        mongoClient = c;

        // Ping
        c.getDatabase("admin").runCommand(new Document("ping", 1));
        testOpts = t;
        testResults = r;
        workerID = id;
        MongoDatabase db = mongoClient.getDatabase(testOpts.databaseName);
        maxCollections = testOpts.numcollections;
        String baseCollectionName = testOpts.collectionName;
        if (maxCollections > 1) {
            colls = new ArrayList<MongoCollection<Document>>();
            lastCollection = 0;
            for (int i = 0; i < maxCollections; i++) {
                String str = baseCollectionName + i;
                colls.add(db.getCollection(str));
            }
        } else {
            coll = db.getCollection(baseCollectionName);
        }

        // id
        sequence = getHighestID();

        ReviewShards();
        rng = new Random();
        if (testOpts.zipfian) {
            zipfian = true;
            zipf = new ZipfDistribution(testOpts.zipfsize, 0.99);
        }

        if (testOpts.workflow != null) {
            workflow = testOpts.workflow;
            workflowed = true;
            keyStack = new ArrayList<Document>();
        }

    }

    private int getNextVal(int mult) {
        int rval;
        if (zipfian) {
            rval = zipf.sample();
        } else {
            rval = (int) Math.abs(Math.floor(rng.nextDouble() * mult));
        }
        return rval;
    }

    private int getHighestID() {
        int rval = 0;

        rotateCollection();
        Document query = new Document();

        // TODO Refactor the query for 3.0 driver
        Document limits = new Document("$gt", new Document("w", workerID));
        limits.append("$lt", new Document("w", workerID + 1));

        query.append("_id", limits);

        Document myDoc = coll.find(query).projection(include("_id")).sort(descending("_id")).first();
        if (myDoc != null) {
            Document id = (Document) myDoc.get("_id");
            rval = id.getInteger("i") + 1;
        }
        return rval;
    }

    // This one was a major rewrite as the whole Bulk Ops API changed in 3.0

    private void flushBulkOps(List<WriteModel<Document>> bulkWriter) {
        // Time this.
        rotateCollection();
        Date starttime = new Date();

        // This is where ALL writes are happening
        // So this can fail part way through if we have a failover
        // In which case we resubmit it

        boolean submitted = false;
        BulkWriteResult bwResult = null;

        while (!submitted && !bulkWriter.isEmpty()) { // can be empty if we removed a Dupe key error
            try {
                submitted = true;
                bwResult = coll.bulkWrite(bulkWriter);
            } catch (Exception e) {
                // We had a problem with this bulk op - some may be completed, some may not

                // I need to resubmit it here
                String error = e.getMessage();

                // Check if it's a sup key and remove it
                Pattern p = Pattern.compile("dup key: \\{ : \\{ w: (.*?), i: (.*?) }");
                // Pattern p = Pattern.compile("dup key");

                Matcher m = p.matcher(error);
                if (m.find()) {
                    // System.out.println("Duplicate Key");
                    // int thread = Integer.parseInt(m.group(1));
                    int uniqid = Integer.parseInt(m.group(2));
                    // System.out.println(" ID = " + thread + " " + uniqid );
                    boolean found = false;
                    for (Iterator<? super WriteModel<Document>> iter = bulkWriter.listIterator(); iter.hasNext();) {
                        // Check if it's a InsertOneModel

                        Object o = iter.next();
                        if (o instanceof InsertOneModel<?>) {
                            @SuppressWarnings("unchecked")
                            InsertOneModel<Document> a = (InsertOneModel<Document>) o;
                            Document id = (Document) a.getDocument().get("_id");

                            // int opthread=id.getInteger("w");
                            // int opid = id.getInteger("i");
                            // System.out.println("opthread: " + opthread + "=" + thread + " opid: " + opid
                            // + "=" + uniqid);
                            if (id.getInteger("i") == uniqid) {
                                // System.out.println(" Removing " + thread + " " + uniqid + " from bulkop as
                                // already inserted");
                                iter.remove();
                                found = true;
                            }
                        }
                    }
                    if (!found) {
                        System.out.println("Cannot find failed op in batch!");
                    }
                } else {
                    // Some other error occurred - possibly MongoCommandException,
                    // MongoTimeoutException
                    System.out.println(e.getClass().getSimpleName() + ": " + error);
                    // Print a full stacktrace since we're in debug mode
                    if (testOpts.debug)
                        e.printStackTrace();
                }
                // System.out.println("No result returned");
                submitted = false;
            }
        }

        Date endtime = new Date();

        Long taken = endtime.getTime() - starttime.getTime();

        int icount = bwResult.getInsertedCount();
        int ucount = bwResult.getMatchedCount();

        // If the bulk op is slow - ALL those ops were slow

        if (taken > testOpts.slowThreshold) {
            testResults.RecordSlowOp("inserts", icount);
            testResults.RecordSlowOp("updates", ucount);
        }
        testResults.RecordOpsDone("inserts", icount);
        testResults.RecordOpsDone("updates", ucount);
        // Increment the inserted docs counter
        testResults.setInsertDocs(icount);
        testResults.setUpdateDocs(ucount);
        insertCount += icount;
        updateCount += ucount;
    }

    private Document simpleKeyQuery() {
        // Key Query
        rotateCollection();
        Document query = new Document();
        int range = sequence * testOpts.workingset / 100;
        int rest = sequence - range;

        int recordno = rest + getNextVal(range);

        query.append("_id", new Document("w", workerID).append("i", recordno));
        Date starttime = new Date();
        Document myDoc;
        List<String> projFields = new ArrayList<String>(testOpts.numFields);

        if (testOpts.projectFields == 0) {
            myDoc = coll.find(query).first();
        } else {
            int numProjFields = (testOpts.projectFields <= testOpts.numFields) ? testOpts.projectFields
                    : testOpts.numFields;
            int i = 0;
            while (i < numProjFields) {
                projFields.add("fld" + i);
                i++;
            }
            myDoc = coll.find(query).projection(fields(include(projFields))).first();
        }

        if (myDoc != null) {

            Date endtime = new Date();
            Long taken = endtime.getTime() - starttime.getTime();
            if (taken > testOpts.slowThreshold) {
                testResults.RecordSlowOp("keyqueries", 1);
            }
            testResults.RecordOpsDone("keyqueries", 1);
            // Increment the read docs counter
            testResults.setReadDocs(1);
            readCount += 1;
        }
        return myDoc;
    }

    private void rangeQuery() {
        // Key Query
        rotateCollection();
        Document query = new Document();
        List<String> projFields = new ArrayList<String>(testOpts.numFields);
        int recordno = getNextVal(sequence);
        query.append("_id", new Document("$gt", new Document("w", workerID).append("i", recordno)));
        Date starttime = new Date();
        MongoCursor<Document> cursor;
        if (testOpts.projectFields == 0) {
            // If the read operations are limited apply the min limit for range queries.
            // This is the read operations limit minus the already executed read operations.
            cursor = coll.find(query).limit(toIntExact(Long.min(testOpts.rangeDocs, (readOpLimit - readCount))))
                    .iterator();
        } else {
            int numProjFields = (testOpts.projectFields <= testOpts.numFields) ? testOpts.projectFields
                    : testOpts.numFields;
            int i = 0;
            while (i < numProjFields) {
                projFields.add("fld" + i);
                i++;
            }
            // If the read operations are limited apply the min limit for range queries.
            // This is the read operations limit minus the already executed read operations.
            cursor = coll.find(query).projection(fields(include(projFields)))
                    .limit(toIntExact(Long.min(testOpts.rangeDocs, (readOpLimit - readCount)))).iterator();
        }
        while (cursor.hasNext()) {

            @SuppressWarnings("unused")
            Document obj = cursor.next();
            // Increment the read docs counter
            testResults.setReadDocs(1);
            readCount += 1;
        }
        cursor.close();

        Date endtime = new Date();
        Long taken = endtime.getTime() - starttime.getTime();
        if (taken > testOpts.slowThreshold) {
            testResults.RecordSlowOp("rangequeries", 1);
        }
        testResults.RecordOpsDone("rangequeries", 1);

    }

    private void rotateCollection() {
        if (maxCollections > 1) {
            coll = colls.get(lastCollection);
            lastCollection = (lastCollection + 1) % maxCollections;
        }
    }

    private void updateSingleRecord(List<WriteModel<Document>> bulkWriter) {
        updateSingleRecord(bulkWriter, null);
    }

    private void updateSingleRecord(List<WriteModel<Document>> bulkWriter, Document key) {
        // Key Query
        rotateCollection();
        Document query = new Document();
        Document change;

        if (key == null) {
            int range = sequence * testOpts.workingset / 100;
            int rest = sequence - range;

            int recordno = rest + getNextVal(range);

            query.append("_id", new Document("w", workerID).append("i", recordno));
        } else {
            query.append("_id", key);
        }

        int updateFields = (testOpts.updateFields <= testOpts.numFields) ? testOpts.updateFields : testOpts.numFields;

        if (updateFields == 1) {
            long changedfield = (long) getNextVal((int) testOpts.NUMBER_SIZE);
            Document fields = new Document("fld0", changedfield);
            change = new Document("$set", fields);
        } else {
            TestRecord tr = createNewRecord();
            tr.internalDoc.remove("_id");
            change = new Document("$set", tr.internalDoc);
        }

        if (!testOpts.findandmodify) {
            bulkWriter.add(new UpdateManyModel<Document>(query, change));
        } else {
            this.coll.findOneAndUpdate(query, change); // These are immediate not batches
        }
        testResults.RecordOpsDone("updates", 1);
        // Increment the inserted docs count.
        testResults.setUpdateDocs(1);
        updateCount += 1;
    }

    private TestRecord createNewRecord() {
        int[] arr = new int[2];
        arr[0] = testOpts.arraytop;
        arr[1] = testOpts.arraynext;
        return new TestRecord(testOpts.numFields, testOpts.depth, testOpts.textFieldLen, workerID, sequence++,
                testOpts.NUMBER_SIZE, arr, testOpts.blobSize);
    }

    // TODO ADD SESSION TO ENABLE TRANSACTIONS
    private TestRecord insertNewRecord(List<WriteModel<Document>> bulkWriter) {
        TestRecord tr = createNewRecord();
        bulkWriter.add(new InsertOneModel<Document>(tr.internalDoc));
        return tr;
    }

    // The test completes when the durarion elapses or if there is a limit for
    // inserted or
    // readed documents in this case the lower limit will end the test.
    private Boolean isTestComplete() {
        Boolean result = false;
        if (isOpLimit()) {
            result = (isReadOpLimit(readCount) || isInsertOpLimit(insertCount) || isUpdateOpLimit(updateCount)) ? true
                    : false;
        } else {
            result = testResults.GetSecondsElapsed() > testOpts.duration;
        }
        ;

        return result;
    }

    private boolean isOpLimit() {
        return (testOpts.readOpLimit == 0 && testOpts.insertOpLimit == 0 && testOpts.updateOpLimit == 0) ? false : true;
    }

    private boolean isInsertOpLimit(long count) {
        long result = 0;
        if (testOpts.insertOpLimit != 0) {
            result = testOpts.insertOpLimit / testOpts.numThreads;
            if ((workerID == 0) && (testOpts.insertOpLimit % testOpts.numThreads != 0)) {
                result += testOpts.insertOpLimit % testOpts.numThreads;
            }
            ;
        }
        ;

        return ((result > 0) && (result == count)) ? true : false;
    }

    private boolean isReadOpLimit(long count) {
        readOpLimit = 0;
        if (testOpts.readOpLimit != 0) {
            readOpLimit += testOpts.readOpLimit / testOpts.numThreads;
            if ((workerID == 0) && (testOpts.readOpLimit % testOpts.numThreads != 0)) {
                readOpLimit += testOpts.readOpLimit % testOpts.numThreads;
            }
            ;
        }
        ;

        return ((readOpLimit > 0) && (readOpLimit == count)) ? true : false;
    }

    private boolean isUpdateOpLimit(long count) {
        long result = 0;
        if (testOpts.updateOpLimit != 0) {
            result += testOpts.updateOpLimit / testOpts.numThreads;
            if ((workerID == 0) && (testOpts.updateOpLimit % testOpts.numThreads != 0)) {
                result += testOpts.updateOpLimit % testOpts.numThreads;
            }
            ;
        }
        ;
        return ((result > 0) && (result == count)) ? true : false;
    }

    private boolean isWriteOpLimit(int batch) {
        long limit = 0;
        if (testOpts.insertOpLimit != 0) {
            limit = testOpts.insertOpLimit / testOpts.numThreads;
            if ((workerID == 0) && (testOpts.insertOpLimit % testOpts.numThreads != 0)) {
                limit += testOpts.insertOpLimit % testOpts.numThreads;
            }
            ;
        }
        ;
        if (testOpts.updateOpLimit != 0) {
            limit += testOpts.updateOpLimit / testOpts.numThreads;
            if ((workerID == 0) && (testOpts.updateOpLimit % testOpts.numThreads != 0)) {
                limit += testOpts.updateOpLimit % testOpts.numThreads;
            }
            ;
        }
        ;

        return (testOpts.insertOpLimit != 0 ? insertCount : 0) + (testOpts.updateOpLimit != 0 ? updateCount : 0)
                + batch == limit ? true : false;
    }

    public void run() {
        // Use a bulk inserter - even if only for one document
        List<WriteModel<Document>> bulkWriter;

        try {
            bulkWriter = new ArrayList<WriteModel<Document>>();
            int bulkops = 0;

            int c = 0;
            System.out.println("Worker thread " + workerID + " Started.");
            while (!isTestComplete()) {
                c++;
                // Timer isn't granullar enough to sleep for each
                if (testOpts.opsPerSecond > 0) {
                    double threads = testOpts.numThreads;
                    double opsperthreadsecond = testOpts.opsPerSecond / threads;
                    double sleeptimems = 1000 / opsperthreadsecond;

                    if (c == 1) {
                        // First time randomise

                        Random r = new Random();
                        sleeptimems = r.nextInt((int) Math.floor(sleeptimems));

                    }
                    Thread.sleep((int) Math.floor(sleeptimems));
                }

                if (!workflowed) {
                    // System.out.println("Random op");
                    // Choose the type of op
                    int allops = testOpts.insertops + testOpts.keyqueries + testOpts.updates + testOpts.rangequeries
                            + testOpts.arrayupdates;
                    int randop = getNextVal(allops);
                    if (randop < testOpts.insertops) {
                        insertNewRecord(bulkWriter);
                        bulkops++;
                    } else if (randop < testOpts.insertops + testOpts.keyqueries) {
                        simpleKeyQuery();
                    } else if (randop < testOpts.insertops + testOpts.keyqueries + testOpts.rangequeries) {
                        rangeQuery();
                    } else {
                        // An in place single field update
                        // fld 0 - set to random number
                        updateSingleRecord(bulkWriter);
                        if (!testOpts.findandmodify)
                            bulkops++;
                    }
                } else {
                    // Following a preset workflow
                    String wfop = workflow.substring(workflowStep, workflowStep + 1);

                    // System.out.println("Executing workflow op [" + workflow +
                    // "] " + wfop);
                    if (wfop.equals("i")) {
                        // Insert a new record, push it's key onto our stack
                        TestRecord r = insertNewRecord(bulkWriter);
                        keyStack.add((Document) r.internalDoc.get("_id"));
                        bulkops++;
                        // System.out.println("Insert");
                    } else if (wfop.equals("u")) {
                        if (keyStack.size() > 0) {
                            updateSingleRecord(bulkWriter, keyStack.get(keyStack.size() - 1));
                            // System.out.println("Update");
                            if (!testOpts.findandmodify)
                                bulkops++;
                        }
                    } else if (wfop.equals("p")) {
                        // Pop the top thing off the stack
                        if (keyStack.size() > 0) {
                            keyStack.remove(keyStack.size() - 1);
                        }
                    } else if (wfop.equals("k")) {
                        // Find a new record an put it on the stack
                        Document r = simpleKeyQuery();
                        if (r != null) {
                            keyStack.add((Document) r.get("_id"));
                        }
                    }

                    // If we have reached the end of the wfops then reset
                    workflowStep++;
                    if (workflowStep >= workflow.length()) {
                        workflowStep = 0;
                        keyStack = new ArrayList<Document>();
                    }
                }

                if ((c % testOpts.batchSize == 0) || isWriteOpLimit(bulkops)) {
                    if (bulkops > 0) {
                        flushBulkOps(bulkWriter);
                        bulkWriter.clear();
                        bulkops = 0;
                        // Check and see if we need to rejig sharding
                        if (numShards != testOpts.numShards) {
                            ReviewShards();
                        }
                    }
                }

            }

        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            if (testOpts.debug)
                e.printStackTrace();
        }
    }
}
