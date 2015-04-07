import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

public class MongoWorker implements Runnable {

	MongoClient mongoClient;
	DB db;
	DBCollection coll;
	POCTestOptions testOpts;
	POCTestResults testResults;
	int workerID;
	int sequence;
	Random rng;
	boolean workflowed = false;
	String workflow;
	int workflowStep = 0;
	ArrayList<BasicDBObject> keyStack;

	public void ReviewShards() {
		if (testOpts.sharded && !testOpts.singleserver) {
			// I'd like to pick a shard and write there - it's going to be
			// faster and
			// We can ensure we distribute our workers over out shards
			// So we will tell mongo that's where we want our records to go
			DB admindb = mongoClient.getDB("admin");
			CommandResult cr;
			cr = admindb.command(new BasicDBObject("split",
					testOpts.databaseName + "." + testOpts.collectionName)
					.append("middle",
							new BasicDBObject("_id", new BasicDBObject("w",
									workerID).append("s", sequence + 1))));

			// And move that to a shard - which shard? take my workerid and mod
			// it with the number of shards
			int shardno = workerID % testOpts.numShards;
			// Get the name of the shard

			DBCursor shardlist = mongoClient.getDB("config")
					.getCollection("shards").find();
			shardlist.skip(shardno);
			shardlist.limit(1);
			String shardName = new String("");
			while (shardlist.hasNext()) {
				BasicDBObject obj = (BasicDBObject) shardlist.next();
				shardName = obj.getString("_id");

			}

			cr = admindb.command(new BasicDBObject("moveChunk",
					testOpts.databaseName + "." + testOpts.collectionName)
					.append("find",
							new BasicDBObject("_id", new BasicDBObject("w",
									workerID).append("s", sequence + 1)))
					.append("to", shardName));

		}
	}

	public MongoWorker(MongoClient c, POCTestOptions t, POCTestResults r, int id) {
		mongoClient = c;
		testOpts = t;
		testResults = r;
		workerID = id;
		db = mongoClient.getDB(testOpts.databaseName);
		coll = db.getCollection(testOpts.collectionName);
		// id
		sequence = getHighestID();

		ReviewShards();
		rng = new Random();

		if (testOpts.workflow != null) {
			workflow = testOpts.workflow;
			workflowed = true;
			keyStack = new ArrayList<BasicDBObject>();
		}

	}

	private int getHighestID() {
		int rval = 0;
		BasicDBObject query = new BasicDBObject();

		BasicDBObject limits = new BasicDBObject("$gt", new BasicDBObject("w",
				workerID));
		limits.append("$lt", new BasicDBObject("w", workerID + 1));
		query.append("_id", limits);

		BasicDBObject myDoc = (BasicDBObject) coll.findOne(query,
				new BasicDBObject("_id", 1), new BasicDBObject("_id", -1));
		if (myDoc != null) {
			BasicDBObject id = (BasicDBObject) myDoc.get("_id");
			rval = id.getInt("i") + 1;
		}
		return rval;
	}

	private BulkWriteOperation flushBulkOps(BulkWriteOperation bulkWriter) {
		// Time this.

		Date starttime = new Date();
		BulkWriteResult bwResult = bulkWriter.execute();
		Date endtime = new Date();

		Long taken = endtime.getTime() - starttime.getTime();

		int icount = bwResult.getInsertedCount();
		int ucount = bwResult.getModifiedCount();

		// If the bulk op is slow - ALL those ops were slow

		if (taken > testOpts.slowThreshold) {
			testResults.RecordSlowOp("inserts", icount);
			testResults.RecordSlowOp("updates", ucount);
		}
		testResults.RecordOpsDone("inserts", icount);
		return coll.initializeUnorderedBulkOperation();
	}

	private BasicDBObject simpleKeyQuery() {
		// Key Query
		BasicDBObject query = new BasicDBObject();
		int range = sequence * testOpts.workingset / 100;
		int rest = sequence - range;

		int recordno = rest
				+ (int) Math.abs(Math.floor(rng.nextDouble() * range));

		query.append("_id",
				new BasicDBObject("w", workerID).append("i", recordno));
		Date starttime = new Date();
		BasicDBObject myDoc = (BasicDBObject) coll.findOne(query);
		if (myDoc != null) {

			Date endtime = new Date();
			Long taken = endtime.getTime() - starttime.getTime();
			if (taken > testOpts.slowThreshold) {
				testResults.RecordSlowOp("keyqueries", 1);
			}
			testResults.RecordOpsDone("keyqueries", 1);
		}
		return (BasicDBObject) myDoc;
	}

	private void rangeQuery() {
		// Key Query
		BasicDBObject query = new BasicDBObject();
		int recordno = (int) Math.abs(Math.floor(rng.nextDouble() * sequence));
		query.append("_id", new BasicDBObject("$gt", new BasicDBObject("w",
				workerID).append("i", recordno)));
		Date starttime = new Date();
		DBCursor cursor = (DBCursor) coll.find(query);
		cursor.limit(20);
		while (cursor.hasNext()) {

			@SuppressWarnings("unused")
			BasicDBObject obj = (BasicDBObject) cursor.next();
		}

		Date endtime = new Date();
		Long taken = endtime.getTime() - starttime.getTime();
		if (taken > testOpts.slowThreshold) {
			testResults.RecordSlowOp("rangequeries", 1);
		}
		testResults.RecordOpsDone("rangequeries", 1);

	}

	private void incrementArrayValue(BulkWriteOperation bulkWriter) {
		// Key Query
		BasicDBObject query = new BasicDBObject();
		int recordno = (int) Math.abs(Math.floor(rng.nextDouble() * sequence));

		query.append("_id",
				new BasicDBObject("w", workerID).append("i", recordno));

		int outerIndex;
		int innerIndex;
		outerIndex = (int) Math.abs(Math.floor(rng.nextDouble()
				* testOpts.arraytop));
		innerIndex = (int) Math.abs(Math.floor(rng.nextDouble()
				* testOpts.arraynext));

		BasicDBObject fields = new BasicDBObject("arr." + outerIndex + "."
				+ innerIndex, 1);
		BasicDBObject change = new BasicDBObject("$inc", fields);

		bulkWriter.find(query).updateOne(change);
		testResults.RecordOpsDone("updates", 1);

	}

	private void updateSingleRecord(BulkWriteOperation bulkWriter) {
		updateSingleRecord(bulkWriter, null);
	}

	private void updateSingleRecord(BulkWriteOperation bulkWriter,
			BasicDBObject key) {
		// Key Query
		BasicDBObject query = new BasicDBObject();
		long changedfield = (long) Math.abs(Math.floor(rng.nextDouble()
				* testOpts.NUMBER_SIZE));

		if (key == null) {
			int range = sequence * testOpts.workingset / 100;
			int rest = sequence - range;

			int recordno = rest
					+ (int) Math.abs(Math.floor(rng.nextDouble() * range));

			query.append("_id",
					new BasicDBObject("w", workerID).append("i", recordno));
		} else {
			query.append("_id", key);
		}
		BasicDBObject fields = new BasicDBObject("fld0", changedfield);
		BasicDBObject change = new BasicDBObject("$set", fields);

		if (testOpts.findandmodify == false) {
			bulkWriter.find(query).updateOne(change);
		} else {
			this.coll.findAndModify(query, change);
		}
		testResults.RecordOpsDone("updates", 1);

	}

	private TestRecord insertNewRecord(BulkWriteOperation bulkWriter) {
		TestRecord tr;
		int[] arr = new int[2];
		arr[0] = testOpts.arraytop;
		arr[1] = testOpts.arraynext;
		tr = new TestRecord(testOpts.numFields, testOpts.textFieldLen,
				workerID, sequence++, testOpts.NUMBER_SIZE, testOpts.numShards,
				arr);
		bulkWriter.insert(tr);
		return tr;
	}

	@Override
	public void run() {
		// Use a bulk inserter - even if ony for one
		BulkWriteOperation bulkWriter;

		try {
			bulkWriter = coll.initializeUnorderedBulkOperation();
			int bulkops = 0;

			int c = 0;
			// System.out.println("Child " + this.workerID + " running");
			while (testResults.GetSecondsElapsed() < testOpts.duration) {
				c++;

				if (workflowed == false) {
					// System.out.println("Random op");
					// Choose the type of op
					int allops = testOpts.insertops + testOpts.keyqueries
							+ testOpts.updates + testOpts.rangequeries
							+ testOpts.arrayupdates;
					int randop = (int) Math.abs(Math.floor(rng.nextDouble()
							* allops));

					if (randop < testOpts.insertops) {
						insertNewRecord(bulkWriter);
						bulkops++;
					} else if (randop < testOpts.insertops
							+ testOpts.keyqueries) {
						simpleKeyQuery();
					} else if (randop < testOpts.insertops
							+ testOpts.keyqueries + testOpts.rangequeries) {
						rangeQuery();
					} else if (randop < testOpts.insertops
							+ testOpts.keyqueries + testOpts.rangequeries
							+ testOpts.arrayupdates) {
						incrementArrayValue(bulkWriter);
						bulkops++;
					} else {
						// An in place single field update
						// fld 0 - set to random number
						updateSingleRecord(bulkWriter);
						if (testOpts.findandmodify == false)
							bulkops++;
					}
				} else {
					// Following a preset workflow
					String wfop = workflow.substring(workflowStep,
							workflowStep + 1);

					// System.out.println("Executing workflow op [" + workflow +
					// "] " + wfop);
					if (wfop.equals("i")) {
						// Insert a new record, push it's key onto our stack
						TestRecord r = insertNewRecord(bulkWriter);
						keyStack.add((BasicDBObject) r.get("_id"));
						bulkops++;
						// System.out.println("Insert");
					} else if (wfop.equals("u")) {
						if (keyStack.size() > 0) {
							updateSingleRecord(bulkWriter,
									keyStack.get(keyStack.size() - 1));
							// System.out.println("Update");
							if (testOpts.findandmodify == false)
								bulkops++;
						}
					} else if (wfop.equals("p")) {
						// Pop the top thing off the stack
						if (keyStack.size() > 0) {
							keyStack.remove(keyStack.size() - 1);
						}
					} else if (wfop.equals("k")) {
						// Find a new record an put it on the stack
						BasicDBObject r = simpleKeyQuery();
						if (r != null) {
							keyStack.add((BasicDBObject) r.get("_id"));
						}
					}

					// If we have reached the end of the wfops then reset
					workflowStep++;
					if (workflowStep >= workflow.length()) {
						workflowStep = 0;
						keyStack = new ArrayList<BasicDBObject>();
					}
				}

				if (c % testOpts.batchSize == 0) {
					if (bulkops > 0) {
						bulkWriter = flushBulkOps(bulkWriter);
						bulkops = 0;
					}
				}

			}

		} catch (Exception e) {
			System.out.println("Error: " + e.getMessage());
			e.printStackTrace();
			return;
		}
	}
}
