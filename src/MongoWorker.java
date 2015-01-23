import java.util.Date;
import java.util.Random;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
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

	public MongoWorker(MongoClient c, POCTestOptions t, POCTestResults r, int id) {
		mongoClient = c;
		testOpts = t;
		testResults = r;
		workerID = id;
		db = mongoClient.getDB(testOpts.databaseName);
		coll = db.getCollection(testOpts.collectionName);
		// Use my workerID and find the highest id used for that
		// id
		sequence = getHighestID();
		rng = new Random();
		// System.out.println(" Connection " + workerID +
		// " reports highest id used is " + sequence);
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
		if (taken > testOpts.slowThreshold) {
			testResults.RecordSlowOp("inserts");
		}
		Long icount = (long) bwResult.getInsertedCount();
		testResults.RecordOpsDone("inserts", icount);
		return coll.initializeUnorderedBulkOperation();
	}

	private void simpleKeyQuery() {
		// Key Query
		BasicDBObject query = new BasicDBObject();
		int recordno = (int) Math.abs(Math.floor(rng.nextDouble() * sequence));
		query.append("_id",
				new BasicDBObject("w", workerID).append("i", recordno));
		Date starttime = new Date();
		BasicDBObject myDoc = (BasicDBObject) coll.findOne(query);
		if (myDoc != null) {

			Date endtime = new Date();
			Long taken = endtime.getTime() - starttime.getTime();
			if (taken > testOpts.slowThreshold) {
				testResults.RecordSlowOp("keyqueries");
			}
			testResults.RecordOpsDone("keyqueries", (long) 1);
		}
	}
	
	private void rangeQuery() {
		// Key Query
		BasicDBObject query = new BasicDBObject();
		int recordno = (int) Math.abs(Math.floor(rng.nextDouble() * sequence));
		query.append("_id",new BasicDBObject( "$gt",
				new BasicDBObject("w", workerID).append("i", recordno)));
		Date starttime = new Date();
		DBCursor cursor = (DBCursor) coll.find(query);
		cursor.limit(20);
		while( cursor.hasNext() )
		{
		     BasicDBObject obj = (BasicDBObject)cursor.next();
		}
	

			Date endtime = new Date();
			Long taken = endtime.getTime() - starttime.getTime();
			if (taken > testOpts.slowThreshold) {
				testResults.RecordSlowOp("rangequeries");
			}
			testResults.RecordOpsDone("rangequeries", (long) 1);
		
	}
	

	private void updateSingleRecord(BulkWriteOperation bulkWriter) {
		// Key Query
		BasicDBObject query = new BasicDBObject();
		int recordno = (int) Math.abs(Math.floor(rng.nextDouble() * sequence));
		long changedfield = (long) Math.abs(Math.floor(rng.nextDouble() * testOpts.NUMBER_SIZE));
		query.append("_id",
				new BasicDBObject("w", workerID).append("i", recordno));
		BasicDBObject fields = new BasicDBObject("fld0",changedfield);
		BasicDBObject change = new BasicDBObject("$set",fields);
		Date starttime = new Date();
		//
		bulkWriter.find(query).updateOne(change);
		
		

			Date endtime = new Date();
			Long taken = endtime.getTime() - starttime.getTime();
			if (taken > testOpts.slowThreshold) {
				testResults.RecordSlowOp("updates");
			}
		testResults.RecordOpsDone("updates", (long) 1);
		
	}
	
	private void insertNewRecord(BulkWriteOperation bulkWriter) {
		TestRecord tr;
		tr = new TestRecord(testOpts.numFields, testOpts.textFieldLen,
				workerID, sequence++,testOpts.NUMBER_SIZE);
		bulkWriter.insert(tr);
	}

	@Override
	public void run() {
		// Use a bulk inserter - even if ony for one
		BulkWriteOperation bulkWriter;

		try {
			bulkWriter = coll.initializeUnorderedBulkOperation();
			int bulkops = 0;
		
			int c=0;
			while (testResults.GetSecondsElapsed() < testOpts.duration) {
				c++;
				// Choose the type of op
				int allops = testOpts.insertops + testOpts.keyqueries + testOpts.updates + testOpts.rangequeries;
				int randop = (int) Math.abs(Math.floor(rng.nextDouble()
						* allops));

				
				if (randop < testOpts.insertops) {
					insertNewRecord(bulkWriter);
					bulkops++;
				} else if(randop < testOpts.insertops + testOpts.keyqueries) {
					simpleKeyQuery();
				} else if(randop < testOpts.insertops + testOpts.keyqueries + testOpts.rangequeries)
				{
					rangeQuery();
				} else {
					//An in place single field update
					//fld 0 - set to random number
					updateSingleRecord(bulkWriter);
				}
				
				
				if (c % testOpts.batchSize == 0) {
					if(bulkops > 0)
					{
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
