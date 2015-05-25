
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateManyModel;

import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;

import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.*;

public class MongoWorker implements Runnable {

	MongoClient mongoClient;
	MongoDatabase db;
	MongoCollection<Document>  coll;
	POCTestOptions testOpts;
	POCTestResults testResults;
	int workerID;
	int sequence;
	int numShards = 0;
	Random rng;
	boolean workflowed = false;
	String workflow;
	int workflowStep = 0;
	ArrayList<Document> keyStack;

	public void ReviewShards() {
		if (testOpts.sharded && !testOpts.singleserver) {
			// I'd like to pick a shard and write there - it's going to be
			// faster and
			// We can ensure we distribute our workers over out shards
			// So we will tell mongo that's where we want our records to go
			MongoDatabase admindb = mongoClient.getDatabase("admin");
			Boolean split = false;
			Document cr;
			while (split == false) {
			
				try {
				cr = admindb.runCommand(new Document("split",
						testOpts.databaseName + "." + testOpts.collectionName)
						.append("middle",
								new Document("_id", new Document("w",
										workerID).append("i", sequence + 1))));
					split=true;
				} catch(Exception e){
					
					if(e.getMessage().contains("is a boundary key of existing"))
					{
						split = true;
					} else {
						System.out.println(e.getMessage());
						try { Thread.sleep(1000); } catch (Exception f) {}
					}
				}
				
			}
			// And move that to a shard - which shard? take my workerid and mod
			// it with the number of shards
			int shardno = workerID % testOpts.numShards;
			// Get the name of the shard

			MongoCursor<Document> shardlist = mongoClient.getDatabase("config")
					.getCollection("shards").find().skip(shardno).limit(1).iterator();
			String shardName = new String("");
			while (shardlist.hasNext()) {
				Document obj = shardlist.next();
				shardName = obj.getString("_id");

			}
		
			boolean move = false;
			while (move == false) {
				cr = null;
				try
				{
				cr = admindb.runCommand(new Document("moveChunk",
						testOpts.databaseName + "." + testOpts.collectionName)
						.append("find",
								new Document("_id", new Document("w",
										workerID).append("i", sequence + 1)))
						.append("to", shardName));
				} catch(Exception e){
					
					if(e.getMessage().contains("that chunk is already on that shard"))
					{
						move = true;
					} else {
						System.out.println(e.getMessage());
						try { Thread.sleep(1000); } catch (Exception g){}
					}
				}
				
			
			}
		
		//System.out.println("Moved {w:" + workerID + ",i:" + (sequence + 1)
		//		+ "} to " + shardName);
		numShards = testOpts.numShards;
		}
	}

	public MongoWorker(MongoClient c, POCTestOptions t, POCTestResults r, int id) {
		mongoClient = c;
		testOpts = t;
		testResults = r;
		workerID = id;
		db = mongoClient.getDatabase(testOpts.databaseName);
		coll = db.getCollection(testOpts.collectionName);
		// id
		sequence = getHighestID();

		ReviewShards();
		rng = new Random();

		if (testOpts.workflow != null) {
			workflow = testOpts.workflow;
			workflowed = true;
			keyStack = new ArrayList<Document>();
		}

	}

	private int getHighestID() {
		int rval = 0;
		Document query = new Document();

		//TODO Refactor the query for 3.0 driver
		Document limits = new Document("$gt", new Document("w",
				workerID));
		limits.append("$lt", new Document("w", workerID + 1));
		
		query.append("_id", limits);

		Document myDoc = (Document) coll.find(query).projection(include("_id"))
													.sort(descending("_id"))
													.first();
		if (myDoc != null) {
			Document id = (Document) myDoc.get("_id");
			rval = id.getInteger("i") + 1;
		}
		return rval;
	}

	//This one was a major rewrite as the whole Bulk Ops API changed in 3.0
	
	private boolean flushBulkOps(List<WriteModel<Document>> bulkWriter) {
		// Time this.

		Date starttime = new Date();
		
		//This is where ALL writes are happening
				
		//So this can fail part way through if we have a failover
		//In which case we resubmit it
		
		boolean submitted = false;
		BulkWriteResult bwResult=null;
	
		while(submitted == false )
		{
			try
			{
				 submitted = true;
				 bwResult = coll.bulkWrite(bulkWriter);
			}
			catch (Exception e)
			{
				//I need to resubmit it here
				System.out.println("Exception: " + e.getMessage());
				if(bwResult != null)
				{
					System.out.println(bwResult.toString());
					submitted = false;
				} else {
					System.out.println("No result returned");
					submitted = false;
				}
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
		return true;
		
	}

	
	
	private Document simpleKeyQuery() {
		// Key Query
		Document query = new Document();
		int range = sequence * testOpts.workingset / 100;
		int rest = sequence - range;

		int recordno = rest
				+ (int) Math.abs(Math.floor(rng.nextDouble() * range));

		query.append("_id",
				new Document("w", workerID).append("i", recordno));
		Date starttime = new Date();
		Document myDoc = (Document) coll.find(query).first();
		if (myDoc != null) {

			Date endtime = new Date();
			Long taken = endtime.getTime() - starttime.getTime();
			if (taken > testOpts.slowThreshold) {
				testResults.RecordSlowOp("keyqueries", 1);
			}
			testResults.RecordOpsDone("keyqueries", 1);
		}
		return (Document) myDoc;
	}
	
	private Document wholeBucketQuery() {
		
		
		// only valid if we are using buckets
		if(testOpts.numBuckets == 0)
		{
			return new Document();
		}
		
		
		Document query = new Document();
		int range = sequence * testOpts.workingset / 100;
		int rest = sequence - range;

		int recordno = rest
				+ (int) Math.abs(Math.floor(rng.nextDouble() * range));

		int bucketno = recordno % testOpts.numBuckets;
		
		query.append("bucket",bucketno);
		
		Date starttime = new Date();
		//This could be slow
		List<Document> foundDocument = coll.find().into(new ArrayList<Document>()); //Fetch all
		
		
		if (foundDocument.size() > 0) {

			Date endtime = new Date();
			Long taken = endtime.getTime() - starttime.getTime();
			if (taken > testOpts.slowThreshold) {
				testResults.RecordSlowOp("keyqueries", 1);
			}
			testResults.RecordOpsDone("keyqueries", 1);
		}
		
		return new Document();
	}
	

	private void rangeQuery() {
		// Key Query
		Document query = new Document();
		int recordno = (int) Math.abs(Math.floor(rng.nextDouble() * sequence));
		query.append("_id", new Document("$gt", new Document("w",
				workerID).append("i", recordno)));
		Date starttime = new Date();
		MongoCursor<Document>  cursor = coll.find(query).limit(20).iterator();
		while (cursor.hasNext()) {

			@SuppressWarnings("unused")
			Document obj = cursor.next();
		}

		Date endtime = new Date();
		Long taken = endtime.getTime() - starttime.getTime();
		if (taken > testOpts.slowThreshold) {
			testResults.RecordSlowOp("rangequeries", 1);
		}
		testResults.RecordOpsDone("rangequeries", 1);

	}

	private void incrementArrayValue(List<? super WriteModel<Document>> bulkWriter) {
		// Key Query
		Document query = new Document();
		int recordno = (int) Math.abs(Math.floor(rng.nextDouble() * sequence));

		query.append("_id",
				new Document("w", workerID).append("i", recordno));

		int outerIndex;
		int innerIndex;
		outerIndex = (int) Math.abs(Math.floor(rng.nextDouble()
				* testOpts.arraytop));
		innerIndex = (int) Math.abs(Math.floor(rng.nextDouble()
				* testOpts.arraynext));

		Document fields = new Document("arr." + outerIndex + "."
				+ innerIndex, 1);
		Document change = new Document("$inc", fields);


		bulkWriter.add(new UpdateManyModel<Document>(query,change));
                
		testResults.RecordOpsDone("updates", 1);

	}

	private void updateSingleRecord(List<WriteModel<Document>>  bulkWriter) {
		updateSingleRecord(bulkWriter, null);
	}

	private void updateSingleRecord(List<WriteModel<Document>>  bulkWriter,
			Document key) {
		// Key Query
		Document query = new Document();
		long changedfield = (long) Math.abs(Math.floor(rng.nextDouble()
				* testOpts.NUMBER_SIZE));

		if (key == null) {
			int range = sequence * testOpts.workingset / 100;
			int rest = sequence - range;

			int recordno = rest
					+ (int) Math.abs(Math.floor(rng.nextDouble() * range));

			query.append("_id",
					new Document("w", workerID).append("i", recordno));
		} else {
			query.append("_id", key);
		}
		Document fields = new Document("fld0", changedfield);
		Document change = new Document("$set", fields);

		if (testOpts.findandmodify == false) {
			bulkWriter.add(new UpdateManyModel<Document>(query,change));
		} else {
			this.coll.findOneAndUpdate(query, change); //These are immediate not batches
		}
		testResults.RecordOpsDone("updates", 1);

	}

	private TestRecord insertNewRecord(List<WriteModel<Document>>  bulkWriter) {
		TestRecord tr;
		int[] arr = new int[2];
		arr[0] = testOpts.arraytop;
		arr[1] = testOpts.arraynext;
		tr = new TestRecord(testOpts.numFields, testOpts.textFieldLen,
				workerID, sequence++, testOpts.NUMBER_SIZE, testOpts.numShards,
				arr);
		
		if(testOpts.numBuckets > 0 && testOpts.bucketSize > 0)
		{
			return insertNewRecordInBucket( bulkWriter,  tr);
		}
		bulkWriter.add(new InsertOneModel<Document>(tr.internalDoc));
		return tr;
	}

	//This one is changing inserts to upserts with push
	
	private TestRecord insertNewRecordInBucket(
			List<WriteModel<Document>> bulkWriter, TestRecord tr) {
		

		int bucket = sequence % testOpts.numBuckets;

		Document query = new Document();
		query.append("bucket", bucket);
		query.append("count", new Document("$lt", testOpts.bucketSize));

		Document fields = new Document("recs", tr.internalDoc);
		Document change = new Document("$push", fields);
		Document incCount = new Document("count", 1);
		
		change.append("$inc", incCount);

		Document setID = tr.RemoveOID();
		change.append("$setOnInsert", new Document("_id",setID));
		
		UpdateOptions uo = new UpdateOptions();
		uo.upsert(true);
		bulkWriter.add(new UpdateManyModel<Document>(query, change, uo));
		testResults.RecordOpsDone("inserts", 1); //Not ideal putting it here
		return tr;
	}
	
	@Override
	public void run() {
		// Use a bulk inserter - even if ony for one
		List<WriteModel<Document>>  bulkWriter;

		try {
			bulkWriter = new ArrayList<WriteModel<Document>>();
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
							+ testOpts.arrayupdates + testOpts.bucketFetchOps;
					int randop = (int) Math.abs(Math.floor(rng.nextDouble()
							* allops));

					if (randop < testOpts.insertops) {
						insertNewRecord(bulkWriter);
						bulkops++;
					} else if (randop < testOpts.insertops
							+ testOpts.keyqueries) {
						simpleKeyQuery();
					}  else if (randop < testOpts.insertops
							+ testOpts.keyqueries + testOpts.rangequeries) {
						rangeQuery();
					} else if (randop < testOpts.bucketFetchOps + testOpts.insertops
							+ testOpts.keyqueries + testOpts.rangequeries) {
						wholeBucketQuery();
					} else if (randop < testOpts.bucketFetchOps + testOpts.insertops
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
						keyStack.add((Document) r.internalDoc.get("_id"));
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

				if (c % testOpts.batchSize == 0) {
					if (bulkops > 0) {
						if( flushBulkOps(bulkWriter) == false)
						{
						// Handle the case where we had a problem
						}
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
			e.printStackTrace();
			return;
		}
	}
}
