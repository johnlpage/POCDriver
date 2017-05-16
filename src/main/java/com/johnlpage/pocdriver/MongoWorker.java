package com.johnlpage.pocdriver;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.math3.distribution.ZipfDistribution;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.WriteModel;

import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.*;

public class MongoWorker implements Runnable {

	MongoClient mongoClient;
	MongoDatabase db;
	MongoCollection<Document>  coll;
	ArrayList<MongoCollection<Document>> colls;
	POCTestOptions testOpts;
	POCTestResults testResults;
	int workerID;
	int sequence;
	int numShards = 0;
	Random rng;
	ZipfDistribution zipf;
	boolean workflowed = false;
	boolean zipfian = false;
	String workflow;
	int workflowStep = 0;
	ArrayList<Document> keyStack;
	int lastCollection;
	int currCollection;
	int maxCollections;
	int curCollections;
	String baseCollectionName;
	int incrementRate = 0;
	int incrementIntvl = 0;
	Date lastIncTime = new Date();
	int collectionKeyRange = 0;
	ArrayList<Integer> collectionHash;

	public void ReviewShards() {
		//System.out.println("Reviewing chunk distribution");
		if (testOpts.sharded && !testOpts.singleserver) {
			// I'd like to pick a shard and write there - it's going to be
			// faster and
			// We can ensure we distribute our workers over out shards
			// So we will tell mongo that's where we want our records to go
			//System.out.println("Sharded and not a single server");
			MongoDatabase admindb = mongoClient.getDatabase("admin");
			Boolean split = false;
			
			while (split == false) {
			
				try {
				Document cr;
		//		System.out.println("Splitting a chunk");
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
						if(e.getMessage().contains("could not aquire collection lock") == false )
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
			//System.out.println("Getting shard name");
			String shardName = new String("");
			while (shardlist.hasNext()) {
				Document obj = shardlist.next();
				
				shardName = obj.getString("_id");
				//System.out.println(shardName);
			}
		
			boolean move = false;
			while (move == false) {
				Document cr = null;
				try
				{
				cr = admindb.runCommand(new Document("moveChunk",
						testOpts.databaseName + "." + testOpts.collectionName)
						.append("find",
								new Document("_id", new Document("w",
										workerID).append("i", sequence + 1)))
						.append("to", shardName));
						move=true;
				} catch(Exception e){
					System.out.println(e.getMessage());
					if(e.getMessage().contains("that chunk is already on that shard"))
					{
						move = true;
					} else {
						if(e.getMessage().contains("could not aquire collection lock") == false )
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
		
		//Ping
		c.getDatabase("admin").runCommand(new Document("ping",1));
		testOpts = t;
		testResults = r;
		workerID = id;
		db = mongoClient.getDatabase(testOpts.databaseName);
		maxCollections = testOpts.numcollections;
		curCollections = t.incrementRate;
		testResults.SetCollectionsNum(curCollections);
		baseCollectionName = testOpts.collectionName;
		incrementRate = t.incrementRate;
		incrementIntvl = t.incrementIntvl;
		collectionKeyRange = t.coll_key_range;

		if (maxCollections > 1) {
			colls = new ArrayList<MongoCollection<Document>>();
			lastCollection = 0;
			currCollection = 0;
			for (int i = 0; i < maxCollections; i++) {
				StringBuilder str = new StringBuilder(0);
				str.append(baseCollectionName);
				str.append(i);
				colls.add(db.getCollection(str.toString()));
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

		if (collectionKeyRange > 0) {
			collectionHash = new ArrayList<Integer>();
			for(int i = 0; i < maxCollections; i++) {
				collectionHash.add(0);
			}
		}

	}
	private int getNextSequenceNum(int mult) {
		int rval = 0;
                rval = (int) Math.abs(Math.floor(rng.nextDouble() * mult));
                return rval;
        }

	private int getNextVal(int mult) {
		int rval = 0;
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
		rotateCollection();
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
				//We had a problem with this bulk op - some may be completed, some may not
				
				//I need to resubmit it here
				String error = e.getMessage();
				
				
				//Check if it's a sup key and remove it
				Pattern p = Pattern.compile("dup key: \\{ : \\{ w: (.*?), i: (.*?) \\}");
			//	Pattern p = Pattern.compile("dup key");
				
				Matcher m = p.matcher(error);
				if(m.find())
				{
					//System.out.println("Duplicate Key");
					//int thread = Integer.parseInt(m.group(1));
					int uniqid = Integer.parseInt(m.group(2));
					//System.out.println(" ID = " + thread + " " + uniqid );
					boolean found=false;
					for ( Iterator<? super WriteModel<Document>> iter = bulkWriter.listIterator(); iter.hasNext(); ) {
						//Check if it's a InsertOneModel
						
						Object o = iter.next();	
						if(o instanceof InsertOneModel<?>)
						{
							@SuppressWarnings("unchecked")
							InsertOneModel<Document> a = (InsertOneModel<Document>) o;
							Document id = (Document) a.getDocument().get("_id");
						
							//int opthread=id.getInteger("w");
							//int opid = id.getInteger("i");
							//System.out.println("opthread: " + opthread + "=" + thread + " opid: " + opid + "=" + uniqid);
						    if ( id.getInteger("i")==uniqid) {
						    	//System.out.println(" Removing " + thread + " " + uniqid + " from bulkop as already inserted");
						        iter.remove();
						        found=true;
						    }
						}
					}
					if(found == false)
					{
						System.out.println("Cannot find failed op in batch!");
					}
				} else {
					System.out.println(error);
				}
				if(bwResult != null)
				{
					//System.out.println("Resubmitting");
					System.out.println(bwResult.toString());
					
					submitted = false;
				} else {
					//System.out.println("No result returned");
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
		testResults.RecordLatency("inserts", taken);
		return true;
		
	}

	
	
	private Document simpleKeyQuery() {
		// Key Query
		rotateCollection();
		Document query = new Document();
		int range = sequence * testOpts.workingset / 100;
		int rest = sequence - range;

		int recordno = rest + getNextVal(range);

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
			testResults.RecordLatency("keyqueries", taken);
			testResults.RecordOpsDone("keyqueries", 1);
		}
		return (Document) myDoc;
	}
	


	private void rangeQuery() {
		// Key Query
		rotateCollection();
		Document query = new Document();
		int recordno =  getNextVal(sequence);
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
		testResults.RecordLatency("rangequeries", taken);
		testResults.RecordOpsDone("rangequeries", 1);

	}

	private void rotateCollection() {
		if (maxCollections > 1) {
			if(incrementIntvl > 0 && curCollections < maxCollections) {
				Date now = new Date();
				int secondsSinceLastCheck = (int)(now.getTime() - lastIncTime.getTime()) / 1000;
				if (secondsSinceLastCheck >= incrementIntvl) {
					curCollections += incrementRate;
					if (curCollections > maxCollections) {
						curCollections = maxCollections;
					}
					//System.out.println(String.format("its been %d seconds, maxcoll is now %d", secondsSinceLastCheck, curCollections));
					lastIncTime = now;
					testResults.SetCollectionsNum(curCollections);
				}
			}
			coll = colls.get(lastCollection);
			currCollection = lastCollection;
			lastCollection = (lastCollection + 1) % curCollections;
		}
	}	

	private void updateSingleRecord(List<WriteModel<Document>>  bulkWriter) {
		updateSingleRecord(bulkWriter, null);
	}

	private void updateSingleRecord(List<WriteModel<Document>>  bulkWriter,
			Document key) {
		// Key Query
		rotateCollection();
		Date starttime = new Date();
		Document query = new Document();
		long changedfield = (long) getNextVal((int) testOpts.NUMBER_SIZE);

		if (key == null) {
			int range = sequence * testOpts.workingset / 100;
			int rest = sequence - range;

			int recordno = rest + getNextVal(range);

			query.append("_id",
					new Document("w", workerID).append("i", recordno));
		} else {
			query.append("_id", key);
		}

		if (collectionKeyRange > 0) {
			query.remove("_id");
			int val = collectionHash.get(currCollection);
			query.append("_id", val);
			collectionHash.set(currCollection, ++val);
			
		}
	
		Document fields = new Document("fld0", changedfield);
		Document change = new Document("$set", fields);

		if (testOpts.findandmodify == false) {
			bulkWriter.add(new UpdateManyModel<Document>(query,change));
		} else {
			this.coll.findOneAndUpdate(query, change); //These are immediate not batches
		}
		Date endtime = new Date();
		Long taken = endtime.getTime() - starttime.getTime();
		testResults.RecordOpsDone("updates", 1);
		testResults.RecordLatency("updates", taken);

	}

	private TestRecord insertNewRecord(List<WriteModel<Document>>  bulkWriter) {
		TestRecord tr;
		int[] arr = new int[2];
		arr[0] = testOpts.arraytop;
		arr[1] = testOpts.arraynext;
		tr = new TestRecord(testOpts.numFields, testOpts.textFieldLen,
				workerID, sequence++, testOpts.NUMBER_SIZE, testOpts.numShards,
				arr,testOpts.blobSize);

		bulkWriter.add(new InsertOneModel<Document>(tr.internalDoc));
		return tr;
	}


	
	public void run() {
		// Use a bulk inserter - even if ony for one
		List<WriteModel<Document>>  bulkWriter;

		try {
			bulkWriter = new ArrayList<WriteModel<Document>>();
			int bulkops = 0;

			
			int c = 0;
			
			while (testResults.GetSecondsElapsed() < testOpts.duration) {
				c++;
				//Timer isn't granullar enough to sleep for each
				if(  testOpts.opsPerSecond > 0) {
					double threads = testOpts.numThreads;
					double opsperthreadsecond = testOpts.opsPerSecond / threads;
					double sleeptimems = 1000 / opsperthreadsecond;
					
					if(c==1){
						//First time randomise
			
						Random r = new Random();
						sleeptimems = r.nextInt((int)Math.floor(sleeptimems));
					
					}
					Thread.sleep((int)Math.floor(sleeptimems));
				}
				if (workflowed == false) {
					// System.out.println("Random op");
					// Choose the type of op
					int allops = testOpts.insertops + testOpts.keyqueries
							+ testOpts.updates + testOpts.rangequeries
							+ testOpts.arrayupdates;
					int randop = getNextSequenceNum(allops);

					if (randop < testOpts.insertops) {
						insertNewRecord(bulkWriter);
						bulkops++;
					} else if (randop < testOpts.insertops
							+ testOpts.keyqueries) {
						simpleKeyQuery();
					}  else if (randop < testOpts.insertops
							+ testOpts.keyqueries + testOpts.rangequeries) {
						rangeQuery();
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
