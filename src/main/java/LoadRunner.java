import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;


public class LoadRunner {

	MongoClient mongoClient;
	

	private void PrepareSystem(POCTestOptions testOpts,POCTestResults results)
	{
		DB db;
		DBCollection coll;
		//Create indexes and suchlike
		db = mongoClient.getDB(testOpts.databaseName);
		coll = db.getCollection(testOpts.collectionName);
		if(testOpts.emptyFirst)
		{
			coll.drop();
		}
		
		for(int x=0;x<testOpts.secondaryidx;x++)
		{
			coll.createIndex(new BasicDBObject("fld"+x,1));
		}
		
		results.initialCount = coll.count();
		//Now have a look and see if we are sharded
		//And how many shards and make sure that the collection is sharded
		if(! testOpts.singleserver) {
			ConfigureSharding(testOpts);
		}
		
		
	}
	
	private void ConfigureSharding(POCTestOptions testOpts)
	{
		DB admindb = mongoClient.getDB("admin");
		CommandResult cr = admindb.command("serverStatus");
		if(cr.ok() == false)
		{
			//System.out.println(cr.getErrorMessage());
			return;
		}
		//System.out.println(cr);
		if (cr.get("process").equals("mongos"))
		{
			//System.out.println("Sharded System");
			testOpts.sharded = true;
			//Turn the auto balancer off - good apps rarely need it
			DB configdb = mongoClient.getDB("config");
			if(configdb != null)
			{
				DBCollection settings = configdb.getCollection("settings");
				settings.update(new BasicDBObject("_id","balancer"), new BasicDBObject("$set",new BasicDBObject("stopped",true)),true,false);
				//System.out.println("Balancer disabled");
			}
			cr = admindb.command(new BasicDBObject("enableSharding",testOpts.databaseName));
			if(cr.ok() == false)
			{
				//System.out.println(cr.getErrorMessage());
				//return;
			}
			
			
			cr = admindb.command(new BasicDBObject("shardCollection",
					testOpts.databaseName+"."+testOpts.collectionName).append("key", new BasicDBObject("_id",1)));
			if(cr.ok() == false)
			{
				System.out.println(cr.getErrorMessage());
				//return;
			}
			
			//See how many shards we have in the system - and get a list of their names
			
			DBCollection shards = configdb.getCollection("shards");
			//DBCursor cursor = (DBCursor) shards.find();
			testOpts.numShards = (int)shards.count();
		}
	}
	
	public void RunLoad(POCTestOptions testOpts, POCTestResults testResults) {

		PrepareSystem(testOpts,testResults);
		// Report on progress by looking at testResults
		Runnable reporter = new POCTestReporter(testResults,mongoClient,testOpts);
		ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		executor.scheduleAtFixedRate(reporter, 0, testOpts.reportTime, TimeUnit.SECONDS);

		
		// Using a thread pool we keep filled
		ExecutorService testexec = Executors
				.newFixedThreadPool(testOpts.numThreads);

		for (int i = 0; i < testOpts.numThreads; i++) {
			testexec.execute(new MongoWorker(mongoClient, testOpts, testResults,i));
		}

		testexec.shutdown();
	
		try {
			testexec.awaitTermination(Long.MAX_VALUE,
					TimeUnit.SECONDS);
			//System.out.println("All Threads Complete: " + b);
			executor.shutdown();
		} catch (InterruptedException e) {
			System.out.println(e.getMessage());
			
		}
	}

	public LoadRunner(POCTestOptions testOpts) {
		try {
			mongoClient = new MongoClient(new MongoClientURI(
					testOpts.connectionDetails));
		} catch (UnknownHostException e) {
			// TODO Move this out
			e.printStackTrace();
		}
	}
}
