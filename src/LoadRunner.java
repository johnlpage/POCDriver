import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class LoadRunner {

	MongoClient mongoClient;
	

	private void PrepareSystem(POCTestOptions testOpts)
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
	}
	
	public void RunLoad(POCTestOptions testOpts, POCTestResults testResults) {

		PrepareSystem(testOpts);
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
			boolean b = testexec.awaitTermination(Long.MAX_VALUE,
					TimeUnit.SECONDS);
			System.out.println("All Threads Complete: " + b);
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
