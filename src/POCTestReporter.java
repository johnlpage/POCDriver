import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;



public class POCTestReporter implements Runnable {
	POCTestResults testResults;
	MongoClient mongoClient;
	POCTestOptions testOpts;
	
	POCTestReporter(POCTestResults r, MongoClient mc,POCTestOptions t)
	{
		mongoClient = mc;
		testResults = r;
		testOpts = t;
	}
	
    public void run() {
    	Long insertsDone = testResults.GetInsertsDone();
    	if (testResults.GetSecondsElapsed() < testOpts.reportTime) return;
    	System.out.println("------------------------");
        System.out.format("After %d seconds, %d records inserted\n",testResults.GetSecondsElapsed(),insertsDone);
        System.out.format("%d loading per second since last report\n",testResults.GetInsertsPerSecondLastInterval());
        
        if(insertsDone>0)
        {
        	Double fastops = 100 -(testResults.GetSlowOps() * 100.0 ) / insertsDone;	
        System.out.format("%.2f %% in under %d milliseconds,",fastops,testOpts.slowThreshold);
        
        } else {
        	 System.out.format("%d,",0);
        }
        //We've reported on what the test has done but we should also report on
        //what's in the database
       
        /*
         * This blocks when thread count is high as it's a read queud behind thousands
         * of writes so Im removeing for now
         * 
        DB db;
    	DBCollection coll;
        db = mongoClient.getDB(testOpts.databaseName);
		coll = db.getCollection(testOpts.collectionName);
		Long count = coll.count();
		System.out.format("%d\n",count);*/
        System.out.println();
     }
  }