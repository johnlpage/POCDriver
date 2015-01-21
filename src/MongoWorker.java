

import java.util.Date;

import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;


public class MongoWorker implements Runnable {

	MongoClient mongoClient;
	DB db;
	DBCollection coll;
	POCTestOptions testOpts;
	POCTestResults testResults;
	
	public MongoWorker(MongoClient c, POCTestOptions t, POCTestResults r) {
		mongoClient = c;
		testOpts = t;
		testResults = r;
		db = mongoClient.getDB(testOpts.databaseName);
		coll = db.getCollection(testOpts.collectionName);
		
	}
	
	@Override
	public void run() {
		//Use a bulk inserter - even if ony for one
		BulkWriteOperation bulkWriter;
		
		try
		{
		bulkWriter = coll.initializeUnorderedBulkOperation();
		for(int c=0;c<100000000;c++)
		{
			TestRecord tr;
			tr = new TestRecord(testOpts.numFields,testOpts.textFieldLen);
			bulkWriter.insert(tr);
			if(c % testOpts.batchSize == 0)
			{
				//Time this.
				Date starttime = new Date();
			
				BulkWriteResult bwResult = bulkWriter.execute();
				
				Date endtime = new Date();
				
				Long taken = endtime.getTime() - starttime.getTime();
				if(taken > testOpts.slowThreshold)
				{
					testResults.RecordSlowOperation();
				}
				Long icount = 
						(long) bwResult.getInsertedCount();
				testResults.RecordInserts(icount);
				bulkWriter = coll.initializeUnorderedBulkOperation();
			}
		
		}
		bulkWriter.execute();
		}
		catch (Exception e)
		{
			System.out.println(e.getMessage());
			return;
		}
	}

}
