package com.johnlpage.pocdriver;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;



public class POCTestReporter implements Runnable {
	POCTestResults testResults;
	MongoClient mongoClient;
	POCTestOptions testOpts;

	POCTestReporter(POCTestResults r, MongoClient mc, POCTestOptions t) {
		mongoClient = mc;
		testResults = r;
		testOpts = t;
		printHeader();
	}

	private void printHeader()
	{
		PrintWriter outfile = null;
		String[] opTypes = POCTestResults.opTypes;
		try {
			outfile = new PrintWriter(new BufferedWriter(new FileWriter(testOpts.logfile, true)));
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
		outfile.print("system_clock,relative_clock,num_collections,total_inserts");
		for (String o: opTypes) {
			outfile.format(",%s,slowops_%s",o,o);
		}
		outfile.println();
		outfile.close();
	}

	private void logData()
	{
		PrintWriter outfile = null;
		StringBuilder outtext = new StringBuilder();
		
		if(testOpts.logfile != null)
		{
			
			try {
			     outfile = new PrintWriter(new BufferedWriter(new FileWriter(testOpts.logfile, true)));
			} catch (IOException e) {
			   System.out.println(e.getMessage());
			}
		}
		

		Long insertsDone = testResults.GetOpsDone("inserts");
		int numCollections = testResults.GetCollectionsNum();
		if (testResults.GetSecondsElapsed() < testOpts.reportTime)
			return;
		outtext.append(String.format("------%d------\n", System.currentTimeMillis()));
		if (testOpts.sharded && !testOpts.singleserver)
		{
			MongoDatabase configdb = mongoClient.getDatabase("config");
			MongoCollection<Document>  shards = configdb.getCollection("shards");
			int numShards = (int)shards.count();
			testOpts.numShards = numShards;
		}
		if (numCollections > 1) {
			outtext.append(String.format("Currently we are using %d collections\n", numCollections));
		}

		outtext.append(String.format("After %d seconds, %d new records inserted - collection has %d in total \n",
				testResults.GetSecondsElapsed(), insertsDone, testResults.initialCount + insertsDone));
		
		if(outfile != null)
		{
			outfile.format("%d,%d,%d,%d", System.currentTimeMillis() ,testResults.GetSecondsElapsed(), numCollections, insertsDone);
		}
		
		
		HashMap<String, Long> results = testResults.GetOpsPerSecondLastInterval();
		String[] opTypes = POCTestResults.opTypes;
		for (String o: opTypes)
		{
			outtext.append(String.format("%d %s per second since last report ", results.get(o), o));
			
			if(outfile != null)
			{
				outfile.format(",%d",results.get(o));
			}
			
			
			Long opsDone = testResults.GetOpsDone(o);
			if (opsDone > 0) {
				Long slowOps = testResults.GetSlowOps(o);
				Double fastops = 100 - (slowOps * 100.0)
						/ opsDone;
				outtext.append(String.format("%.2f %% in under %d milliseconds", fastops,
						testOpts.slowThreshold));
				if(outfile != null)
				{
					outfile.format(",%d", slowOps);
				}
			} else {
				outtext.append(String.format("%.2f %% in under %d milliseconds",(float)100,testOpts.slowThreshold));
				if(outfile != null)
				{
					outfile.format(",%d", 0);
				}
			}
			outtext.append("\n");
		
		}
		if(outfile != null)
		{
			outfile.println();
			outfile.close();
		}
		System.out.println(outtext);
	}

	public void run() {

		logData();
	
	}
}
