import org.apache.commons.cli.ParseException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class POCDriver {

	public static void main(String[] args) {
		
		POCTestOptions testOpts;
		POCTestResults testResults = new POCTestResults();
		
		System.out.println("MongoDB Proof Of Concept - Load Generator");
		try {
			testOpts = new POCTestOptions(args);
			// Quit after displaying help message
			if (testOpts.helpOnly) {
				return;
			}
			
			if(testOpts.arrayupdates > 0 && (testOpts.arraytop<1 || testOpts.arraynext<1))
			{
				System.out.println("You must specify and array size to update arrays");
				return;
			}
			if(testOpts.printOnly)
			{
				TestRecord tr;
				int[] arr = new int[2];
				arr[0] = testOpts.arraytop;
				arr[1] = testOpts.arraynext;
				tr = new TestRecord(testOpts.numFields, testOpts.textFieldLen,
						1, 12345678,testOpts.NUMBER_SIZE,testOpts.numShards,arr);
				//System.out.println(tr);
				Gson gson = new GsonBuilder().setPrettyPrinting().create();
				JsonParser jp = new JsonParser();
				JsonElement je = jp.parse(tr.toString());
				//TODO Collapse inner newlines
				
				System.out.println(gson.toJson(je));
			 return;
			}
			
		} catch (ParseException e) {

			System.err.println(e.getMessage());
			return;
		}


		LoadRunner runner = new LoadRunner(testOpts);
		runner.RunLoad(testOpts,testResults);

	}

}
