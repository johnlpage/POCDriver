import org.apache.commons.cli.ParseException;
import org.bson.BSON;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.mongodb.util.JSON;

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
			//Sets up sample data dont remove
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
				String json = gson.toJson(je);
				StringBuilder newJson = new StringBuilder();
				int arrays = 0;
				boolean inquotes = false;
				for(int c=0;c<json.length();c++)
				{
					char inChar = json.charAt(c);
					if(inChar == '[') { arrays++; }
					if(inChar == ']') { arrays--; }
					if(inChar == '"') {inquotes = !inquotes; }
				
					if(arrays >1 && inChar == '\n')
					{
						continue;
					}
					if(arrays > 1 && !inquotes && inChar == ' ' ) { continue;}
					newJson.append(json.charAt(c));
				}
				if(testOpts.printOnly)
				{
				System.out.println(newJson.toString());
				byte[] b = BSON.encode(tr);
				System.out.println(String.format("Records are %.2f KB each as BSON",
						(float)new Float(b.length) / 1024,2));
				
				
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
