import org.apache.commons.cli.ParseException;

public class POCDriver {

	public static void main(String[] args) {
		
		POCTestOptions testOpts;
		POCTestResults testResults = new POCTestResults();
		
		System.out.println("MongoDB Proof Of Concept - Load Generator");
		try {
			testOpts = new POCTestOptions(args);
			// Quit after displayign help message
			if (testOpts.helpOnly) {
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
