import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


public class POCTestOptions {
	int batchSize = 1;
	int numFields = 10;
	int textFieldLen = 30;
	int numThreads = 16;
	int reportTime = 10;
	int slowThreshold = 500;
	boolean logstats = false;
	
	String statsfile = "pocload.csv";
	String databaseName = "POCDB";
	String collectionName = "POCCOLL";
	
	boolean helpOnly = false;
	String connectionDetails = "mongodb://localhost:27017";
	
	public POCTestOptions(String[] args) throws ParseException
	{
		CommandLineParser parser = new GnuParser();
		
		Options cliopt;
		cliopt = new Options();
		cliopt.addOption("u",true,"Mongodb connection details (default 'mongodb://localhost:27017' )");
		cliopt.addOption("t",true,"Number of threads (default 32)");
		cliopt.addOption("o",true,"Output stats to  <file> (default pocload.csv )");
		cliopt.addOption("b",true,"Bulk op size (default 512)");
		cliopt.addOption("s",true,"Slow operation threshold (default 500)");
		cliopt.addOption("h",false,"Show Help");
		cliopt.addOption("f",true,"Number of fields in test records (default 10)");
		cliopt.addOption("l",true,"Length of text fields in bytes (default 30)");
		cliopt.addOption("i",true,"Ratio of insert operations (default 100)");
		cliopt.addOption("k",true,"Ratio of key query operations (default 100)");
		
		CommandLine cmd = parser.parse(cliopt, args);
	
		if(cmd.hasOption("b"))
		{
			batchSize = Integer.parseInt(cmd.getOptionValue("b"));
		}
		
		if(cmd.hasOption("s"))
		{
			slowThreshold = Integer.parseInt(cmd.getOptionValue("s"));
		}
		
		// automatically generate the help statement
		if(cmd.hasOption("h"))
		{
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "POCDriver", cliopt );
			helpOnly = true;
		}
		
		if(cmd.hasOption("u"))
		{
			connectionDetails = cmd.getOptionValue("u");
		}	
			
		if(cmd.hasOption("l"))
		{
			textFieldLen = Integer.parseInt(cmd.getOptionValue("l"));
		}
		
		if(cmd.hasOption("f"))
		{
			numFields = Integer.parseInt(cmd.getOptionValue("f"));
		}

		if(cmd.hasOption("o"))
		{
			statsfile = cmd.getOptionValue("o");
			logstats = true;
		}
		
		if(cmd.hasOption("t"))
		{
			numThreads = Integer.parseInt(cmd.getOptionValue("t"));
		}
		
	}
}
