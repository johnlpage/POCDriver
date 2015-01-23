import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


public class POCTestOptions {
	int batchSize = 1;
	int numFields = 10;
	final long NUMBER_SIZE = 1000000;
	int textFieldLen = 30;
	int numThreads = 16;
	int reportTime = 10;
	int slowThreshold = 50;
	boolean logstats = false;
	int insertops = 100;
	int keyqueries = 0;
	int updates = 0;
	int rangequeries=0;
	int duration = 18000;
	String logfile = null;
	
	String statsfile = "pocload.csv";
	String databaseName = "POCDB";
	String collectionName = "POCCOLL";
	boolean emptyFirst = false;
	int secondaryidx=0;
	
	boolean helpOnly = false;
	String connectionDetails = "mongodb://localhost:27017";
	
	public POCTestOptions(String[] args) throws ParseException
	{
		CommandLineParser parser = new GnuParser();
		
		Options cliopt;
		cliopt = new Options();
		cliopt.addOption("c","host",true,"Mongodb connection details (default 'mongodb://localhost:27017' )");
		cliopt.addOption("e",false,"Remove data from collection on startup");
		cliopt.addOption("d","duration",true,"Test duration in seconds, default 18,000");
		cliopt.addOption("t","threads",true,"Number of threads (default 32)");
		cliopt.addOption("o","logfile",true,"Output stats to  <file> ");
		cliopt.addOption("b","bulksize",true,"Bulk op size (default 512)");
		cliopt.addOption("s","slowthreshold",true,"Slow operation threshold (default 500)");
		cliopt.addOption("h","help",false,"Show Help");
		cliopt.addOption("n","namespace",true,"Namespace to use , for example myDatabase.myCollection");
		cliopt.addOption("f","numfields",true,"Number of fields in test records (default 10)");
		cliopt.addOption("l","textfieldsize",true,"Length of text fields in bytes (default 30)");
		cliopt.addOption("i","insert",true,"Ratio of insert operations (default 100)");
		cliopt.addOption("k","keyquery",true,"Ratio of key query operations (default 100)");
		cliopt.addOption("u","update",true,"Ratio of update operations (default 100)");
		cliopt.addOption("r","rangequery",true,"Ratio of range query operations (default 100)");
		cliopt.addOption("x","indexes",true,"Number of secondary indexes (default 0)");
		CommandLine cmd = parser.parse(cliopt, args);
		if(cmd.hasOption("n"))
		{
			String ns = cmd.getOptionValue("n");
			String[] parts = ns.split("\\.");
			if(parts.length != 2)
			{
				System.err.println("namespace format is 'DATABASE.COLLECTION' ");
				System.exit(1);
			}
			databaseName=parts[0];
			collectionName=parts[1];
		}
		
		if(cmd.hasOption("e"))
		{
			emptyFirst=true;
		}
		
		if(cmd.hasOption("r"))
		{
			rangequeries = Integer.parseInt(cmd.getOptionValue("r"));
		}
		
		if(cmd.hasOption("d"))
		{
			duration = Integer.parseInt(cmd.getOptionValue("d"));
		}
		
		if(cmd.hasOption("i"))
		{
			insertops = Integer.parseInt(cmd.getOptionValue("i"));
		}
		
		if(cmd.hasOption("x"))
		{
			secondaryidx = Integer.parseInt(cmd.getOptionValue("x"));
		}
		
		if(cmd.hasOption("o"))
		{
			logfile = cmd.getOptionValue("o");
		}
		
		if(cmd.hasOption("k"))
		{
			keyqueries = Integer.parseInt(cmd.getOptionValue("k"));
		}
		
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
		
		if(cmd.hasOption("c"))
		{
			connectionDetails = cmd.getOptionValue("c");
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
