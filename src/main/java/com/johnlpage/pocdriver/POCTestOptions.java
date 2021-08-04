package com.johnlpage.pocdriver;


import org.apache.commons.cli.*;


//Yes - lots of public values, getters are OTT here.

public class POCTestOptions {
	int batchSize = 512;
	int numFields = 10;
	int depth = 0;
	final long NUMBER_SIZE = 1000000;
	int textFieldLen = 30;
	int numThreads = 4;
	int threadIdStart = 0;
	int reportTime = 10;	
	int[] slowThresholds = new int[]{50};  // default to 50
	int insertops = 100;
	int opsPerSecond = 0;
	int keyqueries = 0;
	int arrayupdates = 0;
	int updates = 0;
	int rangequeries=0;
	int duration = 18000;
	int numShards = 1;
	String logfile = null;
	boolean sharded = false;
	boolean singleserver = false;
	private String statsfile = "pocload.csv";
	String databaseName = "POCDB";
	String collectionName = "POCCOLL";
	String workflow = null;
	boolean emptyFirst = false;
	boolean printOnly = false;
	int secondaryidx=0;
	int arraytop = 0;
	int arraynext = 0;
	int numcollections = 1;
	int rangeDocs=10;
	int updateFields=1;
	int projectFields=0;
	boolean orderedBatch = true;
	boolean opsratio = false;


	/**
	 * Control whether we show full stacktraces on error
	 */
	boolean debug=false;

	//Zipfian stuff
	boolean zipfian = false;
	int zipfsize = 0;

    int blobSize = 0;

	boolean findandmodify=false;
	int workingset = 100;
	boolean helpOnly = false;
	String connectionDetails = "mongodb://localhost:27017";
	boolean fulltext;
	String[] locationCodes = null;
	String[] defaultLocationCodes = new String[]{"AD","AE","AF","AG","AI","AL","AM","AO","AQ","AR","AS","AT","AU","AU-ACT","AU-NSW","AU-NT","AU-QLD","AU-SA","AU-TAS","AU-VIC","AU-WA","AW","AX","AZ","BA","BB","BD","BE","BE-BRU","BE-VLG","BE-WAL","BF","BG","BH","BI","BJ","BL","BM","BN","BO","BQ","BR","BR-AC","BR-AL","BR-AM","BR-AP","BR-BA","BR-CE","BR-DF","BR-ES","BR-GO","BR-MA","BR-MG","BR-MS","BR-MT","BR-PA","BR-PB","BR-PE","BR-PI","BR-PR","BR-RJ","BR-RN","BR-RO","BR-RR","BR-RS","BR-SC","BR-SE","BR-SP","BR-TO","BS","BT","BV","BW","BY","BZ","CA","CA-AB","CA-BC","CA-MB","CA-NB","CA-NL","CA-NS","CA-NT","CA-NU","CA-ON","CA-PE","CA-QC","CA-SK","CA-YT","CC","CD","CF","CG","CH","CI","CK","CL","CM","CN","CN-11","CN-12","CN-13","CN-14","CN-15","CN-21","CN-22","CN-23","CN-31","CN-32","CN-33","CN-34","CN-35","CN-36","CN-37","CN-41","CN-42","CN-43","CN-44","CN-45","CN-46","CN-50","CN-51","CN-52","CN-53","CN-54","CN-61","CN-62","CN-63","CN-64","CN-65","CO","CR","CU","CV","CW","CX","CY","CZ","DE","DE-BB","DE-BE","DE-BW","DE-BY","DE-HB","DE-HE","DE-HH","DE-MV","DE-NI","DE-NW","DE-RP","DE-SH","DE-SL","DE-SN","DE-ST","DE-TH","DJ","DK","DM","DO","DZ","EC","EE","EG","EH","ER","ES","ET","FI","FJ","FK","FM","FO","FR","FR-ARA","FR-BFC","FR-E","FR-F","FR-GES","FR-H","FR-HDF","FR-J","FR-NAQ","FR-NOR","FR-OCC","FR-R","FR-U","GA","GB","GB-ENG","GB-NIR","GB-SCT","GB-WLS","GD","GE","GF","GG","GH","GI","GL","GM","GN","GP","GQ","GR","GS","GT","GU","GW","GY","HK","HK-KKC","HM","HN","HR","HT","HU","ID","IE","IE-C","IE-L","IE-M","IE-U","IL","IM","IN","IN-AN","IN-AP","IN-AR","IN-AS","IN-BR","IN-CH","IN-CT","IN-DD","IN-DL","IN-DN","IN-GA","IN-GJ","IN-HP","IN-HR","IN-JH","IN-JK","IN-KA","IN-KL","IN-LD","IN-MH","IN-ML","IN-MN","IN-MP","IN-MZ","IN-NL","IN-OR","IN-PB","IN-PY","IN-RJ","IN-SK","IN-TG","IN-TN","IN-TR","IN-UL","IN-UP","IN-WB","IO","IQ","IR","IS","IT","JE","JM","JO","JP","JP-01","JP-02","JP-03","JP-04","JP-05","JP-06","JP-07","JP-08","JP-09","JP-10","JP-11","JP-12","JP-13","JP-14","JP-15","JP-16","JP-17","JP-18","JP-19","JP-20","JP-21","JP-22","JP-23","JP-24","JP-25","JP-26","JP-27","JP-28","JP-29","JP-30","JP-31","JP-32","JP-33","JP-34","JP-35","JP-36","JP-37","JP-38","JP-39","JP-40","JP-41","JP-42","JP-43","JP-44","JP-45","JP-46","JP-47","KE","KG","KH","KI","KM","KN","KP","KR","KR-11","KR-22","KR-26","KR-27","KR-28","KR-29","KR-30","KR-31","KR-41","KR-42","KR-43","KR-44","KR-45","KR-46","KR-47","KR-48","KR-49","KW","KY","KZ","LA","LB","LC","LI","LK","LR","LS","LT","LU","LV","LY","MA","MC","MD","ME","MF","MG","MH","MK","ML","MM","MN","MO","MP","MQ","MR","MS","MT","MU","MV","MW","MX","MY","MZ","NA","NC","NE","NF","NG","NI","NL","NL-DR","NL-FL","NL-FR","NL-GE","NL-GR","NL-LI","NL-NB","NL-NH","NL-OV","NL-UT","NL-ZE","NL-ZH","NO","NP","NR","NU","NZ","OM","PA","PE","PF","PG","PH","PK","PL","PM","PN","PR","PS","PT","PW","PY","QA","RE","RO","RS","RU","RW","SA","SB","SC","SD","SE","SG","SG-01","SG-02","SG-03","SG-04","SG-05","SH","SI","SJ","SK","SL","SM","SN","SO","SR","SS","ST","SV","SX","SY","SZ","TC","TD","TF","TG","TH","TJ","TK","TL","TM","TN","TO","TR","TT","TV","TW","TW-CHA","TW-CYI","TW-CYQ","TW-HSQ","TW-HSZ","TW-HUA","TW-ILA","TW-KEE","TW-KHH","TW-KIN","TW-LIE","TW-MIA","TW-NAN","TW-NWT","TW-PEN","TW-PIF","TW-TAO","TW-TNN","TW-TPE","TW-TTT","TW-TXG","TW-YUN","TZ","UA","UG","UM","US","US-AK","US-AL","US-AR","US-AZ","US-CA","US-CO","US-CT","US-DC","US-DE","US-FL","US-GA","US-HI","US-IA","US-ID","US-IL","US-IN","US-KS","US-KY","US-LA","US-MA","US-MD","US-ME","US-MI","US-MN","US-MO","US-MS","US-MT","US-NC","US-ND","US-NE","US-NH","US-NJ","US-NM","US-NV","US-NY","US-OH","US-OK","US-OR","US-PA","US-RI","US-SC","US-SD","US-TN","US-TX","US-UT","US-VA","US-VT","US-WA","US-WI","US-WV","US-WY","UY","UZ","VA","VC","VE","VG","VI","VN","VU","WF","WS","YE","YT","ZA","ZM","ZW"};
	
	POCTestOptions(String[] args) throws ParseException
	{
		CommandLineParser parser = new DefaultParser();
		
		Options cliopt;
		cliopt = new Options();
		cliopt.addOption("a","arrays",true,"Shape of any arrays in new sample documents x:y so -a 12:60 adds an array of 12 length 60 arrays of integers");
		cliopt.addOption("b","bulksize",true,"Bulk op size (default 512)");
		cliopt.addOption("c","host",true,"MongoDB connection details (default 'mongodb://localhost:27017' )");
		cliopt.addOption("d","duration",true,"Test duration in seconds, default 18,000");
		cliopt.addOption("e","empty",false,"Remove data from collection on startup");
		cliopt.addOption("f","numfields",true,"Number of top level fields in test documents (default 10)");
		cliopt.addOption(null,"depth",true,"The depth of the document created (default 0)");
		cliopt.addOption("g","arrayupdates",true,"Ratio of array increment ops requires option 'a' (default 0)");
		cliopt.addOption("h","help",false,"Show Help");
		cliopt.addOption("i","inserts",true,"Ratio of insert operations (default 100)");
		cliopt.addOption("j","workingset",true,"Percentage of database to be the working set (default 100)");
		cliopt.addOption("k","keyqueries",true,"Ratio of key query operations (default 0)");
		cliopt.addOption("l","textfieldsize",true,"Length of text fields in bytes (default 30)");
		cliopt.addOption("m","findandmodify",false,"Use findAndModify instead of update and retrieve document (with -u or -v only)");
		cliopt.addOption("n","namespace",true,"Namespace to use , for example myDatabase.myCollection");
		cliopt.addOption("o","logfile",true,"Output stats to  <file> ");
		cliopt.addOption("p","print",false,"Print out a sample document according to the other parameters then quit");
		cliopt.addOption("q","opsPerSecond",true,"Try to rate limit the total ops/s to the specified amount");
		cliopt.addOption("r","rangequeries",true,"Ratio of range query operations (default 0)");
		cliopt.addOption("s","slowthreshold",true,"Slow operation threshold in ms, use comma to separate multiple thresholds(default 50)");
		cliopt.addOption("t","threads",true,"Number of threads (default 4)");
		cliopt.addOption("u","updates",true,"Ratio of update operations (default 0)");
		cliopt.addOption("v","workflow",true,"Specify a set of ordered operations per thread from [iukp]");
		cliopt.addOption("w","nosharding",false,"Do not shard the collection");
		cliopt.addOption("x","indexes",true,"Number of secondary indexes - does not remove existing (default 0)");
		cliopt.addOption("y","collections",true,"Number of collections to span the workload over, implies w (default 1)");
		cliopt.addOption("z","zipfian",true,"Enable zipfian distribution over X number of documents (default 0)");
		cliopt.addOption(null,"threadIdStart",true,"Start 'workerId' for each thread. 'w' value in _id. (default 0)");
		cliopt.addOption(null,"fulltext",false,"Create fulltext index (default false)");
		cliopt.addOption(null,"binary",true,"Add a binary blob of size KB");
		cliopt.addOption(null,"rangedocs",true,"Number of documents to fetch for range queries (default 10)");
		cliopt.addOption(null,"updatefields",true,"Number of fields to update (default 1)");
		cliopt.addOption(null,"projectfields",true,"Number of fields to project in finds (default 0, which is no projection)");				
		cliopt.addOption(null,"debug",false,"Show more detail if exceptions occur during inserts/queries");

		cliopt.addOption(null,"ordered",true,"Use ordered or unordered batches");

		cliopt.addOption(null,"opratio",false,"Maintain a strict ratio of number of ops not time - legacy mode");
		cliopt.addOption(null,"location",true,"Adds a location field in the payload ( agrs : comma,seperated,list,of,country,code,)");


		CommandLine cmd = parser.parse(cliopt, args);
		

		if(cmd.hasOption("opsratio"))
		{
			opsratio = true;
		}

		if(cmd.hasOption("binary"))
		{
			blobSize = Integer.parseInt(cmd.getOptionValue("binary"));
		}
		
		if(cmd.hasOption("q"))
		{
			opsPerSecond = Integer.parseInt(cmd.getOptionValue("q"));
		}
		
		if(cmd.hasOption("j"))
		{
			workingset = Integer.parseInt(cmd.getOptionValue("j"));
		}
		
		if(cmd.hasOption("v"))
		{
			workflow = cmd.getOptionValue("v");
		}	
		
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
		
		if(cmd.hasOption("a"))
		{
			String ao = cmd.getOptionValue("a");
			String[] parts = ao.split(":");
			if(parts.length != 2)
			{
				System.err.println("array format is 'top:second'");
				System.exit(1);
			}
			arraytop = Integer.parseInt(parts[0]);
			arraynext = Integer.parseInt(parts[1]);
		}
		
		if(cmd.hasOption("e"))
		{
			emptyFirst=true;
		}
		
		
		if(cmd.hasOption("p"))
		{
			printOnly=true;
		}
		
		if(cmd.hasOption("w"))
		{
			singleserver=true;
		}
		if(cmd.hasOption("r"))
		{
			rangequeries = Integer.parseInt(cmd.getOptionValue("r"));
		}
		
		if(cmd.hasOption("d"))
		{
			duration = Integer.parseInt(cmd.getOptionValue("d"));
		}
		
		if(cmd.hasOption("g"))
		{
			arrayupdates = Integer.parseInt(cmd.getOptionValue("g"));
		}
		
		if(cmd.hasOption("u"))
		{
			updates = Integer.parseInt(cmd.getOptionValue("u"));
		}
		
		if(cmd.hasOption("i"))
		{
			insertops = Integer.parseInt(cmd.getOptionValue("i"));
		}
		
		if(cmd.hasOption("x"))
		{
			secondaryidx = Integer.parseInt(cmd.getOptionValue("x"));
		}
		if(cmd.hasOption("y"))
		{
			numcollections = Integer.parseInt(cmd.getOptionValue("y"));
			singleserver=true;
		}
		if(cmd.hasOption("z"))
		{
			zipfian = true;
			zipfsize = Integer.parseInt(cmd.getOptionValue("z"));
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
			String[] strs = cmd.getOptionValue("s").split(",");
			slowThresholds = new int[strs.length];
			for(int i=0;i<strs.length;i++){
				slowThresholds[i] = Integer.parseInt( strs[i]);
			}			
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
		
		if(cmd.hasOption("m"))
		{
			findandmodify = true;
		}
		
		if(cmd.hasOption("f"))
		{
			numFields = Integer.parseInt(cmd.getOptionValue("f"));
		}

		if(cmd.hasOption("depth"))
		{
			depth = Integer.parseInt(cmd.getOptionValue("depth"));
		}

		if(cmd.hasOption("o"))
		{
			statsfile = cmd.getOptionValue("o");
		}
		
		if(cmd.hasOption("t"))
		{
			numThreads = Integer.parseInt(cmd.getOptionValue("t"));
		}
		if(cmd.hasOption("fulltext"))
        {
            fulltext = true;
        }
		
		if(cmd.hasOption("threadIdStart"))
		{
			threadIdStart = Integer.parseInt(cmd.getOptionValue("threadIdStart"));
		}

		if(cmd.hasOption("rangedocs"))
		{
			rangeDocs = Integer.parseInt(cmd.getOptionValue("rangedocs"));
		}

		if(cmd.hasOption("updatefields"))
		{
			updateFields = Integer.parseInt(cmd.getOptionValue("updatefields"));
		}

		if(cmd.hasOption("projectfields"))
		{
			projectFields = Integer.parseInt(cmd.getOptionValue("projectfields"));
		}

		if(cmd.hasOption("debug"))
		{
			debug = true;
		}

		if(cmd.hasOption("ordered"))
		{
			orderedBatch = Boolean.parseBoolean(cmd.getOptionValue("ordered"));
    }

		if(cmd.hasOption("location")){
			String areaCodesInString = cmd.getOptionValue("location");
			if(areaCodesInString.equalsIgnoreCase("random")){
				locationCodes = defaultLocationCodes;
			} else if (areaCodesInString != null){
				locationCodes = areaCodesInString.split(",");
			} else {
				locationCodes = null;
			}

		}
	}
}
