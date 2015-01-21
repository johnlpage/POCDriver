import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class POCTestResults {
	Date	startTime;
	Date	lastInsertIntervalDate;
	Long lastInsertIntervalCount;
	
	private AtomicLong insertsDone;
	private AtomicLong slowOps;
	
	POCTestResults()
	{
		startTime = new Date();
		lastInsertIntervalCount = new Long(0);
		insertsDone = new AtomicLong(0);
		slowOps = new AtomicLong(0);
		lastInsertIntervalDate = new Date();
	}
	
	//This returns inserts per second since we last called it
	//Rather than us keeping an overall figure
	
	Long GetInsertsPerSecondLastInterval()
	{
		Long intervalInsertsPerSecond;
	

		Date now = new Date();
		Long milliSecondsSinceLastCheck =  now.getTime()-lastInsertIntervalDate.getTime();
		Long recordsNow = GetInsertsDone();
		
		intervalInsertsPerSecond = ((recordsNow - lastInsertIntervalCount) * 1000) / milliSecondsSinceLastCheck;
	
		lastInsertIntervalDate = now;
		lastInsertIntervalCount = recordsNow;
		
		return intervalInsertsPerSecond;
	}
	
	public void SetStartTime(Date when)
	{
		startTime = when;
	}
	
	public Long GetSecondsElapsed()
	{
		Date now = new Date();
		return (now.getTime()-startTime.getTime())/1000;
	}
	
	public Long GetInsertsDone()
	{
		return insertsDone.get();
	}
	
	public Long GetSlowOps()
	{
		return slowOps.get();
	}
	
	public void RecordSlowOperation()
	{
		slowOps.incrementAndGet();
	}
	
	public void RecordInserts(Long howmany)
	{
		insertsDone.addAndGet(howmany);
	}
}
