import java.util.Date;
import java.util.Random;

import com.mongodb.BasicDBObject;


//A Test Record is a MongoDB Record Object that is self populating
@SuppressWarnings("serial")
public class TestRecord extends BasicDBObject {

	Random rng;
	final double VOCAB_SIZE = 100000;

	
	private String CreateString( int length , int fieldNo)
	{
		//We create a string by picking a number
		//Hashing it into a string
		//We use a gaussian distribution for the numbers
		StringBuilder sb = new StringBuilder();
		
		while(sb.length() < length)
		{
			long r = (long) Math.abs(Math.floor(rng.nextGaussian() * VOCAB_SIZE));
			if ( r % 5 != 0){
				sb.append("word" + r+ " ");
			} else {
				sb.append(r + " ");
				}
		}
		return sb.toString();
	}
	
	
	//This needs to be clever as we really need to be able to 
	//Say - assuming nothin was removed - what is already in the DB
	//Therefore we will have a one-up per thread
	//A thread starting will find out what it's highest was
	
	public void AddOID(int workerid, int sequence)
	{
		BasicDBObject oid = new BasicDBObject();
		oid.append("w",workerid);
		oid.append("i",sequence);
		this.append("_id", oid);
	}
	
	
	public TestRecord( int nFields , int fieldSize, int workerID, int sequence, long numberSize)
	{
		rng = new Random();
		int fieldNo;
		//Always a field 0
		for(fieldNo=0;(fieldNo<nFields || fieldNo==0);fieldNo++)
		{
			if(fieldNo % 3 == 0)
			{
				//Field should always be a long this way
				long r = (long) Math.abs(Math.floor(rng.nextGaussian() * numberSize));
				this.append("fld"+fieldNo, r);
			} else
				if (fieldNo % 5 == 0)
				{
					long r = (long) Math.abs(Math.floor(rng.nextGaussian() * Long.MAX_VALUE));
					Date now = new Date(r);
					this.append("fld"+fieldNo, now);
				}
				else {
					//put in a string
					String fieldContent = CreateString( fieldSize , fieldNo );
					this.append("fld"+fieldNo, fieldContent);
				}
		}
		AddOID( workerID,sequence);
	}
}
