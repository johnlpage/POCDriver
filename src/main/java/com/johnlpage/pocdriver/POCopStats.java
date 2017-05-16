package com.johnlpage.pocdriver;


import java.util.concurrent.atomic.AtomicLong;


public class POCopStats {
	public AtomicLong	intervalCount;
	public AtomicLong	totalOpsDone;
	public AtomicLong	slowOps;
	public AtomicLong       totalLatency;
	
	POCopStats()
	{
			intervalCount = new AtomicLong(0);
			totalOpsDone= new AtomicLong(0);
			slowOps= new AtomicLong(0);
			totalLatency =  new AtomicLong(0);
	}
}
