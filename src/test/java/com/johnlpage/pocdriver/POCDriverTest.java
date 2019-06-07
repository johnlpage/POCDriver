package com.johnlpage.pocdriver;

import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.junit.*;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class POCDriverTest {

    @Test
    public void testInsertOpLimit() {
    	POCDriver classUnserTest = new POCDriver();
    	classUnserTest.main(new String[] {"-insertOpLimit", "10001"});    	
    }
    
    @Test
    public void testReadOpLimit() {
    	POCDriver classUnserTest = new POCDriver();
    	classUnserTest.main(new String[] {"-readOpLimit", "10001","-i","0","-r","100"});    	
    }
    
    @Test
    public void testUpdateOpLimit() {
    	POCDriver classUnserTest = new POCDriver();
    	classUnserTest.main(new String[] {"-updateOpLimit", "10001", "-i","0","-u","100"});    	
    }

 }
