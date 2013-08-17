package MOSTNeo4jProcedures;


import static org.junit.Assert.assertEquals;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Console;
import java.io.File;
import java.sql.Timestamp;
import java.util.Date;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;

/**
 * @author Milinda Fernando 
 */

public class Neo4jDatabaseProcedureTest {

	private Neo4jDatabaseProcedure db;
	
	@Before
	public void setup() throws Exception  {
	    db=new Neo4jDatabaseProcedure();
        db.SetDatabase("/home/milinda/Desktop/MOSTDB");
        db.CreateDatabase();

		
	}
	@Test
	public void testGetStartNode(){
		
		Node start=db.getStartNode();
		assertEquals(1, start.getId());
		
	}
	
	@Test
	public void testAddZone(){
		
		assertEquals(0, db.getZoneCount());
		boolean state;
		state = db.addZone("Zone_1", "This is a test Zone", "Sri lanka",
				"Colombo", "Sri lanka", "dematagoda", "Bank of Ceylon",
				"1st floor", "234", 234.2, 800.32);
		assertEquals(true, state);
		assertEquals(1, db.getZoneCount());
		
		state = db.addZone("Zone_2", "This is a test Zone", "Sri lanka",
				"Colombo", "Sri lanka", "dematagoda", "Bank of Ceylon",
				"1st floor", "234", 234.2, 800.32);
		assertEquals(true, state);
		assertEquals(2, db.getZoneCount());
		
		
		Node temp = db.getZoneByID(0);
		assertEquals("0",temp.getProperty("idzone").toString());
        
		temp=db.getZoneByID(1);
		assertEquals("1",temp.getProperty("idzone").toString() );
		
		
	}
	
 @Test
	public void testAddDatapoint(){
		
		boolean state=db.addDatapoint("datapoint_1", "Test datapoint", "Celsius",
				0.001, -30.00, 55.230, 323.32, 10.00, 5.00, 12.0, "+ -",
				"virtual", "hello", "Test datapoint");
		assertEquals(true, state);
		state=db.addDatapoint("datapoint_1", "Test datapoint", "Celsius",
				0.001, -30.00, 55.230, 323.32, 10.00, 5.00, 12.0, "+ -",
				"virtual", "hello", "Test datapoint");
		assertEquals(false, state);
		state=db.addDatapoint("datapoint_2", "Test datapoint", "Celsius",
				0.001, -30.00, 55.230, 323.32, 10.00, 5.00, 12.0, "+ -",
				"virtual", "hello", "Test datapoint");
		assertEquals(true, state);
		
		
	}

 
   @Test
	public void testAddDatapointToZone(){
		
		this.testAddZone();
		this.testAddDatapoint();
		boolean state=db.addDatapointToZone("datapoint_1", 0);
		assertEquals(true, state);
		state=db.addDatapointToZone("datapoint_2", 1);
		assertEquals(true, state);
	}
	
	@Test
	public void testGetDatapointByName(){
		
		this.testAddZone();
		this.testAddDatapoint();
		Node datapoint=db.getDatapointByName("datapoint_1");
		assertEquals("datapoint_1", datapoint.getProperty("datapoint_name"));
		db.addDatapointToZone("datapoint_1",0);
		assertEquals("datapoint_1", datapoint.getProperty("datapoint_name"));
		
	}
	@Test
	public void testAddDataForced(){
		
		this.testAddZone();
		this.testAddDatapoint();
		boolean state=db.addDataForced("datapoint_1", "23-07-2013 12:23:45", 25.78);
		assertEquals(true, state);
		
	}
	@Test
	public void testAddData(){
		this.testAddZone();
		this.testAddDatapoint();
	    System.out.println(new Timestamp(new Date().getTime()).toString());
		boolean state=db.addData("datapoint_1",new Timestamp(new Date().getTime()).toString(), 26.34);
		assertEquals(true, state);
	}
	
	
	@After
	public void tearDown()  {
		
		 db.StopDatabase();

	        File file = new File("/home/milinda/Desktop/MOSTDB/index");
	        String[] myFiles;
	        if (file.isDirectory()) {
	            myFiles = file.list();
	            for (int i = 0; i < myFiles.length; i++) {
	                File myFile = new File(file, myFiles[i]);
	                myFile.delete();
	            }
	        }
	        file.delete();

	        file = new File("/home/milinda/Desktop/MOSTDB");

	        if (file.isDirectory()) {
	            myFiles = file.list();
	            for (int i = 0; i < myFiles.length; i++) {
	                File myFile = new File(file, myFiles[i]);
	                myFile.delete();
	            }
	        }



		
	}

	

}
