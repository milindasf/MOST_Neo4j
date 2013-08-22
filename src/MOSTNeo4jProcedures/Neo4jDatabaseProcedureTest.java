package MOSTNeo4jProcedures;


import static org.junit.Assert.assertEquals;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Console;
import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;
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
		
		boolean state=db.addDatapoint("datapoint_1", "Test datapoint", "boolean",
				0.001, -30.00, 55.230, 323.32, 10.00, 5.00, 12.0, "+ -",
				"virtual", "hello", "Test datapoint");
		assertEquals(true, state);
		state=db.addDatapoint("datapoint_1", "Test datapoint", "boolean",
				0.001, -30.00, 55.230, 323.32, 10.00, 5.00, 12.0, "+ -",
				"virtual", "hello", "Test datapoint");
		assertEquals(false, state);
		state=db.addDatapoint("datapoint_2", "Test datapoint", "analog",
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
		boolean state=db.addDataForced("datapoint_1", "2013-07-23 12:23:45.00", 25.78);
		assertEquals(true, state);
		
	}
	@Test
	public void testAddData(){
		this.testAddZone();
		this.testAddDatapoint();
	    System.out.println(new Timestamp(new Date().getTime()).toString());
		boolean state=db.addData("datapoint_1",new Timestamp(new Date().getTime()).toString(), 26.34);
		assertEquals(false, state); // Violates the Dead band Constraint.
	}
	
	@Test
	public void testGetValues(){
		
		this.testAddDataForced();
		Node[] data=db.getValues("datapoint_1","2013-06-23 12:23:34.43",null);
		for(int i=0;i<data.length;i++){
			System.out.println("data_"+i+":"+data[i].getProperty("value"));
			
		}
		assertEquals(1, data.length);
		data=db.getValues("datapoint_1", null, "2013-08-23 12:23:34.34");
		for(int i=0;i<data.length;i++){
			System.out.println("data_"+i+":"+data[i].getProperty("value"));
			
		}
		assertEquals(1, data.length);
		data=db.getValues("datapoint_1", "2013-06-23 12:23:34.43", "2013-08-23 12:23:34.34");
		for(int i=0;i<data.length;i++){
			System.out.println("data_"+i+":"+data[i].getProperty("value"));
			
		}
		data=db.getValues("datapoint_1",null,null);
		for(int i=0;i<data.length;i++){
			System.out.println("data_"+i+":"+data[i].getProperty("value"));
			
		}
		
		
		
	}
	
	@Test
	public void testGetNumberOfValues(){
		
		this.testAddDataForced();
		Node [] data=db.getValues("datapoint_1", "2013-06-23 12:23:34.43", "2013-08-23 12:23:34.34");
		assertEquals(2, data.length);
		
	}
	@Test
	public void testGetValuesPeriodicallyBinary(){
		
		this.testAddDatapoint();
		boolean state;
		//ADDING DATA FOR DATAPOINT_1
		state=db.addDataForced("datapoint_1", "2013-07-23 12:13:34.23", 23.09);
		System.out.println("Data(Forced) added"+state);
		state=db.addDataForced("datapoint_1", "2013-07-23 12:14:34.23", 22.09);
		System.out.println("Data(Forced) added"+state);
		state=db.addDataForced("datapoint_1", "2013-07-23 12:15:34.23", 21.09);
		System.out.println("Data(Forced) added"+state);
		state=db.addDataForced("datapoint_1", "2013-07-23 12:16:34.23", 20.09);
		System.out.println("Data(Forced) added"+state);
		state=db.addDataForced("datapoint_1", "2013-07-23 12:17:34.23", 24.09);
		System.out.println("Data(Forced) added"+state);
		state=db.addDataForced("datapoint_1", "2013-07-23 12:12:34.23", 20.09);
		System.out.println("Data(Forced) added"+state);
		
        // ADDING DATA FOR DATAPOINT_2

		//ADDING DATA FOR DATAPOINT_1
		state=db.addDataForced("datapoint_2", "2013-07-23 12:13:34.23", 23.09);
		System.out.println("Data(Forced) added"+state);
		state=db.addDataForced("datapoint_2", "2013-07-23 12:14:34.23", 22.09);
		System.out.println("Data(Forced) added"+state);
		state=db.addDataForced("datapoint_2", "2013-07-23 12:15:34.23", 21.09);
		System.out.println("Data(Forced) added"+state);
		state=db.addDataForced("datapoint_2", "2013-07-23 12:16:34.23", 20.09);
		System.out.println("Data(Forced) added"+state);
		state=db.addDataForced("datapoint_2", "2013-07-23 12:17:34.23", 24.09);
		System.out.println("Data(Forced) added"+state);
		state=db.addDataForced("datapoint_2", "2013-07-23 12:12:34.23", 20.09);
		System.out.println("Data(Forced) added"+state);

		System.out.println("For Mode _1");
		ArrayList<data_periodic> temp= db.getValuesPerodicBinary("datapoint_1", "2013-07-23 12:12:12.12","2013-07-23 12:15:32.12",1000.0, 1);
		for(int i=0;i<temp.size();i++){
			
			System.out.println(""+temp.toString());
			
		}
		
		System.out.println("For Mode _2");
		temp= db.getValuesPerodicBinary("datapoint_1", "2013-07-23 12:12:12.12","2013-07-23 12:15:32.12",1000.0, 2);
		for(int i=0;i<temp.size();i++){
			
			System.out.println(""+temp.toString());
			
		}
		System.out.println("For Mode _3");
		temp= db.getValuesPerodicBinary("datapoint_1", "2013-07-23 12:12:12.12","2013-07-23 12:15:32.12",1000.0, 3);
		for(int i=0;i<temp.size();i++){
			
			System.out.println(""+temp.toString());
			
		}
		
		System.out.println("For Mode _1001");
		temp= db.getValuesPerodicBinary("datapoint_1", "2013-07-23 12:12:12.12","2013-07-23 12:15:32.12",1000.0, 1001);
		assertEquals(null, temp);
		System.out.println("For Mode _1002");
		temp= db.getValuesPerodicBinary("datapoint_1", "2013-07-23 12:12:12.12","2013-07-23 12:15:32.12",1000.0, 1002);
		assertEquals(null, temp);
		System.out.println("For Mode _1003");
		temp= db.getValuesPerodicBinary("datapoint_1", "2013-07-23 12:12:12.12","2013-07-23 12:15:32.12",1000.0, 1003);
		assertEquals(null, temp);
		
		
		
		
		
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
