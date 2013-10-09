package MOSTNeo4jProcedures;


import static org.junit.Assert.assertEquals;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.File;
import java.sql.Timestamp;
import java.util.Date;
import org.neo4j.graphdb.Node;

/**
 * @author Milinda Fernando 
 */

public class Neo4jDatabaseProcedureTest {

	private Neo4jDatabaseProcedure db;
	
	@Before
	public void setup() throws Exception  {
	    db=new Neo4jDatabaseProcedure();
	    db.SetDatabasePath("/home/milinda/neo4j-community-1.8.2","MOST.db");
        db.CreateDatabase();

		
	}
	
	@Test
	public void testGetStartNode(){
		Node start=db.getStartNode();
		assertEquals(1, start.getId());
		
	}
			
	@Test
    public void testAddDataForced(){
		
		boolean state=db.addDataForced("datapoint_1", "2013-07-23 12:23:45.00", 25.78);
		assertEquals(true, state);
		
	}

	@Test
	public void testAddData(){
		this.addTestDatatoDataPoints();
		boolean state=db.addData("datapoint_1",new Timestamp(new Date().getTime()).toString(), 26.34);
		assertEquals(true, state);
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
		assertEquals(1, data.length);
		
	}
			
	@Test
	public void testEmptyDatapoint(){
		
		this.addTestDatatoDataPoints();
		int y=db.getNumberofValues("datapoint_1","2012-06-23 12:23:23.34" , "2015-06-23 12:23:23.34");
		db.emptyDatapoint("datapoint_1","2013-07-23 12:14:34.23","2013-07-23 12:17:34.23" );
		int x=db.getNumberofValues("datapoint_1","2012-06-23 12:23:23.34" , "2015-06-23 12:23:23.34");
		assertEquals(4, y-x);
		db.emptyDatapoint("datapoint_1");
		x=db.getNumberofValues("datapoint_1","2012-06-23 12:23:23.34" , "2015-06-23 12:23:23.34");
		assertEquals(0, x);
		db.emptyDatapoint("datapoint_2");
		x=db.getNumberofValues("datapoint_2","2012-06-23 12:23:23.34" , "2015-06-23 12:23:23.34");
		assertEquals(0, x);
		
		
		
	}
	
	
	private void addTestDatatoDataPoints(){
// You have to call this function after adding the datapoints to the data base.		
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

		
		
		
	}
	
	
	@After
	public void tearDown()  {
		
		 db.ShutDownDatabase();

	        File file = new File("/home/milinda/neo4j-community-1.8.2/data/MOST.db/index");
	        String[] myFiles;
	        if (file.isDirectory()) {
	            myFiles = file.list();
	            for (int i = 0; i < myFiles.length; i++) {
	                File myFile = new File(file, myFiles[i]);
	                myFile.delete();
	            }
	        }
	        file.delete();

	        file = new File("/home/milinda/neo4j-community-1.8.2/data/MOST.db");

	        if (file.isDirectory()) {
	            myFiles = file.list();
	            for (int i = 0; i < myFiles.length; i++) {
	                File myFile = new File(file, myFiles[i]);
	                myFile.delete();
	            }
	        }



		
	}

	

}