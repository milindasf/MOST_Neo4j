package MOSTNeo4jProcedures;

public class Main {

	public static void main(String [] args){
		
		
		Neo4jDatabaseProcedure db=new Neo4jDatabaseProcedure();
		db.SetDatabase("/home/milinda/Desktop/MOSTDB");
		db.CreateDatabase();
		
		// ADDING ZONES
		boolean state;
		state = db.addZone("Zone_1", "This is a test Zone", "Sri lanka",
				"Colombo", "Sri lanka", "dematagoda", "Bank of Ceylon",
				"1st floor", "234", 234.2, 800.32);
		System.out.println("Zone added state:"+state);
		//ADDING SECOND ZONE
		state = db.addZone("Zone_2", "This is a test Zone", "Sri lanka",
				"Colombo", "Sri lanka", "dematagoda", "Bank of Ceylon",
				"1st floor", "234", 234.2, 800.32);
		System.out.println("Zone added state:"+state);
		
		//ADDING DATAPOINTS
		state=db.addDatapoint("datapoint_1", "Test datapoint", "boolean",
				0.001, -30.00, 55.230, 323.32, 10.00, 5.00, 12.0, "+ -",
				"virtual", "hello", "Test datapoint");
		System.out.println("Datapoint added state:"+state);
		state=db.addDatapoint("datapoint_2", "Test datapoint", "analog",
				0.001, -30.00, 55.230, 323.32, 10.00, 5.00, 12.0, "+ -",
				"virtual", "hello", "Test datapoint");
		System.out.println("Datapoint added state:"+state);
		
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
	
	
}