package MOSTNeo4jProcedures;

import java.sql.Timestamp;

public class Main {

	public static void main(String [] args){
		
		
		Neo4jDatabaseProcedure db=new Neo4jDatabaseProcedure();
		db.SetDatabasePath("/home/milinda/neo4j-community-1.8.2","MOST.db");
		db.CreateDatabase();
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
		
	    Double rand;
	    //Adding Random Data to datapoint_1
	    Timestamp date=Timestamp.valueOf("2013-07-23 12:18:23.35");
	    for(int i=0;i<100;i++){
	       	rand=20+Math.random()*30;
	    	state=db.addDataForced("datapoint_1", date.toString(), rand);
			System.out.println("Data(Forced) added"+state);
	    date=new Timestamp(date.getTime()+5000);
	    	
	    }
		
		
		
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

		 for(int i=0;i<100;i++){
			    
		    	rand=20+Math.random()*30;
		    	state=db.addDataForced("datapoint_2", date.toString(), rand);
				System.out.println("Data(Forced) added"+state);
		    date=new Timestamp(date.getTime()+5000);
		    	
		    }
			
		
		
		
		
		
		
		
	}
	
	
}