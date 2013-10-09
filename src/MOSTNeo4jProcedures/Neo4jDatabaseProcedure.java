package MOSTNeo4jProcedures;

/**
 * Author: Milinda Shayamal Fernandp
 * Date: 8/17/13
 * Time: 11:28 AM
 * To change this template use File | Settings | File Templates.
 */

import java.io.File;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class Neo4jDatabaseProcedure {

	private GraphDatabaseFactory graphDbFactory;
	private EmbeddedGraphDatabase graphDb;
	private String neo4jHome;
	private File dbFolder;
	private Node startNode;
	private Date date;
	private ExecutionEngine executeEng;
	private ExecutionResult result;
    private String databaseName; 
	
	public void SetDatabasePath(String neo4jHome, String databaseName) {

		this.neo4jHome = neo4jHome;
		this.databaseName=databaseName;
		dbFolder = new File(neo4jHome+"/data/"+databaseName);
		graphDbFactory = new GraphDatabaseFactory();
		date = new Date();

	}

	public void CreateDatabase() {
		if (dbFolder.exists() && dbFolder.list().length != 0) {
			// Database Exits.
			graphDb = (EmbeddedGraphDatabase) graphDbFactory
					.newEmbeddedDatabaseBuilder(this.neo4jHome+"/data/"+this.databaseName)
					.loadPropertiesFromFile(
							this.neo4jHome + "/conf/neo4j.properties")
					.newGraphDatabase();
			
		} else {

			graphDb = (EmbeddedGraphDatabase) graphDbFactory
					.newEmbeddedDatabase(neo4jHome+"/data/"+this.databaseName);
			this.createStartNode();
			
		}

		executeEng = new ExecutionEngine(this.graphDb);
		this.startNode=this.getStaNodeFromExistingDatabase();
		registerShutdownHook(graphDb);// To Ensure that the database is shutdown
		// properly when JVM Exits
	}

	public void ShutDownDatabase() {

		try {
			graphDb.shutdown();
		} catch (Exception e) {
			System.out.println("Error Occurred while Shutting down the database");
		}

	}

	private static void registerShutdownHook(final EmbeddedGraphDatabase graphDb) {
		/*
		 * Registers a shutdown hook for the Neo4j instance so that it shuts
		 * down nicely when the VM exits (even if you "Ctrl-C" the running
		 * example before it's completed)
		 */
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}

	private static enum RelTypes implements RelationshipType {
		HasData
	}

	public Node getStartNode() {
		return this.startNode;
	}

	public Node getStaNodeFromExistingDatabase(){
	
		String cypherquerry = "START startNode=NODE(1) RETURN startNode;";
		result=executeEng.execute(cypherquerry);
		Iterator<Node> iterator=result.columnAs("startNode");
		if(iterator.hasNext()){
			return iterator.next();
		}else{
			return null;
		}
		
		
	}
	
	private void createStartNode() {

		Transaction tx = this.graphDb.beginTx();
		try {
			this.startNode = graphDb.createNode();
			startNode.setProperty("message",
					"This is the start node of the database");
			startNode.setProperty("timeStamp", this.date.getTime());
			tx.success();
		} catch (Exception e) {
			System.out.println("Error Occurred while creating the node.");
			tx.failure();
		} finally {

			tx.finish();
		}

	}

	public boolean addDataForced(String p_datapoint_name, String p_datetime,
			Double p_value) {

		boolean state = false;
		Node startNode = this.getStartNode();
		Node data;
		if (startNode != null) {

			Transaction tx = graphDb.beginTx();
			try {
				data = this.graphDb.createNode();
				data.setProperty("datapoint_name", p_datapoint_name);
				data.setProperty("timestamp", p_datetime);
				data.setProperty("value", p_value);

				Relationship rel = startNode.createRelationshipTo(data,
						RelTypes.HasData);
				rel.setProperty("timestamp",
						new Timestamp(this.date.getTime()).toString());

				tx.success();
				state = true;

			} catch (Exception e) {
				System.out.println("Error Occurred While adding the data");
				e.printStackTrace();
				tx.failure();
				state = false;
			} finally {
				tx.finish();
			}

		} else {
			state = false;
		}

		return state;
	}

	public boolean addData(String p_datapoint_name, String p_timestamp,
			Double p_value) {

		boolean state = false;
		boolean constraints = true; // This variable will remain in true if all
		// the constraints are not violated...
		int errorCode = 0;
		// ERRORCODES: - { 10,11,12,13,14 }
		String cypherQuerry = "";
		Double lastValue = null;
		String lastTimestamp = "";
		Integer timeStampDif = null;
		Double dpDeadband = null;
		Double maxValue = null;
		Double minValue = null;
		Double sampleInterval = null;
		Double sampleIntervalMin = null;

		Node startNode = this.getStartNode();
		Node data;
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("node_id", startNode.getId());
		params.put("p_timestamp", p_timestamp);
		params.put("p_datapoint_name", p_datapoint_name);
		cypherQuerry = "START n = NODE({node_id}) MATCH n-[:HasData]->data WHERE data.datapoint_name={p_datapoint_name} AND data.timestamp<={p_timestamp} RETURN data ORDER BY data.timestamp DESC LIMIT 1;";
		try {

			result = executeEng.execute(cypherQuerry, params);
			Iterator<Node> iterator = result.columnAs("data");
			if (iterator.hasNext()) {
				data = iterator.next();
				lastTimestamp = data.getProperty("timestamp").toString();
				lastValue = Double.parseDouble(data.getProperty("value")
						.toString());

			}

		} catch (Exception e) {
			System.out.println("Error Occurred while Executing the Query");
			return false;
		}

		Timestamp lasttime = Timestamp.valueOf(lastTimestamp);
		Timestamp timestamp = Timestamp.valueOf(p_timestamp);

		timeStampDif = (int) (timestamp.getTime() - lasttime.getTime()) / 1000;

		// ------ These values needed to be get from the MySQL database......

		dpDeadband = null;
		maxValue = null;
		minValue = null;
		sampleInterval = null;
		sampleIntervalMin = null;

		// ////////////////////////////////////////////////////////////////////////

		// ######CONSTRAINTS################
		// # MINVALUE CONSTRAINT
		// # ignore if min is NULL
		// # ERROR CODE -13

		// MIN Value Constraint
		if (minValue != null) {

			if (p_value < minValue) {
				constraints = false;
				errorCode = -13;
				System.out.println("Error code:-13");
				return false;
			}

		}
		// Max value Constraint
		if (maxValue != null) {
			if (p_value > maxValue) {
				constraints = false;
				errorCode = -12;
				System.out.println("Error code:-12");
				return false;
			}

		}
		params.clear();
		params.put("node_id", startNode.getId());
		params.put("p_datapoint_name", p_datapoint_name);
		cypherQuerry = "START n= NODE({node_id}) MATCH n-[:HasData]->data WHERE data.datapoint_name={p_datapoint_name} RETURN data;";
		try {

			result = executeEng.execute(cypherQuerry, params);
			Iterator<Node> iterator = result.columnAs("data");

			if (!iterator.hasNext()) {

				Transaction tx = this.graphDb.beginTx();
				try {

					data = this.graphDb.createNode();
					data.setProperty("datapoint_name", p_datapoint_name);
					data.setProperty("timestamp", p_timestamp);
					data.setProperty("value", p_value);

					Relationship rel = startNode.createRelationshipTo(data,
							RelTypes.HasData);
					rel.setProperty("timestamp",
							new Time(date.getTime()).toString());
					tx.success();

				} catch (Exception b) {
					System.out
							.println("Error Occured While updating data node in database");
					tx.failure();
				} finally {
					tx.finish();
				}
				state = true;
				return true;
			}

		} catch (Exception e) {

			System.out.println("Error Occured while Executing the Querry");
			return false;

		}

		// # SAMPLE_INTERVAL CONSTRAINT
		// # ignore if sampleInterval is NULL --> go to deadband and
		// sample_interval_min check
		// # Note: if something < null --> false; same for >, etc. --> works
		// # return SELECT 2 if outside of sample_interval
		//
		if (sampleInterval == null || timeStampDif < sampleInterval) {

			// # inside sample_interval (or sampleInterval is NULL) -->
			//
			// # MIN SAMPLE INTERVAL CONSTRAINT
			// # ignore if sampleIntervalMin is NULL
			// # ERROR CODE -10

			if (sampleIntervalMin != null && timeStampDif < sampleIntervalMin) {

				errorCode = -10;
				System.out.println("Error code:-10");
				return false;
			} else if (dpDeadband == null
					|| ((p_value < (-dpDeadband / 2 + lastValue)) && (p_value > (dpDeadband / 2 + lastValue)))) {
				// #DEADBAND CONSTRAINT
				// # ignore if deadband is NULL
				// # Note: if something < null --> false; same for >, etc. -->
				// works
				// # tested:
				// "SELECT IF(true OR 10 NOT BETWEEN (- null/2 + 5) AND (null/2 + 5),2,3);"
				// #ERROR CODE -11
				Transaction tx = this.graphDb.beginTx();
				try {
					data = this.graphDb.createNode();
					data.setProperty("datapoint_name", p_datapoint_name);
					data.setProperty("timestamp", p_timestamp);
					data.setProperty("value", p_value);

					Relationship rel = startNode.createRelationshipTo(data,
							RelTypes.HasData);
					rel.setProperty("timestamp",
							new Time(date.getTime()).toString());
					tx.success();
				} catch (Exception e) {
					System.out
							.println("Error Occured While updating data node in database");
					tx.failure();
				} finally {
					tx.finish();
				}

				return true;

			} else {
				errorCode = -11;
				System.out.println("Error Code:-11");
				return false;

			}

		} else {

			// # sample interval exceeded - value is inserted
			// #added because outside of sample_interval

			Transaction tx = this.graphDb.beginTx();
			try {
				data = this.graphDb.createNode();
				data.setProperty("datapoint_name", p_datapoint_name);
				data.setProperty("timestamp", p_timestamp);
				data.setProperty("value", p_value);

				Relationship rel = startNode.createRelationshipTo(data,
						RelTypes.HasData);
				rel.setProperty("timestamp",
						new Time(date.getTime()).toString());
				tx.success();
			} catch (Exception e) {
				System.out
						.println("Error Occured While updating data node in database");
				tx.failure();
			} finally {
				tx.finish();
			}

			return true;

		}

	}

	public Node[] getValues(String p_datapoint_name, String p_starttime,
			String p_endtime) {

		Node[] data = null;
		Timestamp starttime;
		Timestamp endtime;
		int timeDiff;
		String cypherQuerry = "";
		Node startNode = this.getStartNode();
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("node_id", startNode.getId());

		try {
			if (p_starttime != null && p_endtime != null) {
				starttime = Timestamp.valueOf(p_starttime);
				endtime = Timestamp.valueOf(p_endtime);
				timeDiff = (int) (endtime.getTime() - starttime.getTime()) / 1000;
			} else {
				timeDiff = 2;
			}

			if (timeDiff < 0) {
				System.out.println("endtime must be later than starttime!");
				data = null;
				return data;
			} else {

				if (p_starttime == null) {

					if (p_endtime == null) {

						params.clear();
						params.put("node_id", startNode.getId());
						params.put("p_datapoint_name", p_datapoint_name);
						cypherQuerry = "START n=NODE ({node_id}) MATCH n-[:HasData]->data WHERE data.datapoint_name={p_datapoint_name} RETURN data ORDER BY data.timestamp DESC LIMIT 1;";
						result = executeEng.execute(cypherQuerry, params);
						Iterator<Node> iterator = result.columnAs("data");
						data = new Node[1];
						data[0] = null;
						if (iterator.hasNext()) {
							data[0] = iterator.next();
							return data;
						}

					} else {
						endtime = Timestamp.valueOf(p_endtime);
						params.clear();
						params.put("node_id", startNode.getId());
						params.put("endtime", p_endtime);
						params.put("p_datapoint_name", p_datapoint_name);
						cypherQuerry = "START n= NODE({node_id}) MATCH n-[:HasData]->data WHERE data.datapoint_name={p_datapoint_name} AND data.timestamp <={endtime} RETURN data ORDER BY data.timestamp DESC LIMIT 1;";
						result = executeEng.execute(cypherQuerry, params);
						Iterator<Node> iterator = result.columnAs("data");
						data = new Node[1];
						data[0] = null;
						if (iterator.hasNext()) {
							data[0] = iterator.next();
							return data;
						}

					}

				} else if (p_endtime == null) {
					starttime = Timestamp.valueOf(p_starttime);
					params.clear();
					params.put("node_id", startNode.getId());
					params.put("starttime", p_starttime);
					params.put("p_datapoint_name", p_datapoint_name);
					cypherQuerry = "START n=NODE ({node_id}) MATCH n-[:HasData]->data WHERE data.datapoint_name={p_datapoint_name} AND data.timestamp>={starttime} RETURN data ORDER BY data.timestamp ASC;";
					result = executeEng.execute(cypherQuerry, params);
					Iterator<Node> iterator = result.columnAs("data");
					data = new Node[1];
					data[0] = null;
					if (iterator.hasNext()) {
						data[0] = iterator.next();
						return data;
					}

				} else {
					starttime = Timestamp.valueOf(p_starttime);
					endtime = Timestamp.valueOf(p_endtime);
					params.clear();
					params.put("node_id", startNode.getId());
					params.put("starttime", p_starttime);
					params.put("endtime", p_endtime);
					params.put("p_datapoint_name", p_datapoint_name);
					cypherQuerry = "START n=NODE ({node_id}) MATCH n-[:HasData]->data WHERE data.datapoint_name={p_datapoint_name} AND data.timestamp>={starttime} AND data.timestamp<={endtime} RETURN data ORDER BY data.timestamp;";
					result = executeEng.execute(cypherQuerry, params);
					Iterator<Node> iterator = result.columnAs("data");
					ArrayList<Node> temp_list = new ArrayList<Node>();
					while (iterator.hasNext()) {
						temp_list.add(iterator.next());
					}
					if (!temp_list.isEmpty()) {
						data = new Node[temp_list.size()];
						for (int i = 0; i < data.length; i++) {
							data[i] = temp_list.get(i);

						}
						return data;
					} else {
						data = null;
					}

					return data;

				}

			}

		} catch (Exception e) {

			System.out
					.println("Error Occurred while reading data from database.");
			data = null;
		}

		return data;
	}

	public int getNumberofValues(String p_datapoint_name, String p_starttime,
			String p_endtime) {

		int numberOfValues = -1;
		Timestamp starttime;
		Timestamp endtime;
		int timeDiff = 0;
		String cypherQuerry = "";
		Node startNode = this.getStartNode();
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("node_id", startNode.getId());

		try {
			if (p_starttime != null && p_endtime != null) {

				starttime = Timestamp.valueOf(p_starttime);
				endtime = Timestamp.valueOf(p_endtime);
				timeDiff = (int) (endtime.getTime() - starttime.getTime()) / 1000;
				if (timeDiff < 0) {
					System.out
							.println("End time must be grataer than the start time");
					return -1;
				} else {

					starttime = Timestamp.valueOf(p_starttime);
					endtime = Timestamp.valueOf(p_endtime);
					params.clear();
					params.put("node_id", startNode.getId());
					params.put("starttime", p_starttime);
					params.put("endtime", p_endtime);
					params.put("p_datapoint_name", p_datapoint_name);
					cypherQuerry = "START n=NODE ({node_id}) MATCH n-[:HasData]->data WHERE data.datapoint_name={p_datapoint_name} AND data.timestamp>={starttime} AND data.timestamp<={endtime} RETURN data ORDER BY data.timestamp ASC;";
					result = executeEng.execute(cypherQuerry, params);
					Iterator<Node> iterator = result.columnAs("data");
					ArrayList<Node> temp_list = new ArrayList<Node>();
					while (iterator.hasNext()) {
						temp_list.add(iterator.next());
					}

					numberOfValues = temp_list.size();
					return numberOfValues;

				}

			}

		} catch (Exception e) {
			System.out
					.println("Error Occurred While Counting number of values");

		}
		return numberOfValues;
	}

	public void emptyDatapoint(String p_datapoint_name) {

		String cypherquerry = "";
		Map<String, Object> params = new HashMap<String, Object>();

		params.put("datapoint_name", p_datapoint_name);
		cypherquerry = "START n=NODE(1) MATCH n-[:HasData]->data WHERE data.datapoint_name={datapoint_name} RETURN data;";
		Node data;
		result = executeEng.execute(cypherquerry, params);
		Iterator<Node> iterator = result.columnAs("data");
		Iterator<Relationship> rel;
		if (!iterator.hasNext()) {
			System.out.println("No data found for the given datapoint...");
			return;
		} else {

			while (iterator.hasNext()) {

				data = iterator.next();
				rel = data.getRelationships(RelTypes.HasData,
						Direction.INCOMING).iterator();
				Transaction tx = graphDb.beginTx();
				try {
					rel.next().delete();
					data.delete();
					tx.success();
				} catch (Exception e) {
					System.out.println("Error occured deleting data...");
					tx.failure();
				} finally {
					tx.finish();
				}

			}

		}

	}

	public void emptyDatapoint(String p_datapoint_name, String p_starttime,
			String p_endtime) {

		Timestamp start;
		Timestamp end;
		try {
			start = Timestamp.valueOf(p_starttime);
			end = Timestamp.valueOf(p_endtime);

		} catch (Exception e) {
			System.out
					.println("Invalid DateTime Formats for start time or end time.");
			return;
		}

		String cypherquerry = "";
		Map<String, Object> params = new HashMap<String, Object>();

		params.put("datapoint_name", p_datapoint_name);
		params.put("starttime", start.toString());
		params.put("endtime", end.toString());
		cypherquerry = "START n=NODE(1) MATCH n-[:HasData]->data WHERE data.datapoint_name={datapoint_name} AND data.timestamp>={starttime} AND data.timestamp<={endtime} RETURN data;";
		Node data;
		result = executeEng.execute(cypherquerry, params);
		Iterator<Node> iterator = result.columnAs("data");
		Iterator<Relationship> rel;
		if (!iterator.hasNext()) {
			System.out.println("No data found for the given datapoint...");
			return;
		} else {

			while (iterator.hasNext()) {

				data = iterator.next();
				rel = data.getRelationships(RelTypes.HasData,
						Direction.INCOMING).iterator();
				Transaction tx = graphDb.beginTx();
				try {
					rel.next().delete();
					data.delete();
					tx.success();
				} catch (Exception e) {
					System.out.println("Error occured deleting data...");
					tx.failure();
				} finally {
					tx.finish();
				}

			}

		}

	}

}
