package MOSTNeo4jProcedures;

/**
 * User: Milinda Fernandp
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

import scala.annotation.target.param;

public class Neo4jDatabaseProcedure {

	private GraphDatabaseFactory graphDbFactory;
	private EmbeddedGraphDatabase graphDb;
	private String databasePath;
	private File dbFolder;
	private Node startNode;
	private Date date;
	private ExecutionEngine execute_eng;
	private ExecutionResult result;

	public void SetDatabase(String databasePath) {

		this.databasePath = databasePath;
		dbFolder = new File(databasePath);
		graphDbFactory = new GraphDatabaseFactory();
		date = new Date();

	}

	public void CreateDatabase() {
		if (dbFolder.exists() && dbFolder.list().length != 0) {
			// Database Exits.
			graphDb = (EmbeddedGraphDatabase) graphDbFactory
					.newEmbeddedDatabaseBuilder(this.databasePath)
					.loadPropertiesFromFile(
							this.databasePath + "/neo4j.properties")
					.newGraphDatabase();
		} else {

			graphDb = (EmbeddedGraphDatabase) graphDbFactory
					.newEmbeddedDatabase(databasePath);
			this.createStartNode();
			execute_eng = new ExecutionEngine(this.graphDb);
		}

		registerShutdownHook(graphDb);// To Ensure that the database is shutdown
		// properly when JVM Exits
	}

	public void StopDatabase() {

		try {

			graphDb.shutdown();
		} catch (Exception e) {
			System.out
					.println("Error Occurred while Shutting down the database");
		}

	}

	private static void registerShutdownHook(final EmbeddedGraphDatabase graphDb) {
		// Registers a shutdown hook for the Neo4j instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running example before it's completed)
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}

	/*
	 * Here are the relationship types that MOST graph database should have.
	 */

	private static enum RelTypes implements RelationshipType {
		HasZone, HasSubZone, HasData, HasWarning, HasDatapoint, HasConnection

	}

	/*
	 * 
	 * Start NODE is created when we create the database.All the Zones will be
	 * attached to the Start Node.
	 */

	public Node getStartNode() {

		return this.startNode;
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

	public boolean addZone(String p_name, String p_description,
			String p_country, String p_state, String p_county, String p_city,
			String p_building, String p_floor, String p_room, Double p_area,
			Double p_volume) {
		boolean state = false;
		Node tempZone;
		Transaction tx = this.graphDb.beginTx();
		int zoneID = this.getZoneCount();
		try {

			tempZone = this.graphDb.createNode();

			tempZone.setProperty("idzone", zoneID); // Primary key of the Node
			tempZone.setProperty("name", p_name);
			tempZone.setProperty("description", p_description);
			tempZone.setProperty("country", p_country);
			tempZone.setProperty("state", p_state);
			tempZone.setProperty("county", p_county);
			tempZone.setProperty("city", p_city);
			tempZone.setProperty("building", p_building);
			tempZone.setProperty("floor", p_floor);
			tempZone.setProperty("room", p_room);
			tempZone.setProperty("area", p_area);
			tempZone.setProperty("volume", p_volume);

			Relationship rel = this.startNode.createRelationshipTo(tempZone,
					RelTypes.HasZone);
			rel.setProperty("timeStamp",
					new Timestamp(this.date.getTime()).toString());
			tx.success();
			// System.out.println("Zone added was successfull");
			state = true;
		} catch (Exception e) {

			System.out
					.println("Error Occurred When adding a Zone to the database");
			e.printStackTrace();
			tx.failure();
			state = false;
		} finally {

			tx.finish();

		}

		return state;

	}

	public int getZoneCount() {

		int numberOfZones = 0;
		String cypher = "START n=NODE(1) MATCH n-[:HasZone]->zone RETURN COUNT(DISTINCT zone) AS Number_Of_Zones;";
		try {

			result = execute_eng.execute(cypher);
			Iterator<Long> n_column = result.columnAs("Number_Of_Zones");
			while (n_column.hasNext()) {
				numberOfZones = Integer.parseInt(n_column.next().toString());
			}
		} catch (Exception e) {
			System.out.println("Error Occurred while executing the query");
			numberOfZones = -1;
		}

		return numberOfZones;

	}

	public boolean addDatapoint(String p_datapoint_name, String p_type,
			String p_unit, Double p_accuracy, Double p_min, Double p_max,
			Double p_deadband, Double p_sample_interval,
			Double p_sample_interval_min, Double p_watchdog,
			String p_math_operations, String p_virtual, String p_custom_attr,
			String p_description) {
		boolean state = false;
		if (p_datapoint_name == null) {

			return state;

		} else {
			Node temp_datapoint;
			temp_datapoint = this.getDatapointByName(p_datapoint_name);
			if (temp_datapoint != null) {
				state = false; // This is for check duplicate datapoints;
				return state;
			}

			if (!p_unit.equals("boolean") && !p_unit.equals("analog")) {
				System.out.println("Datapoint unit must be boolean or analog");
				return false;

			}

			Transaction tx = graphDb.beginTx();
			try {

				temp_datapoint = graphDb.createNode();
				temp_datapoint.setProperty("datapoint_name", p_datapoint_name);
				temp_datapoint.setProperty("type", p_type);
				temp_datapoint.setProperty("unit", p_unit);
				temp_datapoint.setProperty("accuracy", p_accuracy);
				temp_datapoint.setProperty("min", p_min);
				temp_datapoint.setProperty("max", p_max);
				temp_datapoint.setProperty("deadband", p_deadband);
				temp_datapoint
						.setProperty("sample_interval", p_sample_interval);
				temp_datapoint.setProperty("sample_interval_min",
						p_sample_interval_min);
				temp_datapoint.setProperty("watchdog", p_watchdog);
				temp_datapoint
						.setProperty("math_operations", p_math_operations);
				temp_datapoint.setProperty("virtual", p_virtual);
				temp_datapoint.setProperty("custom_attr", p_custom_attr);
				temp_datapoint.setProperty("description", p_description);
				temp_datapoint.setProperty("idzone", -1); // Here -1 denotes
				// that the this
				// datapoint doesn't
				// belongs to any
				// zones.
				Relationship rel = this.startNode.createRelationshipTo(
						temp_datapoint, RelTypes.HasDatapoint);
				rel.setProperty("timestamp",
						new Timestamp(this.date.getTime()).toString());

				tx.success();
				this.addDataForced(p_datapoint_name,
						new Timestamp(this.date.getTime()).toString(), 0.0);
				state = true;

			} catch (Exception e) {
				System.out
						.println("Error Occurred while entering the datapoint");
				e.printStackTrace();
				tx.failure();
			} finally {

				tx.finish();
			}

			return state;

		}

	}

	public Node getZoneByID(int p_idzone) {

		Node temp = null;
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("idzone", p_idzone);
		String cypher = "START n=NODE(1) MATCH n-[:HasZone]->zone WHERE zone.idzone={idzone} RETURN zone;";
		try {
			result = execute_eng.execute(cypher, params);
			Iterator<Node> iterator = result.columnAs("zone");
			while (iterator.hasNext()) {
				temp = iterator.next();
			}

		} catch (Exception e) {
			System.out.println("Error Occurred while executing query");
			temp = null;
		}

		return temp;
	}

	public boolean addDatapointToZone(String p_datapoint_name, Integer p_idzone) {

		boolean state = false;

		Node temp = this.getDatapointByName(p_datapoint_name);
		Node zone = this.getZoneByID(p_idzone);

		if (temp != null & zone != null) {

			Transaction tx = this.graphDb.beginTx();
			try {
				temp.setProperty("idzone", p_idzone);
				temp.getSingleRelationship(RelTypes.HasDatapoint,
						Direction.INCOMING).delete();
				Relationship rel = zone.createRelationshipTo(temp,
						RelTypes.HasDatapoint);
				rel.setProperty("timestamp",
						new Timestamp(this.date.getTime()).toString());
				tx.success();
				state = true;

			} catch (Exception e) {

				System.out.println("Error occurred while adding the datapoint");
				e.printStackTrace();
				tx.failure();

			} finally {
				tx.finish();
			}

		} else {
			state = false;

		}

		return state;

	}

	public Node getDatapointByName(String p_datapoint_name) {

		Node temp = null;
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("datapoint_name", p_datapoint_name);
		String cypher = "START n=NODE(1) MATCH n-[:HasZone]->()-[:HasDatapoint]->datapoint WHERE datapoint.datapoint_name={datapoint_name} RETURN datapoint;";
		try {
			result = execute_eng.execute(cypher, params);
			Iterator<Node> iterator = result.columnAs("datapoint");
			if (iterator.hasNext()) {
				temp = iterator.next();

			} else {
				cypher = "START n=NODE(1) MATCH n-[:HasDatapoint]->datapoint WHERE datapoint.datapoint_name={datapoint_name} RETURN datapoint;";
				result = execute_eng.execute(cypher, params);
				iterator = result.columnAs("datapoint");
				if (iterator.hasNext()) {
					temp = iterator.next();
				} else {
					temp = null;
				}
			}

		} catch (Exception e) {
			System.out.println("Error occurred while executing query");
			temp = null;
		}

		return temp;

	}

	public boolean addDataForced(String p_datapoint_name, String p_datetime,
			Double p_value) {

		boolean state = false;
		Node datapoint = this.getDatapointByName(p_datapoint_name);
		Node data;
		if (datapoint != null) {

			Transaction tx = graphDb.beginTx();
			try {
				data = this.graphDb.createNode();
				data.setProperty("datapoint_name", p_datapoint_name);
				data.setProperty("timestamp", p_datetime);
				data.setProperty("value", p_value);

				Relationship rel = datapoint.createRelationshipTo(data,
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

		Node datapoint = this.getDatapointByName(p_datapoint_name);
		Node data;
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("node_id", datapoint.getId());
		params.put("p_timestamp", p_timestamp);
		cypherQuerry = "START n = NODE({node_id}) MATCH n-[:HasData]->data WHERE data.timestamp<={p_timestamp} RETURN data ORDER BY data.timestamp DESC LIMIT 1;";
		try {

			result = execute_eng.execute(cypherQuerry, params);
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

		dpDeadband = Double.parseDouble(datapoint.getProperty("deadband")
				.toString());
		maxValue = Double.parseDouble(datapoint.getProperty("max").toString());
		minValue = Double.parseDouble(datapoint.getProperty("min").toString());
		sampleInterval = Double.parseDouble(datapoint.getProperty(
				"sample_interval").toString());
		sampleIntervalMin = Double.parseDouble(datapoint.getProperty(
				"sample_interval_min").toString());

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
		params.put("node_id", datapoint.getId());
		cypherQuerry = "START n= NODE({node_id}) MATCH n-[:HasData]->data RETURN data;";
		try {

			result = execute_eng.execute(cypherQuerry, params);
			Iterator<Node> iterator = result.columnAs("data");

			if (!iterator.hasNext()) {

				Transaction tx = this.graphDb.beginTx();
				try {

					data = this.graphDb.createNode();
					data.setProperty("datapoint_name", p_datapoint_name);
					data.setProperty("timestamp", p_timestamp);
					data.setProperty("value", p_value);

					Relationship rel = datapoint.createRelationshipTo(data,
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

					Relationship rel = datapoint.createRelationshipTo(data,
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

				Relationship rel = datapoint.createRelationshipTo(data,
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
		Node datapoint = this.getDatapointByName(p_datapoint_name);
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("node_id", datapoint.getId());

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
						params.put("node_id", datapoint.getId());
						cypherQuerry = "START n=NODE ({node_id}) MATCH n-[:HasData]->data RETURN data ORDER BY data.timestamp DESC LIMIT 1;";
						result = execute_eng.execute(cypherQuerry, params);
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
						params.put("node_id", datapoint.getId());
						params.put("endtime", p_endtime);
						cypherQuerry = "START n= NODE({node_id}) MATCH n-[:HasData]->data WHERE data.timestamp <={endtime} RETURN data ORDER BY data.timestamp DESC LIMIT 1;";
						result = execute_eng.execute(cypherQuerry, params);
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
					params.put("node_id", datapoint.getId());
					params.put("starttime", p_starttime);
					cypherQuerry = "START n=NODE ({node_id}) MATCH n-[:HasData]->data WHERE data.timestamp>={starttime} RETURN data ORDER BY data.timestamp ASC;";
					result = execute_eng.execute(cypherQuerry, params);
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
					params.put("node_id", datapoint.getId());
					params.put("starttime", p_starttime);
					params.put("endtime", p_endtime);
					cypherQuerry = "START n=NODE ({node_id}) MATCH n-[:HasData]->data WHERE data.timestamp>={starttime} AND data.timestamp<={endtime} RETURN data ORDER BY data.timestamp;";
					result = execute_eng.execute(cypherQuerry, params);
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
		Node datapoint = this.getDatapointByName(p_datapoint_name);
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("node_id", datapoint.getId());

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
					params.put("node_id", datapoint.getId());
					params.put("starttime", p_starttime);
					params.put("endtime", p_endtime);
					cypherQuerry = "START n=NODE ({node_id}) MATCH n-[:HasData]->data WHERE data.timestamp>={starttime} AND data.timestamp<={endtime} RETURN data ORDER BY data.timestamp ASC;";
					result = execute_eng.execute(cypherQuerry, params);
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

	public ArrayList<data_periodic> getValuesPerodicBinary(
			String p_datapoint_name, String p_starttime, String p_endtime,
			Double p_period, int p_mode) {

		Timestamp lv_firstDate;
		Double lv_firstValue;
		Double lv_lastValidValue;
		Double lv_CurrentValue;
		Double lv_quality;
		Timestamp lv_starttime_per;
		Timestamp lv_endtime_per;
		Timestamp lv_starttime_interpol;
		Boolean lv_lastperiod_was_valid;
		Integer lv_countPeriod;
		Boolean lv_outputEnabled;
		Node datapoint;
		String cypherquerry = "";
		Map<String, Object> params = new HashMap<String, Object>();

		lv_outputEnabled = true;
		Timestamp start;
		Timestamp end;
		try {
			start = Timestamp.valueOf(p_starttime);
			end = Timestamp.valueOf(p_endtime);
		} catch (Exception e) {
			System.out.println("Invalid format of the Timestamp Strings");
			return null;
		}
		// ///////////////////////////////////////////////////
		try {

			if ((end.getTime() - start.getTime()) < p_period) {
				System.out
						.println("Endtime must be later than the starttime + one period");
				return null;
			}

			datapoint = this.getDatapointByName(p_datapoint_name);
			if (datapoint == null) {
				System.out
						.println("Given datapoint name does not exist in the database");
				return null;
			} else {

				ArrayList<data_periodic> temp_data_periodic = new ArrayList<data_periodic>();
				data_periodic temp;

				if (this.getNumberofValues(p_datapoint_name, p_starttime,
						p_endtime) == 0) {
					temp = new data_periodic(null, p_starttime,
							p_datapoint_name, 0.0);
					temp_data_periodic.add(temp);
					lv_endtime_per = new Timestamp(start.getTime() + 1);
					lv_endtime_per = new Timestamp(start.getTime()
							+ Math.round(p_period));

					// do calculations while endtime for next periode sooner
					// than final endtime
					while (end.getTime() - lv_endtime_per.getTime() >= 0) {
						temp = new data_periodic(null,
								lv_endtime_per.toString(), p_datapoint_name,
								0.0);
						temp_data_periodic.add(temp);

						// shift to next periode
						// lv_endtime_per+1 because SQL BETWEEN function
						// includes the limits to (closed set)

						lv_starttime_per = new Timestamp(
								lv_endtime_per.getTime() + 1);
						lv_endtime_per = new Timestamp(lv_endtime_per.getTime()
								+ Math.round(p_period));
					}

				} else {

					// #---------------------------------------------------------------------------------------------
					// #-------------------------------- start of main algorithm
					// ------------------------------------
					// #---------------------------------------------------------------------------------------------
					//
					lv_starttime_per = start;
					lv_endtime_per = new Timestamp(lv_starttime_per.getTime()
							+ Math.round(p_period));

					Node[] data = this.getValues(p_datapoint_name, p_starttime,
							p_endtime);
					lv_firstDate = Timestamp.valueOf(data[0].getProperty(
							"timestamp").toString());

					// #the first value (the value with the earliest date) is
					// directly interpreted as the value at the starttime (no
					// averageing/interpolation,
					// #because averaging/interpolation for a certain timepoint
					// is done with the period before)

					lv_firstValue = Double.parseDouble(data[0].getProperty(
							"value").toString());

					// the values in the table data_periodic are temporary, so
					// the values in this table need to be deleted

					lv_lastValidValue = lv_firstValue;
					lv_lastperiod_was_valid = true;

					// #set if the generated values shall be returned to calling
					// function (by the SELECT statement at the end of this SP)

					if (p_mode > 1000) {
						lv_outputEnabled = false;
						p_mode = p_mode - 1000;
					}

					temp = new data_periodic(lv_firstValue, p_starttime,
							p_datapoint_name, 0.0);
					temp_data_periodic.add(temp);

					lv_countPeriod = 1;

					switch (p_mode) {

					case 1:

						// #---------------------------------------------------------------------------------------------
						// #------------------ mode = 1 (majority decision /
						// sample & hold)------------------------------
						// #---------------------------------------------------------------------------------------------

						// #algorithm for majority decision (if there are more
						// than one values) and sample & hold (if there is no
						// value)
						// #do calculations while endtime for next period sooner
						// than final endtime

						while ((end.getTime() - lv_endtime_per.getTime()) >= 0) {

							// #check if there are values in current period
							// datapoint=this.getDatapointByName(p_datapoint_name);
							params.clear();
							params.put("node_id", datapoint.getId());
							params.put("lv_starttime_per",
									lv_starttime_per.toString());
							params.put("lv_endtime_per",
									lv_endtime_per.toString());
							params.put("datapoint_name", p_datapoint_name);
							cypherquerry = "start n=NODE({node_id}) MATCH n-[:HasData]->data WHERE data.timestamp>={lv_starttime_per} and data.timestamp<={lv_endtime_per} and data.datapoint_name={datapoint_name} RETURN DISTINCT data.datapoint_name AS datapoint_name;";
							result = execute_eng.execute(cypherquerry, params);
							Iterator<String> iterator = result
									.columnAs("datapoint_name");
							if (iterator.hasNext()) {
								// No need to clear the params....
								cypherquerry = "start n=NODE({node_id}) MATCH n-[:HasData]->data WHERE data.timestamp>={lv_starttime_per} and data.timestamp<={lv_endtime_per} and data.datapoint_name={datapoint_name} RETURN COUNT(data) AS count;";
								result = execute_eng.execute(cypherquerry,
										params);
								Iterator<Long> iterator3 = result
										.columnAs("count");
								// #lv_quality is used for majority decision
								// algorithm

								lv_quality = Double.parseDouble(iterator3
										.next().toString());

								// #majority decision of values, everything not
								// 0 is interpreted as TRUE, if half values are
								// TRUE the result is TRUE

								// No need to clear the params....
								cypherquerry = "start n=NODE({node_id}) MATCH n-[:HasData]->data WHERE data.timestamp>={lv_starttime_per} and data.timestamp<={lv_endtime_per} and data.datapoint_name={datapoint_name} and data.value=0 RETURN COUNT(data) AS count;";
								result = execute_eng.execute(cypherquerry,
										params);
								iterator3 = result.columnAs("count");
								if (lv_quality < 2 * Integer.parseInt(iterator3
										.next().toString())) {
									lv_CurrentValue = 0.0;

								} else {
									lv_CurrentValue = 1.0;
								}
								lv_countPeriod = 1;
								// #assigning of value (result of last period)
								// to the endtime of the period

								temp = new data_periodic(lv_CurrentValue,
										lv_endtime_per.toString(),
										p_datapoint_name, lv_quality);
								temp_data_periodic.add(temp);

								// #store last value of current period (-> for
								// use in hold mode)
								// No Need to Clear the parms....

								cypherquerry = "start n=NODE({node_id}) MATCH n-[:HasData]->data WHERE data.timestamp<={lv_endtime_per} and data.datapoint_name={datapoint_name} RETURN data ORDER BY data.timestamp DESC LIMIT 1;";
								result = execute_eng.execute(cypherquerry,
										params);
								Iterator<Node> iterator2 = result
										.columnAs("data");
								lv_lastValidValue = Double
										.parseDouble(iterator2.next()
												.getProperty("value")
												.toString());

							} else {

								lv_countPeriod = lv_countPeriod + 1;
								lv_quality = 1.0 / lv_countPeriod;
								// #assigning of value (the last value of the
								// last valid period-> hold mode) to the endtime
								// of the period

								temp = new data_periodic(lv_lastValidValue,
										lv_endtime_per.toString(),
										p_datapoint_name, lv_quality);
								temp_data_periodic.add(temp);

							}

							// #shift to next period
							// #lv_endtime_per+1 because SQL BETWEEN function
							// includes the limits to (closed set)

							lv_starttime_per = new Timestamp(
									lv_endtime_per.getTime() + 1);
							lv_endtime_per = new Timestamp(
									lv_endtime_per.getTime()
											+ Math.round(p_period));

						}
						break;

					case 2:
						// #---------------------------------------------------------------------------------------------
						// #--------------------------- mode = 2 (forced 0 /
						// default 1)----------------------------------
						// #---------------------------------------------------------------------------------------------

						// #algorithm for forced 0 (if there is at least one 0
						// in the period the periodic value is zero) and default
						// 1 (if there is no value in the period the periodic
						// value is 1)
						// #do calculations while endtime for next period sooner
						// than final endtime
						while ((end.getTime() - lv_endtime_per.getTime()) >= 0) {
							// #check if there are values in current period
							params.clear();
							params.put("node_id", datapoint.getId());
							params.put("lv_starttime_per",
									lv_starttime_per.toString());
							params.put("lv_endtime_per",
									lv_endtime_per.toString());
							params.put("datapoint_name", p_datapoint_name);
							cypherquerry = "start n=NODE({node_id}) MATCH n-[:HasData]->data WHERE data.timestamp>={lv_starttime_per} and data.timestamp<={lv_endtime_per} and data.datapoint_name={datapoint_name} RETURN DISTINCT data.datapoint_name AS datapoint_name;";
							result = execute_eng.execute(cypherquerry, params);
							Iterator<String> iterator = result
									.columnAs("datapoint_name");
							if (iterator.hasNext()) {

								// #check if there is at least one zero in
								// current period and set lv_lastValidValue to 0
								// if so, to 1 if there is no zero in current
								// period
								cypherquerry = "start n=NODE({node_id}) MATCH n-[:HasData]->data WHERE data.timestamp>={lv_starttime_per} AND data.timestamp<={lv_endtime_per} AND data.datapoint_name={datapoint_name} AND data.value=0 RETURN DISTINCT data.datapoint_name AS datapoint_name;";
								result = execute_eng.execute(cypherquerry,
										params);
								iterator = result.columnAs("datapoint_name");
								if (iterator.hasNext()) {
									lv_lastValidValue = 0.0;
								} else {
									lv_lastValidValue = 1.0;
								}
								cypherquerry = "start n=NODE({node_id}) MATCH n-[:HasData]->data WHERE data.timestamp>={lv_starttime_per} AND data.timestamp<={lv_endtime_per} AND data.datapoint_name={datapoint_name}  RETURN COUNT(data) AS count;";
								result = execute_eng.execute(cypherquerry,
										params);
								Iterator<Long> iterator3 = result
										.columnAs("count");
								lv_quality = Double.parseDouble(iterator3
										.next().toString());
								lv_countPeriod = 1;

							} else {

								// #if there are no values in current period set
								// lv_lastValidValue to 1
								lv_countPeriod = lv_countPeriod + 1;
								lv_quality = 1.0 / lv_countPeriod;
								lv_lastValidValue = 1.0;

							}
							// #assigning of value (result of last period or if
							// there was no value 1) to the endtime of the
							// period
							temp = new data_periodic(lv_lastValidValue,
									lv_endtime_per.toString(),
									p_datapoint_name, lv_quality);
							temp_data_periodic.add(temp);
							// #shift to next period
							// #lv_endtime_per+1 because SQL BETWEEN function
							// includes the limits to (closed set)

							lv_starttime_per = new Timestamp(
									lv_endtime_per.getTime() + 1);
							lv_endtime_per = new Timestamp(
									lv_endtime_per.getTime()
											+ Math.round(p_period));

						}
						break;

					case 3:

						// #---------------------------------------------------------------------------------------------
						// #--------------------------- mode = 3 (forced 1 /
						// default 0)----------------------------------
						// #---------------------------------------------------------------------------------------------

						// #algorithm for forced 1 (if there is at least one 1
						// in the period the periodic value is 1) and default 0
						// (if there is no value in the period the periodic
						// value is 0)

						while ((end.getTime() - lv_endtime_per.getTime()) >= 0) {

							// #check if there are values in current period
							params.clear();
							params.put("node_id", datapoint.getId());
							params.put("lv_starttime_per",
									lv_starttime_per.toString());
							params.put("lv_endtime_per",
									lv_endtime_per.toString());
							params.put("datapoint_name", p_datapoint_name);
							cypherquerry = "start n=NODE({node_id}) MATCH n-[:HasData]->data WHERE data.timestamp>={lv_starttime_per} and data.timestamp<={lv_endtime_per} and data.datapoint_name={datapoint_name} RETURN DISTINCT data.datapoint_name AS datapoint_name;";
							result = execute_eng.execute(cypherquerry, params);
							Iterator<String> iterator = result
									.columnAs("datapoint_name");
							if (iterator.hasNext()) {
								// #check if there is at least one zero in
								// current period and set lv_lastValidValue to 0
								// if so, to 1 if there is no zero in current
								// period

								cypherquerry = "start n=NODE({node_id}) MATCH n-[:HasData]->data WHERE data.timestamp>={lv_starttime_per} AND data.timestamp<={lv_endtime_per} AND data.datapoint_name={datapoint_name} AND data.value<>0 RETURN DISTINCT data.datapoint_name AS datapoint_name;";
								result = execute_eng.execute(cypherquerry,
										params);
								iterator = result.columnAs("datapoint_name");
								if (iterator.hasNext()) {

									lv_lastValidValue = 1.0;
								} else {
									lv_lastValidValue = 0.0;
								}
								cypherquerry = "start n=NODE({node_id}) MATCH n-[:HasData]->data WHERE data.timestamp>={lv_starttime_per} AND data.timestamp<={lv_endtime_per} AND data.datapoint_name={datapoint_name} RETURN COUNT(data) AS count;";
								result = execute_eng.execute(cypherquerry,
										params);
								Iterator<Long> iterator3 = result
										.columnAs("count");
								lv_quality = Double.parseDouble(iterator3
										.next().toString());
								lv_countPeriod = 1;

							} else {

								// #if there are no values in current period set
								// lv_lastValidValue to 0
								lv_countPeriod = lv_countPeriod + 1;
								lv_quality = 1.0 / lv_countPeriod;
								lv_lastValidValue = 0.0;

							}
							// #assigning of value (result of last period or if
							// there was no value 0) to the endtime of the
							// period

							temp = new data_periodic(lv_lastValidValue,
									lv_endtime_per.toString(),
									p_datapoint_name, lv_quality);
							temp_data_periodic.add(temp);
							// #shift to next period
							// #lv_endtime_per+1 because SQL BETWEEN function
							// includes the limits to (closed set)
							lv_starttime_per = new Timestamp(
									lv_endtime_per.getTime() + 1);
							lv_endtime_per = new Timestamp(
									lv_endtime_per.getTime()
											+ Math.round(p_period));

						}
						break;
					default:
						System.out
								.println("Wanted mode not supported by this stored procedure! Allowed modes: 1,2,3 (and corresponding modes above 1000)");
						break;

					}

				}

				if (lv_outputEnabled) {
					return temp_data_periodic;
				} else {
					return null;
				}

			}

		} catch (Exception e) {
			System.out
					.println("Error occured in getValuesPerodicBinary procedure");
			e.printStackTrace();
			return null;
		}

	}

	public ArrayList<data_periodic> getValuesPeriodicAnalog(
			String p_datapoint_name, String p_starttime, String p_endtime,
			Double p_period, int p_mode) {

		// Variables
		Timestamp lv_firstDate;
		Double lv_firstValue;
		Double lv_lastValidValue;
		Double lv_currentValue;
		Double lv_currentValueMax;
		Double lv_quality;
		Timestamp lv_starttime_per;
		Timestamp lv_endtime_per;
		Timestamp lv_starttime_interpol = null;
		Timestamp lv_lastValidValueTimestamp;
		Boolean lv_lastperiode_was_valid;
		Integer lv_countPeriod;
		Boolean lv_outputEnabled;
		Double[] calcAvgOutput;
		Node datapoint;
		Timestamp start;
		Timestamp end;

		String cypherquerry = "";
		Map<String, Object> params = new HashMap<String, Object>();

		try {
			start = Timestamp.valueOf(p_starttime);
			end = Timestamp.valueOf(p_endtime);

		} catch (Exception e) {
			System.out.println("Invalid format of the Timestamp Strings");
			return null;
		}

		// #initialize local variables if needed
		lv_outputEnabled = true;
		// #check if given dates(time) are allowed
		if ((end.getTime() - start.getTime()) < p_period) {
			System.out
					.println("endtime must be later than starttime + one periode");
			return null;
		} else {
			// #check if datapoint_name exists
			datapoint = this.getDatapointByName(p_datapoint_name);
			if (datapoint == null) {
				System.out
						.println("given datapoint_name does not exist in database(in table datapoint)");
				return null;
			} else {

				data_periodic temp;
				ArrayList<data_periodic> temp_data_periodic = new ArrayList<data_periodic>();

				// #TODO: instead of the following query also the first SET
				// lv_firstValue=... (some lines beneath this) could be used and
				// cached (because it is needed anywayin most cases). If the
				// returned value is NULL, no values are in the database

				if (this.getNumberofValues(p_datapoint_name, p_starttime,
						p_endtime) == 0) {

					temp = new data_periodic(null, p_starttime,
							p_datapoint_name, 0.0);
					temp_data_periodic.add(temp);

					lv_starttime_per = new Timestamp(start.getTime() + 1);
					lv_endtime_per = new Timestamp(start.getTime()
							+ Math.round(p_period));

					// #do calculations while endtime for next periode sooner
					// than final endtime

					while ((end.getTime() - lv_endtime_per.getTime()) >= 0) {

						temp = new data_periodic(null,
								lv_endtime_per.toString(), p_datapoint_name,
								0.0);
						temp_data_periodic.add(temp);

						// #shift to next periode
						// #lv_endtime_per+1 because SQL BETWEEN function
						// includes the limits to (closed set)

						lv_starttime_per = new Timestamp(
								lv_endtime_per.getTime() + 1);
						lv_endtime_per = new Timestamp(lv_endtime_per.getTime()
								+ Math.round(p_period));

					}

				} else {

					// #---------------------------------------------------------------------------------------------
					// #-------------------------------- start of main algorithm
					// ------------------------------------
					// #---------------------------------------------------------------------------------------------

					lv_starttime_per = new Timestamp(start.getTime() + 1);
					lv_endtime_per = new Timestamp(start.getTime()
							+ Math.round(p_period));

					Node[] data = this.getValues(p_datapoint_name, p_starttime,
							p_endtime);

					// -- get first date in given timeslot of given
					// datapoint_name
					lv_firstDate = Timestamp.valueOf(data[0].getProperty(
							"timestamp").toString());

					// #the first value (the value with the earliest date) is
					// directly interpreted as the value at the starttime (no
					// averageing/interpolation,
					// #because averaging/interpolation for a certain timepoint
					// is done with the periode before)

					lv_firstValue = Double.parseDouble(data[0].getProperty(
							"value").toString());

					// #the values in the table data_periodic are temporary, so
					// the values in this table need to be delted
					lv_lastValidValue = lv_firstValue;
					lv_lastperiode_was_valid = true;
					lv_countPeriod = 1;

					// #set if the generated values shall be returned to calling
					// function (by the SELECT statement at the end of this SP)

					if (p_mode > 1000) {

						lv_outputEnabled = false;
						p_mode = p_mode - 1000;
					}
					// #insert first value in table data_periodic
					temp = new data_periodic(lv_firstValue, p_starttime,
							p_datapoint_name, 0.0);
					temp_data_periodic.add(temp);

					// #---------------------------------------------------------------------------------------------
					// #-------------- mode = 1 (weighted averaging and linear
					// interpolation)------------------------
					// #---------------------------------------------------------------------------------------------

					switch (p_mode) {

					case 1:
						// #mode 1:
						// #algorithm for weighted averaging and linear
						// interpolation of the values
						// #do calculations while endtime for next periode
						// sooner than final endtime
						while ((end.getTime() - lv_endtime_per.getTime()) >= 0) {
							if (lv_lastperiode_was_valid) {

								// #check if avg value of periode exists (in
								// other words, if there are values in this
								// periode given)
								// #TODO: test if DISTINCT has an effect, don't
								// think so (but could be needed to prevent
								// errors or confounded with LIMIT 1)
								// #lv_starttime_per+1 done at the end of the
								// loop. Needed because SQL BETWEEN function
								// includes the limits to (closed set)
								params.clear();
								params.put("node_id", datapoint.getId());
								params.put("lv_starttime_per",
										lv_starttime_per.toString());
								params.put("lv_endtime_per",
										lv_endtime_per.toString());
								params.put("datapoint_name", p_datapoint_name);
								cypherquerry = "start n=NODE({node_id}) MATCH n-[:HasData]->data WHERE data.timestamp>={lv_starttime_per} AND data.timestamp<={lv_endtime_per} AND data.datapoint_name={datapoint_name} RETURN DISTINCT data.datapoint_name AS datapoint_name;";
								result = execute_eng.execute(cypherquerry,
										params);
								Iterator<String> iterator = result
										.columnAs("datapoint_name");
								if (iterator.hasNext()) {

									calcAvgOutput = this.calcAverageWeighted(
											p_datapoint_name,
											lv_starttime_per.toString(),
											lv_endtime_per.toString(),
											lv_lastValidValue);
									lv_currentValue = calcAvgOutput[0];
									lv_lastValidValue = calcAvgOutput[1];
									lv_quality = calcAvgOutput[2];
									// #calculated average value is assigned to
									// the endtime of the periode
									// #quality is equal to the number of values
									// in current periode (many values -> high
									// quality)
									temp = new data_periodic(lv_currentValue,
											lv_endtime_per.toString(),
											p_datapoint_name, lv_quality);
									temp_data_periodic.add(temp);
								} else {

									lv_lastperiode_was_valid = false;
									lv_starttime_interpol = new Timestamp(
											lv_starttime_per.getTime() - 1);
								}

							} else {

								// #TODO: test if DISTINCT has an effect, don't
								// think so (but could be needed to prevent
								// errors or confounded with LIMIT 1)
								params.clear();
								params.put("node_id", datapoint.getId());
								params.put("lv_starttime_per",
										lv_starttime_per.toString());
								params.put("lv_endtime_per",
										lv_endtime_per.toString());
								params.put("datapoint_name", p_datapoint_name);
								cypherquerry = "start n=Node({node_id}) MATCH n-[:HasData]->data WHERE data.timestamp>={lv_starttime_per} AND data.timestamp<{lv_endtime_per} AND data.datapoint_name={datapoint_name} RETURN DISTINCT data.datapoint_name AS datapoint;";
								result = execute_eng.execute(cypherquerry,
										params);
								Iterator<String> iterator = result
										.columnAs("datapoint");
								if (iterator.hasNext()) {
									lv_lastperiode_was_valid = true;
									// -- interpolate algorithm
									//
									// -- lv_endtime_per is the timepoint of the
									// current valid value
									// -- startvalue for weighted average is
									// NULL, so there is no and it is ignored by
									// stored procedure calcAverageWeighted

									// #lv_firstValue because returned value can
									// not be assigned to lv_lastValidValue
									// because this is the startvalue needed for
									// the interpolation
									// #perhaps it would be better to use normal
									// averaging in this mode!?

									Double[] output = this.calcAverageWeighted(
											p_datapoint_name,
											lv_starttime_per.toString(),
											lv_endtime_per.toString(), null);
									lv_currentValue = output[0];
									lv_firstValue = output[1];
									lv_quality = output[2];

									this.interpolateValuesLinear(
											p_datapoint_name,
											lv_starttime_per.toString(),
											lv_endtime_per.toString(),
											p_period, lv_lastValidValue,
											lv_currentValue, temp_data_periodic);

									lv_lastValidValue = lv_currentValue;

								}

							}

							// #shift to next periode
							// #lv_endtime_per+1 because SQL BETWEEN function
							// includes the limits to (closed set)
							lv_starttime_per = new Timestamp(
									lv_endtime_per.getTime() + 1);
							lv_endtime_per = new Timestamp(
									lv_endtime_per.getTime()
											+ Math.round(p_period));

						}

						if (lv_lastperiode_was_valid == false) {

							 this.interpolateValuesLinear(p_datapoint_name,
							 lv_starttime_interpol.toString(),
							 lv_starttime_per.toString(), p_period,
							 lv_lastValidValue,
							 lv_lastValidValue,temp_data_periodic);
							//System.out
								//	.println("lv_starttime_interpol is not initialized");
						}
						break;

					case 2:
						// #---------------------------------------------------------------------------------------------
						// #-------------- mode = 2 (weighted averaging / sample
						// & hold)---------------------------------
						// #---------------------------------------------------------------------------------------------

						// #mode 2:
						// #algorithm for weighted averaging and sample & hold
						// of values

						while ((end.getTime() - lv_endtime_per.getTime()) >= 0) {
							// #do calculations while endtime for next periode
							// sooner than final endtime
							// #check if there are values in current periode
							// #lv_starttime_per+1 done at the end of the loop.
							// Needed because SQL BETWEEN function includes the
							// limits to (closed set)
							// #TODO: check if DISTINCT has an effect or LIMIT 1
							// needed/better!
							//

							params.clear();
							params.put("node_id", datapoint.getId());
							params.put("lv_starttime_per",
									lv_starttime_per.toString());
							params.put("lv_endtime_per",
									lv_endtime_per.toString());
							params.put("datapoint_name", p_datapoint_name);
							cypherquerry = "start n=NODE({node_id}) MATCH n-[:HasData]->data WHERE data.timestamp>={lv_starttime_per} AND data.timestamp<={lv_endtime_per} AND data.datapoint_name={datapoint_name} RETURN DISTINCT data.datapoint_name AS datapoint_name;";
							result = execute_eng.execute(cypherquerry, params);
							Iterator<String> iterator = result
									.columnAs("datapoint_name");
							if (iterator.hasNext()) {
								// #calculate weighted average value of last
								// periode
								Double[] output = this.calcAverageWeighted(
										p_datapoint_name,
										lv_starttime_per.toString(),
										lv_endtime_per.toString(),
										lv_lastValidValue);
								lv_currentValue = output[0];
								lv_lastValidValue = output[1];
								lv_quality = output[2];

								lv_countPeriod = 1;
								// #assigning of current value (calculated
								// result of last period) to the endtime of the
								// period
								temp = new data_periodic(lv_currentValue,
										lv_endtime_per.toString(),
										p_datapoint_name, lv_quality);
								temp_data_periodic.add(temp);
							} else {

								lv_countPeriod = lv_countPeriod + 1;
								lv_quality = 1.0 / (lv_countPeriod);

								// #assigning of value (the last entry of the
								// last period that was valid (hold)) to the
								// endtime of the current period
								temp = new data_periodic(lv_lastValidValue,
										lv_endtime_per.toString(),
										p_datapoint_name, lv_quality);
								temp_data_periodic.add(temp);

							}
							// #shift to next periode
							// #lv_endtime_per+1 because SQL BETWEEN function
							// includes the limits to (closed set)

							lv_starttime_per = new Timestamp(
									lv_endtime_per.getTime() + 1);
							lv_endtime_per = new Timestamp(
									lv_endtime_per.getTime()
											+ Math.round(p_period));
						}
						break;

					case 3:
						// #---------------------------------------------------------------------------------------------
						// #----------------------- mode = 3 (difference value /
						// zero)-----------------------------------
						// #---------------------------------------------------------------------------------------------

						// #mode 3:
						// #algorithm for difference value and zero

						while ((end.getTime() - lv_endtime_per.getTime()) >= 0) {

							// #get quality (number of measurements) for current
							// period
							params.clear();
							params.put("node_id", datapoint.getId());
							params.put("lv_starttime_per",
									lv_starttime_per.toString());
							params.put("lv_endtime_per",
									lv_endtime_per.toString());
							params.put("datapoint_name", p_datapoint_name);
							cypherquerry = "START n=NODE({node_id}) MATCH n-[:HasData]->data WHERE data.timestamp>={lv_starttime_per} AND data.timestamp<={lv_endtime_per} AND data.datapoint_name={datapoint_name} RETURN COUNT(data) AS count;";
							result = execute_eng.execute(cypherquerry, params);
							Iterator<Long> iterator = result.columnAs("count");
							lv_quality = new Double(iterator.next());
							// #check if there are values in current periode

							// #lv_starttime_per+1 done at the end of the loop.
							// Needed because SQL BETWEEN function includes the
							// limits to (closed set)
							// #TODO: check if DISTINCT has an effect or LIMIT 1
							// needed/better!

							cypherquerry = "START n=NODE({node_id}) MATCH n-[:HasData]->data WHERE data.timestamp>={lv_starttime_per} AND data.timestamp<={lv_endtime_per} AND data.datapoint_name={datapoint_name} RETURN DISTINCT data.datapoint_name AS datapoint_name;";
							result = execute_eng.execute(cypherquerry, params);
							Iterator<String> iterator2 = result
									.columnAs("datapoint_name");
							if (iterator2.hasNext()) {

								// #calculate difference value of last (highest)
								// value with last valid value (last value
								// before this period)
								params.clear();
								params.put("node_id", datapoint.getId());
								params.put("lv_endtime_per",
										lv_endtime_per.toString());
								params.put("datapoint_name", p_datapoint_name);
								cypherquerry = "START n=NODE({node_id}) MATCH n-[:HasData]->data WHERE data.timestamp<={lv_endtime_per} AND data.datapoint_name={datapoint_name} RETURN data ORDER BY data.timestamp DESC LIMIT 1;";
								result = execute_eng.execute(cypherquerry,
										params);
								Iterator<Node> iterator3 = result
										.columnAs("data");
								lv_currentValueMax = Double
										.parseDouble(iterator3.next()
												.getProperty("value")
												.toString());

								lv_currentValue = lv_currentValueMax
										- lv_lastValidValue;

								// #TODO: check if lv_currentValue is negative
								// (could happen if counter overflow)
								// #assigning of current value (calculated
								// result of last period) to the endtime of the
								// period
								temp = new data_periodic(lv_currentValue,
										lv_endtime_per.toString(),
										p_datapoint_name, lv_quality);
								temp_data_periodic.add(temp);
								lv_lastValidValue = lv_currentValueMax;

							} else {

								// #assigning of value (zero) to the endtime of
								// the current period
								temp = new data_periodic(0.0,
										lv_endtime_per.toString(),
										p_datapoint_name, lv_quality);

							}
							// #shift to next periode
							// #lv_endtime_per+1 because SQL BETWEEN function
							// includes the limits to (closed set)

							lv_starttime_per = new Timestamp(
									lv_endtime_per.getTime() + 1);
							lv_endtime_per = new Timestamp(
									lv_endtime_per.getTime()
											+ Math.round(p_period));

						}

						break;

					default:
						System.out
								.println("Wanted mode not supported by this stored procedure! Allowed modes: 1,2,3 (and corresponding modes above 1000)");
						break;

					}

				}

				if (lv_outputEnabled) {

					return temp_data_periodic;

				} else {
					return null;

				}

			}
		}

	}

	public void interpolateValuesLinear(String p_datapoint_name,
			String p_starttime, String p_endtime, Double p_period,
			Double p_value_starttime, Double p_value_endtime,
			ArrayList<data_periodic> existing_data_periodic) {

		Timestamp start;
		Timestamp end;

		Timestamp lv_starttime_per;
		Timestamp lv_endtime_per;
		Double lv_value;
		Double lv_k;
		Double lv_x;
		Double lv_d;
		Double lv_delta_x;
		Double lv_quality;
		String cypherquerry = "";
		Map<String, Object> params = new HashMap<String, Object>();

		Node datapoint;
		data_periodic temp;
		ArrayList<data_periodic> temp_data_periodic = existing_data_periodic;

		try {

			start = Timestamp.valueOf(p_starttime);
			end = Timestamp.valueOf(p_endtime);
		} catch (Exception e) {
			System.out.println("Invalid Datetime format");
			return;
		}

		try {
			datapoint = this.getDatapointByName(p_datapoint_name);
			lv_starttime_per = start;
			lv_endtime_per = new Timestamp(lv_starttime_per.getTime()
					+ Math.round(p_period));
			// -- y = k*x + d

			lv_delta_x = (end.getTime() - start.getTime()) / 1000.0;
			lv_k = (p_value_endtime - p_value_starttime) / lv_delta_x;
			lv_x = p_period;
			lv_d = p_value_starttime;
			// #quality depands on length of interpolation (long interpolation
			// -> lower quality)
			lv_quality = (p_period / lv_delta_x);

			while ((end.getTime() - lv_endtime_per.getTime()) >= 0) {
				// #do calculations while endtime for next periode sooner than
				// final endtime
				// -- y = k*x + d
				lv_value = lv_k * lv_x + lv_d;

				temp = new data_periodic(lv_value, lv_endtime_per.toString(),
						p_datapoint_name, lv_quality);
				temp_data_periodic.add(temp);
				lv_endtime_per = new Timestamp(lv_endtime_per.getTime()
						+ Math.round(p_period));
				lv_x = lv_x + p_period;

			}
			params.put("node_id", datapoint.getId());
			params.put("datapoint_name", p_datapoint_name);
			params.put("p_endtime", p_endtime);
			params.put("starttime_1",
					new Timestamp(start.getTime() + 1).toString());
			cypherquerry = "start n=Node({node_id}) MATCH n-[:HasData]->data WHERE data.timestamp>={starttime_1} AND data.timestamp<={p_endtime} AND data.datapoint_name={datapoint_name} RETURN COUNT(data) AS count;";
			result = execute_eng.execute(cypherquerry, params);
			Iterator<Long> iterator = result.columnAs("count");
			lv_quality = new Double(iterator.next());

		} catch (Exception e) {
			System.out
					.println("Error Occured in the Interpolate values linear method...");
			e.printStackTrace();
			return;
		}

	}

	public Double[] calcAverageWeighted(String p_datapoint_name,
			String p_starttime, String p_endtime, Double p_startvalue) {

		// VARIABLES

		Integer lv_index;
		Integer lv_indexMax;
		Integer lv_timedurationSum;
		Timestamp lv_timestamp_1;
		Timestamp lv_timestamo_2;

		Timestamp start;
		Timestamp end;

		String cypherquerry = "";
		Map<String, Object> params = new HashMap<String, Object>();
		Node datapoint;

		// TO BE RETURNED...

		// Return Values
		Double p_currentValue; // output[0];
		Double p_lastValidValue; // output[1];
		Integer p_quality; // output[2];

		Double[] output = new Double[3];

		datapoint = this.getDatapointByName(p_datapoint_name);
		if (datapoint == null) {
			System.out.println("Datapoint not found inthe database..!");
			return null;
		}

		try {
			start = Timestamp.valueOf(p_starttime);
			end = Timestamp.valueOf(p_endtime);

		} catch (Exception e) {
			System.out.println("Invalid Timestamp format...!");
			return null;
		}

		lv_index = 0;
		data_calcAverageWeighted temp;
		ArrayList<data_calcAverageWeighted> temp_data_calcAverageWeighted = new ArrayList<data_calcAverageWeighted>();

		try {

			// #insert startdata (given from calling function) in (temporary)
			// table data_calcAverageWeighted
			temp = new data_calcAverageWeighted(p_startvalue, p_starttime, null);
			temp_data_calcAverageWeighted.add(temp);

			// #copy data that shall be weighted into seperate (temporary) table
			// data_calcAverageWeighted
			params.put("node_id", datapoint.getId());
			params.put("p_starttime", p_starttime);
			params.put("p_endtime", p_endtime);
			params.put("p_datapoint_name", p_datapoint_name);
			cypherquerry = "START n=NODE({node_id}) MATCH n-[:HasData]->data WHERE data.timestamp>={p_starttime} AND data.timestamp<={p_endtime} AND data.datapoint_name={p_datapoint_name} RETURN data;";
			result = execute_eng.execute(cypherquerry, params);
			Iterator<Node> iterator = result.columnAs("data");
			Node data_temp;
			while (iterator.hasNext()) {

				data_temp = iterator.next();
				temp = new data_calcAverageWeighted(
						Double.parseDouble(data_temp.getProperty("value")
								.toString()), data_temp
								.getProperty("timestamp").toString(), null);
				temp_data_calcAverageWeighted.add(temp);
			}

			// #insert last data into table data_calcAverageWeighted. Only
			// timestamp needed because of calculation of the duration of the
			// last value
			temp = new data_calcAverageWeighted(0.0, p_endtime, 0);
			temp_data_calcAverageWeighted.add(temp);

			// get size of the array list
			lv_indexMax = temp_data_calcAverageWeighted.size();

			while (lv_index + 1 < lv_indexMax) {

				lv_timestamp_1 = Timestamp
						.valueOf(temp_data_calcAverageWeighted.get(lv_index)
								.getTimestamp());
				lv_timestamo_2 = Timestamp
						.valueOf(temp_data_calcAverageWeighted
								.get(lv_index + 1).getTimestamp());
				temp_data_calcAverageWeighted.get(lv_index).setTimeDuration(
						(int) ((lv_timestamp_1.getTime() - lv_timestamo_2
								.getTime()) / 1000));
				lv_index = lv_index + 1;
			}

			if (p_startvalue == null) {
				temp_data_calcAverageWeighted.get(0).setTimeDuration(0);
				temp_data_calcAverageWeighted.get(0).setValue(0.0);
			}

			// #calculate weighted average value
			lv_timedurationSum = 0;
			for (int i = 0; i < lv_indexMax; i++) {
				if (temp_data_calcAverageWeighted.get(i).getTimeDuration() != null) {
					lv_timedurationSum = lv_timedurationSum
							+ temp_data_calcAverageWeighted.get(i)
									.getTimeDuration();
				}
			}

			if (lv_timedurationSum == 0) {
				// #TODO: could cause problems if no values are in current
				// period (lv_indexMax-1 then 0!?), test if this can happen!
				p_currentValue = temp_data_calcAverageWeighted.get(
						lv_indexMax - 2).getValue();

			} else {
				p_currentValue = 0.0;
				for (int i = 0; i < temp_data_calcAverageWeighted.size(); i++) {

					if (temp_data_calcAverageWeighted.get(i).getTimeDuration() != null) {
						p_currentValue = p_currentValue
								+ (temp_data_calcAverageWeighted.get(i)
										.getValue() * temp_data_calcAverageWeighted
										.get(i).getTimeDuration());
					}

				}
			}

			// #TODO: could cause problems if no values are in current period
			// (lv_indexMax-1 then 0!?), test if this can happen!
			p_lastValidValue = temp_data_calcAverageWeighted.get(
					lv_indexMax - 2).getValue();

			// #set quality, not full correct if more than one dataset for the
			// same timestamp of an datapoint (but this should not be too often)
			p_quality = lv_index - 1;
			output[0] = p_currentValue;
			output[1] = p_lastValidValue;
			output[2] = new Double(p_quality);

			return output;

		} catch (Exception e) {
			System.out
					.println("Error Occured in the calcAverageWeighted Procedure");
			return null;
		}

	}

	
	public ArrayList<data_periodic> getValuesPeriodic(String p_datapoint_name,String p_starttime,String p_endtime,Double p_period,Integer p_mode){
		
		Timestamp start;
		Timestamp end;
		Node datapoint;
		
		try{
			start=Timestamp.valueOf(p_starttime);
			end=Timestamp.valueOf(p_endtime);
			
		}catch(Exception e){
			System.out.println("Invalid datetime formats...");
		    return null;
		}
		
		//#check if given dates(time) are allowed
		if((end.getTime()-start.getTime())<p_period){
			System.out.println("endtime must be later than starttime + one periode");
			return null;
		}else{
			
			datapoint=this.getDatapointByName(p_datapoint_name);
			if(datapoint==null){
				System.out.println("given datapoint_name does not exist in database(in table datapoint)");
				return null;
			}else{
				
				if(p_mode==0 || p_mode==null){
					p_mode=1;
				}
				
			}
			
		}
		
		data_periodic temp;
		ArrayList<data_periodic> temp_data_periodic=null;
		
		//#input data should be correct, now starting algorithm for choosing appropriate (depending on type, more exactly the unit of the datapoint) stored procedure
		
		if(datapoint.getProperty("unit").toString().equals("boolean")){
			
			temp_data_periodic=this.getValuesPerodicBinary(p_datapoint_name, p_starttime, p_endtime, p_period, p_mode);
			
		}else{
			temp_data_periodic=this.getValuesPeriodicAnalog(p_datapoint_name, p_starttime, p_endtime, p_period, p_mode);
			
			
		}
		
		return temp_data_periodic;
		
		
		
	}
	
	
	
	
	
	
	
	
	
	
	
}

// TEMP TABLE FOR data_periodic. USED in getvaluePeriodicBinary Functions
class data_periodic {
	Double value;
	String timestamp;
	Double quality;
	String datapoint_name;

	public data_periodic(Double value, String timestamp, String datapoint_name,
			Double quality) {
		this.value = value;
		this.timestamp = timestamp;
		this.quality = quality;
		this.datapoint_name = datapoint_name;
	}

}// This will be treated as the Temp table
//

class data_calcAverageWeighted {

	Double value;
	String timestamp;
	Integer timeduration;

	public data_calcAverageWeighted(Double value, String timestamp,
			Integer timeduration) {
		this.value = value;
		this.timestamp = timestamp;
		this.timeduration = timeduration;

	}

	public String getTimestamp() {
		return this.timestamp;
	}

	public Integer getTimeDuration() {
		return this.timeduration;
	}

	public Double getValue() {
		return this.value;
	}

	public void setTimeDuration(Integer timeduration) {
		this.timeduration = timeduration;
	}

	public void setValue(Double value) {
		this.value = value;
	}

}
