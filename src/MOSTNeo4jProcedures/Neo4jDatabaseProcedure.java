
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

    Start NODE is created when we create the database.All the Zones will be
    attached to the Start Node.

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

            starttime = Timestamp.valueOf(p_starttime);
            endtime = Timestamp.valueOf(p_endtime);
            timeDiff = (int) (endtime.getTime() - starttime.getTime()) / 1000;
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
                        data = null;
                        if (iterator.hasNext()) {
                            data[0] = iterator.next();
                            return data;
                        }

                    } else {
                        params.clear();
                        params.put("node_id", datapoint.getId());
                        params.put("endtime", p_endtime);
                        cypherQuerry = "START n= NODE({node_id}) MATCH n-[:HasData]->data WHERE data.timestamp <={endtime} RETURN data ORDER BY data.timestamp DESC LIMIT 1;";
                        result = execute_eng.execute(cypherQuerry, params);
                        Iterator<Node> iterator = result.columnAs("data");
                        data = null;
                        if (iterator.hasNext()) {
                            data[0] = iterator.next();
                            return data;
                        }

                    }

                } else if (p_endtime == null) {
                    params.clear();
                    params.put("node_id", datapoint.getId());
                    params.put("starttime", p_starttime);
                    cypherQuerry = "START n=NODE ({node_id}) MATCH n-[:HasData]->data WHERE data.timestamp>={starttime} RETURN data ORDER BY data.timestamp ASC;";
                    result = execute_eng.execute(cypherQuerry, params);
                    Iterator<Node> iterator = result.columnAs("data");
                    data = null;
                    if (iterator.hasNext()) {
                        data[0] = iterator.next();
                        return data;
                    }

                } else {
                    params.clear();
                    params.put("node_id", datapoint.getId());
                    params.put("starttime", p_starttime);
                    params.put("endtime", p_endtime);
                    cypherQuerry = "START n=NODE ({node_id}) MATCH n-[:HasData]->data WHERE data.timestamp>={starttime} AND data.timestamp<={endtime} RETURN data ORDER BY data.timestamp;";
                    result = execute_eng.execute(cypherQuerry, params);
                    Iterator<Node> iterator = result.columnAs("data");
                    data = null;
                    int i = 0;
                    while (iterator.hasNext()) {
                        data[i] = iterator.next();
                        i++;
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


}
