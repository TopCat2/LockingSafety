package com.bjss.plynn.nioLockingDemo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

/*
 * Trivial baseline for JDBC code
 */
public class JdbcBaseline
{
    static final String DBPROPERTIESFILE = "db.properties";
    public static final String COLNAME_CLAIMED = "got_it";
    public static final String COLNAME_PREVSTATUS = "prev_status";
    public static final String COLNAME_FILENAME = "name";
    public static final String COLNAME_MISSING = "file_missing";
    static Logger myLog = LoggerFactory.getLogger(JdbcBaseline.class);

    static public void main(String args[]) throws SQLException, ClassNotFoundException, IOException
    {

        if (args.length < 1)
        {
            myLog.error("Must enter a file name; there are no arguments.");
            System.exit(1);
        }
        ConnectToDB(DBPROPERTIESFILE);
    }

    public static void ConnectToDB(String dbPropFile) throws IOException, ClassNotFoundException, SQLException
    {
        Properties prop = new Properties();
        InputStream iStream = JdbcBaseline.class.getClassLoader().getResourceAsStream(dbPropFile);
        if (iStream == null)
        {
            throw new FileNotFoundException("Property file " + DBPROPERTIESFILE + "was not found in the classpath");
        }
        prop.load(iStream);

        //  Database credentials"
        final String USER = prop.getProperty("db.jdbc.user", "NULL");
        final String PASS = prop.getProperty("db.jdbc.password", "NULL");
        final String JDBC_DRIVER = prop.getProperty("db.jdbc.driver", "NULL");
        final String DB_URL = prop.getProperty("db.jdbc.url", "NULL");

        Connection conn;
        Statement stmt;

        //Register JDBC driver
        Class.forName(JDBC_DRIVER);

        //Open a connection
        myLog.debug("Connecting to database");
        conn = DriverManager.getConnection(DB_URL, USER, PASS);
        showDbFiles(conn);

        System.out.println("---------------------------------------------------------");
        Boolean tt = claimFile(conn, "peter.txt");
        myLog.debug("claimFile returned " + tt);
        showDbFiles(conn);
        if (! tt)
        {
            myLog.info("File was already completed; nothing to do.");
            return;
        }

        System.out.println("---------------------------------------------------------");
        completeFile(conn, "peter.txt");
        showDbFiles(conn);
    }

    private static void showDbFiles(Connection conn) throws SQLException
    {
        Statement stmt;//Execute a query
        myLog.debug("Creating statement for display");
        stmt = conn.createStatement();
        String sql;
        sql = "SELECT file_name, execution_count, execution_status, begin_time, "
                + "complete_time, run_node, run_pid FROM files_processed";
        ResultSet rs = stmt.executeQuery(sql);

        while(rs.next()){
            //Retrieve by column name
            int count  = rs.getInt("execution_count");
            String fileName = rs.getString("file_name");
            String executionStatus = rs.getString("execution_status");
            Timestamp finished = rs.getTimestamp("complete_time");

            //Display values
            System.out.print("file " + fileName);
            System.out.print(" ran " + count + " times and is " + executionStatus);
            if (finished != null)
            {
                System.out.print(" at " + finished);
            }
            System.out.println();
        }
    }

    private static Boolean claimFile(Connection conn, String file) throws SQLException
    {
        myLog.debug("Creating statement for claiming");

        CallableStatement doPC = conn.prepareCall("{call claim_file (?, ?, ?)}");
        doPC.setString(COLNAME_FILENAME, file);
        doPC.registerOutParameter(COLNAME_CLAIMED, Types.BOOLEAN);
        doPC.registerOutParameter(COLNAME_PREVSTATUS, Types.VARCHAR);
        doPC.executeUpdate();
        myLog.debug("File " + file + " was previously " + doPC.getString(COLNAME_PREVSTATUS)
                + ". The return from the stored procedure execution was " +  doPC.getBoolean(COLNAME_CLAIMED));

        return doPC.getBoolean(COLNAME_CLAIMED);
    }

    private static void failFile(Connection conn, String file) throws SQLException
    {
        myLog.debug("Creating statement for failing");

        CallableStatement doPC = conn.prepareCall("{call fail_file (?, ?)}");
        doPC.setString(COLNAME_FILENAME, file);
        doPC.registerOutParameter(COLNAME_MISSING, Types.BOOLEAN);
        doPC.executeUpdate();
        myLog.debug("File " + file + " was marked failed.");

        if (doPC.getBoolean(COLNAME_MISSING)) {
            throw new IllegalStateException("Attempt to fail file " + file +" but it was not found in the database");
        }

        return;
    }

    private static void completeFile(Connection conn, String file) throws SQLException
    {
        myLog.debug("Creating statement for failing");

        CallableStatement doPC = conn.prepareCall("{call complete_file (?, ?)}");
        doPC.setString(COLNAME_FILENAME, file);
        doPC.registerOutParameter(COLNAME_MISSING, Types.BOOLEAN);
        doPC.executeUpdate();
        myLog.debug("File " + file + " was marked completed.");

        if (doPC.getBoolean(COLNAME_MISSING)) {
            throw new IllegalStateException("Attempt to complete file " + file +" but it was not found in the database");
        }

        return;
    }

}