/**
 * This file is for testing purposes only. It checks the Oracle connection.
 *
 * @author  Petya Vasileva                                                                                                                                
 * @version 1.5                                                                                                                                         
 * @since   2020-01-26                                                                                                                            
 */

import java.awt.List;
import java.lang.reflect.Array;
import oracle.jdbc.pool.OracleDataSource;                                                                                                               
import java.io.*;
import java.sql.*;
import java.util.*;                                                                                                          

public class TryConn {
    
    public static void main(String[] args) throws Exception {
	
	System.out.println("--- Oracle JDBC Connection ------");	
	try {
	    Class.forName("oracle.jdbc.driver.OracleDriver");
	} catch (ClassNotFoundException e) {	    
	    System.out.println("Where is your Oracle JDBC Driver?");
	    e.printStackTrace();
	    return;
	}
	
	System.out.println("Oracle JDBC Driver Registered!");
	
	Connection conn = null;
	try {
	    Properties prop = new Properties();
	    prop.load(new FileInputStream("data.properties"));
	    String schema = prop.getProperty("schema");
	    String pass = prop.getProperty("password");
	    String tnsAdmin = prop.getProperty("tns");
	    String dbURL = prop.getProperty("dbURL");
	    
	    System.setProperty("oracle.net.tns_admin", tnsAdmin);
	    Class.forName ("oracle.jdbc.OracleDriver");

	    conn = DriverManager.getConnection(dbURL, schema, pass);
	    //	    conn = ods.getConnection();
	    System.out.println("Connection established");

	} catch (Exception e) {
	    e.printStackTrace();
	}
	finally {
	    if (conn != null) try { conn.close(); } catch (Exception e) {}
	}
    }}
