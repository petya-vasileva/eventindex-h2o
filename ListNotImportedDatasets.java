/**                                                                                                                                                      
 * ListNotImportedDatasets goes through all validated datasets on HDFS 
 * and checks if they exist in the Oracle database, 
 * if not the paths to the datasets are added to the specified file.
 *                                                                                                                                                     
 * The program takes 3 arguments:                                                                                                                         
 * start date String (format: yyyy-mm-dd hh-mi-ss)                                                                                                        
 * end date (format: yyyy-mm-dd hh-mi-ss)                                                                                                                 
 * file name where it will store the dataset paths 
 *                                                                                                                                                   
 * @author  Petya Vasileva
 * @version 1.4
 * @since   2020-01-26                                                                                                                                 
 */

import java.awt.List;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.io.IOException;
import java.io.FileInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class ListNotImportedDatasets {

    private static String fromDate, toDate, fileName;

    private PreparedStatement stmt = null;
    private static ResultSet rs = null;
    private static Connection conn = null;
    
    public static void main(String[] args) {
	ListNotImportedDatasets lfd = new ListNotImportedDatasets();
	fromDate = args[0];
        toDate = args[1];	
	fileName = args[2];

      	try {
	    lfd.OpenConnection();
	    lfd.GetVaLidatedDatasets();	
        } catch (Exception e) {
	    e.printStackTrace();
        }  finally {
	    if (conn != null) 
		try { 
		    conn.close();
		    System.out.println("Connection closed");
		} catch (Exception e) {}
        }  
    }

    private void GetVaLidatedDatasets() throws IOException, URISyntaxException {
        Configuration configuration = new Configuration();
        FileSystem hdfs = FileSystem.get(new URI("/user/atlevind/ObjectStoreConsumerData/"), configuration);	
        FileStatus[] status = hdfs.listStatus(new Path("/user/atlevind/ObjectStoreConsumerData/datasets/"));
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	String dsNotInDB = null;

        try {
            long d1 = df.parse(fromDate).getTime();
            long d2 = df.parse(toDate).getTime();

            Path todoFile = new Path(fileName);
	    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(hdfs.create(todoFile)));

	    int cnt = 0;
	    for(int i=0;i<status.length;i++){
		String dataset = status[i].getPath().getName().toString();
		Path valPath = new Path(status[i].getPath() + "/.VALIDATED");
		Date modTime = new Date(status[i].getModificationTime());

		if (dataset.startsWith("data")) {
		    if (hdfs.exists(valPath)) {
			FileStatus val_status = hdfs.getFileStatus(valPath);
			if ((val_status.getModificationTime() > d1 && val_status.getModificationTime() < d2)) {
			    dsNotInDB = ExistsInDB(dataset);
			    
			    cnt++;
			    if (dsNotInDB != null) {
				System.out.println(status[i].getPath().toString()+"  "+modTime);
				bw.write(status[i].getPath().toString());
				bw.newLine();
			    }
			}
		    }
		}
	    }

	    bw.flush();
	    bw.close();

        } catch (Exception e) {
            e.printStackTrace();
        }  finally {
        }
    }

       
    private String ExistsInDB(String dataset) throws Exception {
        String notIn = null;
	
	String getFailed = "select case "+
                           "     when exists ("+
                           "       select *"+
                           "         from ("+
                           "            select PROJECT ||'.00'||"+
                           "                   RUNNUMBER ||'.'||"+
                           "                   STREAMNAME ||'.'||"+
                           "                   PRODSTEP ||'.'||"+
                           "                   DATATYPE ||'.'||"+
                           "                   AMITAG as dataset"+
                           "             from atlas_eventindex.ei_realevent_datasets"+
	    //             "            where insert_date > to_date(?, 'yyyy-mm-dd hh24:mi:ss')"+
	    //             "              and insert_date < to_date(?, 'yyyy-mm-dd hh24:mi:ss')"+
                           "              )"+
                           "        where dataset = ?"+
                           "    )"+
                           "    then 1"+
                           "    else 0"+
                           "   end as failed_dataset"+
	                   " from dual";        

	try {

	    stmt = conn.prepareStatement(getFailed);
	    stmt.setString(1, dataset);
	
	    rs = stmt.executeQuery();
	    while (rs.next()) {
		if (rs.getInt(1) == 0) {
		    notIn = dataset;
		}
	    }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
         }
	return notIn;
    }

    private void OpenConnection() throws Exception {
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
	    conn.setAutoCommit(false);
            System.out.println("Connection established");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void CloseConnection() throws Exception {
        if (stmt != null)
            stmt.close();
        rs.close();
        conn.commit();
        conn.close();
    }

}
