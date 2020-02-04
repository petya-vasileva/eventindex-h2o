/**                                                                                                                  
 * GetValidatedDatasets_OS program scans the HDFS ObjectStoreConsumerData                                                                                  * for new real-event datasets which are validated. If there are any, it writes                                                                            * them to a specified file in the atlasdba filespace on HDFS.                                                                               
 *                                                                                                                                                         * The program takes 3 arguments: 
 * start date String (format: yyyy-mm-dd hh-mi-ss)
 * end date (format: yyyy-mm-dd hh-mi-ss) 
 * file name where it will store the dataset paths
 *                                                                                                                                                         * @author  Petya Vasileva                                                                                                                                 * @version 2.4                                                                                                                                            * @since   2020-01-28                                                                                                                                     */

import java.awt.List;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;

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

public class GetValidatedDatasets {
    public static void main(String[] args) throws IOException, URISyntaxException {
	Configuration configuration = new Configuration();
	//FileSystem hdfs = FileSystem.get(new URI("/user/atlevind/ConsumerData/"), configuration);
	FileSystem hdfs = FileSystem.get(new URI("/user/atlevind/ObjectStoreConsumerData/"), configuration);

	FileStatus[] status = hdfs.listStatus(new Path("/user/atlevind/ObjectStoreConsumerData/datasets/"));
	SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	try {
	    //long d1 = df.parse("10-09-2017 01:00:01").getTime();
	    //long d2 = df.parse("17-09-2017 01:00:00").getTime();
	    long d1 = df.parse(args[0]).getTime();
	    long d2 = df.parse(args[1]).getTime();

	    Path todoFile = new Path(args[2]);
	    //Path todoFile = new Path("h2o_todos/initial_list_os_01-06-01-08.txt");
		//		File [] files;
  		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(hdfs.create(todoFile)));
		
		int cnt = 0;
		for(int i=0;i<status.length;i++){
		    String dataset = status[i].getPath().getName().toString();
		    Path valPath = new Path(status[i].getPath() + "/.VALIDATED");
		    

		    Date modTime = new Date(status[i].getModificationTime());
			
		    //System.out.println(">>>>>>>>>>>"+moddificationTime);
		    //files = file.listFiles();
		    if (dataset.startsWith("data")) {
			if (hdfs.exists(valPath)) {
			    FileStatus val_status = hdfs.getFileStatus(valPath);			    
			    //System.out.println(df.format(val_status.getModificationTime()));
			    if ((val_status.getModificationTime() > d1 && val_status.getModificationTime() < d2)) {
				//System.out.println(hdfs.getContentSummary(status[i].getPath()).getLength()/1024/1024/1024 + "GB " + dataset);
			        System.out.println(dataset);
				cnt++;
				bw.write(status[i].getPath().toString());
				bw.newLine();
			    }
			}
		    }
		}

		System.out.println("\n NUMBER OF DATASETS FOR THE PERIOD: >>>" + cnt);
		bw.flush();
		bw.close();
	
        } catch (Exception e) {
	    e.printStackTrace();
	}

    }
}
