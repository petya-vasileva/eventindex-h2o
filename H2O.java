/**
 * H2O program takes a single dataset and uploads all events from Hadoop to Oracle
 *
 * It takes 2 argument:
 * 1. dataset path
 * 2. time of execution
 *                                                                                                                                                      
 * @author  Petya Vasileva                                                                                                                              
 * @version 2.3                                                                                                                                         
 * @since   2020-01-26                                                                                                                                  
 */


import java.io.*;
import java.math.BigDecimal;
import java.math.MathContext;
import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.net.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;

public class H2O {
    private BufferedReader br = null;
    private String curLine = null;
    private final ArrayList<String> fileURIs = new ArrayList<String>();
    private final List<Integer> index = new ArrayList<Integer>();
    private long datasetId;
    private PreparedStatement stmt = null;
    private Connection conn = null;
    private long gRowCnt = 0;
    private Boolean retried = false;
    
    private Timestamp startTime;
    private Timestamp endTime;
    private String schemaInfo;
    private Timestamp modTime;
    private Timestamp hadoopImpTime;
    private String fileName;
    private String datasetPath;
    private Timestamp importProcessStartTime;
    
    /**
     * The following fields are parsed from the path of the dataset (also exist at the
     * 1st line of the .VALIDATED file) They should be the same for the whole
     * dataset.
     */
    private String projName = null; // data15_13Tev
    private long runNum = 0; // 00284484
    private String stream = null; // physics_ZeroBias
    private String prodStep = null; // merge
    private String dataType = null; // AOD
    private String amiTag = null; // r7446_p2492
    
    // Parsed from the data file
    // Event number is taken from the key in the data file
    private long evntNum = 0;
    private Integer LUMIBLOCKN = null;
    private Integer BUNCHID = null;
    private String GUID0 = null;
    private String GUID_TYPE0 = null;
    private String GUID1 = null;
    private String GUID_TYPE1 = null;
    private String GUID2 = null;
    private String GUID_TYPE2 = null;
    
    public static void main(String[] args) throws Exception {
        H2O h2o = new H2O();
        h2o.datasetPath = args[0];
	h2o.importProcessStartTime = Timestamp.valueOf(args[1]);

        //System.out.println(h2o.datasetPath);

        try {
            h2o.OpenConnection();
            h2o.startTime = new Timestamp(System.currentTimeMillis());
            
            Path pt = new Path(h2o.datasetPath);
            FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] status = fs.listStatus(new Path(h2o.datasetPath));
            // get the time of the last modification on the whole dataset
            h2o.modTime = new Timestamp(status[0].getModificationTime());
            
            // in case it is neccessary to run the import of a single file from the
            // dataset
            Path singleFile = null;
            if (args.length > 1 && args[1].startsWith("-file=")) {
                singleFile = new Path(args[1].substring("-file=".length()));
            }
            
            // create path to the .VALIDATED file
            pt = new Path(h2o.datasetPath + "/.VALIDATED");
            h2o.br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            
            // Initialize
            h2o.curLine = h2o.br.readLine();
            h2o.ParseVALIDATEDFile(singleFile);
            h2o.InsertDataset();
            
            // Parse each file and import the data
            for (int i = 0; i < h2o.fileURIs.size(); i++) {
		h2o.fileName = h2o.fileURIs.get(i);
                h2o.ParseDataFiles(h2o.fileURIs.get(i));
            }
            h2o.endTime = new Timestamp(System.currentTimeMillis());

            h2o.InsertDatasetInfo();
        } catch (H2OException e) {
            h2o.SendErrorInfo(e);
        } catch (Exception e) {
            h2o.SendErrorInfo(e);
        } finally {
            h2o.br.close();
            h2o.CloseConnection();
        }
    }
    
    private void ParseVALIDATEDFile(Path singleFile) throws H2OException, Exception {
        /*		 Parse the file in format:
         import.srcdir=/user/atlevind/ConsumerData/datasets/data15_13TeV.00284484.physics_ZeroBias.merge.AOD.r7446_p2492
         import.schema=LumiBlockN=int BunchId=int EventTime=int
         EventTimeNanoSec=int EventWeight=float McChannelNumber=int,
         Lvl1ID=String IsSimulation=int IsCalibration=int IsTestBeam=int
         L1trigMask=String L2trigMask=String EFtrigMask=String SMK=int
         HLTPSK=int L1PSK=int nam0=String db0=String cnt0=String clid0=String
         tech0=String oid0=String nam1=String db1=String cnt1=String
         clid1=String tech1=String oid1=String nam2=String db2=String
         cnt2=String clid2=String tech2=String oid2=String nam3=String
         db3=String cnt3=String clid3=String tech3=String oid3=String
         import.key=RunNumber_EventNumber=String
         import.keyformat=%08d-%011d
         import.status=VALIDATED
         import.timestamp=145251271
         #file: hdfs://p01001533040197.cern.ch:9000/user/atlevind/ConsumerData/datasets/data15_13TeV.00284484.physics_ZeroBias.merge.AOD.r7446_p2492/8840A3A5-8BEF-4342-9D4F-56E649D2FE90_6b845e4383e04e3899c40b8159dea86a_e479a104-a5d1-4e9b-bc3a-5f18515bda3b_7402556.G_2727224071.1.csv_mapfile
         #file: hdfs://p01001533040197.cern.ch:9000/user/atlevind/ConsumerData/datasets/data15_13TeV.00284484.physics_ZeroBias.merge.AOD.r7446_p2492/5154E650-8647-7B48-917D-D8DE01B5D4D1_b00492c325634ce790e0ed30cd1f2ba1_d8e180aa-1306-4e21-be9e-1129a8701e4c_7402556.G_2727224357.1.csv_mapfile
         */
        ParseFileHeader();
        if (singleFile == null) {
            ParseFilePayload();
        } else {
            // When run with -file=...
            fileURIs.add(singleFile.toString());
        }
    }
    
    private void ParseFileHeader() throws Exception, H2OException {
        if (!curLine.startsWith("import.srcdir")) {
            throw new H2OException("\n Expected row starting with *** import.schema *** \n Found: " + curLine);
        }
        
        // import.srcdir=/user/atlevind/ConsumerData/datasets/data15_13TeV.00267167.physics_ZeroBias.merge.AOD.r7455_p2492
        String src = curLine.substring("import.srcdir=/user/atlevind/ObjectStoreConsumerData/datasets/".length());
        String[] srcData = src.split("\\.");
        
        projName = srcData[0]; // data15_13Tev
        runNum = Long.parseLong(srcData[1]); // 00284484
        stream = srcData[2]; // physics_ZeroBias
        prodStep = srcData[3]; // merge
        dataType = srcData[4]; // AOD
	//System.out.println("length" + srcData.length);
	if (srcData.length > 5)
           amiTag = srcData[5];
	else amiTag = "-"; // r7455_p2492
        MoveToNextLine();
        
        // Parse the import.schema
        if (!curLine.startsWith("import.schema")) {
            throw new H2OException("Expected row starting with *** import.schema *** \n Found: " + curLine);
        }
        schemaInfo = curLine;
        ParseSchema(curLine);
        MoveToNextLine();
        
        // Parse the import.key
        if (!curLine.startsWith("import.key")) {
            throw new H2OException("Expected row starting with *** import.key *** \n Found:" + curLine);
        }
        
        String key = curLine.substring("import.key=".length());
        String[] seqFileKey = key.split("=");
        if (!seqFileKey[0].equals("RunNumber_EventNumber")) {
            throw new H2OException("Expected key: RunNumber_EventNumber. \n Found: " + seqFileKey[0]);
        }
        
        MoveToNextLine();
        
        if (curLine.startsWith("import.keyformat")) {
            MoveToNextLine(); // skip import.keyformat
        }
        
        if (curLine.startsWith("import.status")) {
            MoveToNextLine(); // skip import.status
        }
        
        if (curLine.startsWith("import.timestamp")) {
            String[] arr = curLine.split("=");
	    hadoopImpTime = new Timestamp(Long.parseLong(arr[1])*1000);
	    MoveToNextLine(); // skip import.timestamp
        }
    }
    
    private void ParseFilePayload() throws Exception, H2OException {
        // #file:
        // hdfs://p01001533040197.cern.ch:9000/user/atlevind/ConsumerData/datasets/data15_13TeV.00284484.physics_ZeroBias.merge.AOD.r7446_p2492/8840A3A5-8BEF-4342-9D4F-56E649D2FE90_6b845e4383e04e3899c40b8159dea86a_e479a104-a5d1-4e9b-bc3a-5f18515bda3b_7402556G_2727224071.1.csv_mapfile
        // #file:
        // hdfs://p01001533040197.cern.ch:9000/user/atlevind/ConsumerData/datasets/data15_13TeV.00284484.physics_ZeroBias.merge.AOD.r7446_p2492/5154E650-8647-7B48-917D-D8DE01B5D4D1_b00492c325634ce790e0ed30cd1f2ba1_d8e180aa-1306-4e21-be9e-1129a8701e4c_7402556.G_2727224357.1.csv_mapfile
        do {
            if (!curLine.startsWith("#file")) {
                throw new H2OException(" Expected row starting with *** #file *** \n Found: " + curLine);
            }
            
            fileURIs.add(curLine.substring("#file  ".length()));
        } while (MoveToNextLine());
    }
    
    private boolean MoveToNextLine() throws Exception {
        curLine = br.readLine();
        return curLine != null;
    }
    
    private void ParseSchema(String line) throws H2OException {
        String schema = curLine.substring("import.schema=".length());
        String[] schArr = schema.split(" ");
        if (schArr.length != 40)
            throw new H2OException("The number of fields in the schema is different! " + schArr.toString());
        String schFlds[];
        
        for (int i = 0; i < schArr.length; i++) {
            schFlds = schArr[i].split("=");
            String fld_name = schFlds[0];
            String fld_type = schFlds[1];
            switch (fld_name) {
                case "LumiBlockN":
                case "BunchId":
                case "nam0":
                case "nam1":
                case "nam2":
                case "db0":
                case "db1":
                case "db2":
                    index.add(i);
                    break;
            }
        }
    }
    
    /** Step 2: Insert the dataset details
     * @throws Exception
     */
    private void InsertDataset() throws Exception {
        // System.out.println(">>>>>>>>>>>>>>>>" + projName + "  " + runNum + "  " + stream + "  " + prodStep + "  " + dataType + "   " + amiTag);
        try { 
	    GetDatasetId();
	    stmt = conn.prepareStatement("INSERT INTO ATLAS_EVENTINDEX.EI_REALEVENT_DATASETS "
					 + "(DATASET_ID, PROJECT, RUNNUMBER, STREAMNAME, PRODSTEP, DATATYPE, AMITAG ) " + "VALUES (?,?,?,?,?,?,?)");
        
	    stmt.setLong(1, datasetId);
	    stmt.setString(2, projName);
	    stmt.setLong(3, runNum);
	    stmt.setString(4, stream);
	    stmt.setString(5, prodStep);
	    stmt.setString(6, dataType);
	    stmt.setString(7, amiTag);
        
	    stmt.addBatch();
	    stmt.executeBatch();
	    stmt.close();
	} catch (Exception e) {                                             
	    if ( ((SQLException)e).getErrorCode() == 1) {
                /*The next few lines can be uncommented when we don't want to overwrite datasets.*/
		//System.err.println("Skip dataset " + datasetPath);
		//conn.close();
		//System.exit(1);
		RetryDataset();
            } else if (((SQLException)e).getErrorCode() == 20101) {
		System.err.println("Run number "+ runNum +" does not exist into TLAS_TAGS_METADATA.COMA_RUNS table."); 
		conn.close();                                                                                                                                                           
		System.exit(201);
	    }
	    else SendErrorInfo(e);
	} 
    }

    /** Step 2.1: Insert the retried datasets that already exist in the system 
     *  and need to be deleted and imported again.	
     * @throws Exception                                                                                                                                                                                                                                                    
     */
    private void RetryDataset() throws Exception {
        // System.out.println(">>>>>>>>>>>>>>>>" + projName + "  " + runNum + "  " + stream + "  " + prodStep + "  " + dataType + "   " + amiTag);                                                                                                                         
	System.out.println("<<<<<< Dataset with id " + datasetId + " overwrites another one! >>>>>>" );                                                                                                                                                                                                                                                                           
        retried = true;
        stmt = conn.prepareStatement("INSERT INTO ATLAS_EVENTINDEX.HIST_EI_REALEVENT_DATASETS "
                                     + "(DATASET_ID, PROJECT, RUNNUMBER, STREAMNAME, PRODSTEP, DATATYPE, AMITAG ) " + "VALUES (?,?,?,?,?,?,?)");

        stmt.setLong(1, datasetId);
        stmt.setString(2, projName);
        stmt.setLong(3, runNum);
        stmt.setString(4, stream);
        stmt.setString(5, prodStep);
        stmt.setString(6, dataType);
        stmt.setString(7, amiTag);

        stmt.addBatch();
        stmt.executeBatch();
        stmt.close();
    }
    
    /** Step 1: Get dataset ID
     * @throws Exception
     */
    private void GetDatasetId() throws Exception {
        String getSequence = "select ATLAS_EVENTINDEX.REALEVENTS_DS_ID_SEQ.nextval from dual";
        ResultSet rs = null;
        
        stmt = conn.prepareStatement(getSequence);
        rs = stmt.executeQuery();
        if (rs.next()) {
            datasetId = rs.getLong(1);
        }
        rs.close();
        stmt.close();
    }
    
    /** Step 3: Insert the data taken from the files found in the .VALIDATED file
     * @param file
     * @throws Exception
     */
    private void ParseDataFiles(String file) throws Exception, H2OException {
        // String file = fileURIs.get(0);
        // 00284484-00795818900
        // 250,3020,1446517518,267401320,1.0,0,486739283,0,0,0,!Cs!B!B!B!O!F!C!B!B!E!J!D!!M!!!P!B!E!P!Bs!P!!/!MY!EP,dX!HG;ke;ke,dX!HG;ke;e!4!!BM!M!L!W!C!L!C!L!C!J!NH!!!!!!I/!KQ,2231,3417,4834,StreamAOD,8840A3A5-8BEF-4342-9D4F-56E649D2FE90,POOLContainer(DataHeader),D82968A1-CF91-4320-B2DD-E0F739CBC7E6,00000202,000001FE-000001BA,StreamESD,B2F30AE8-9BBD-3D45-A09A-AD2CBB5583C5,POOLContainer(DataHeader),D82968A1-CF91-4320-B2DD-E0F739CBC7E6,00000202,00000279-0000003E,StreamRAW,FAF87BB2-D981-E511-9188-02163E00E205,00000000,00000000-0000-0000-0000-000000000000,00001000,00000000-3842DCA8,,,,,,
        // 00284484-00795821424
        // 250,1224,1446517518,307105730,1.0,0,486741807,0,0,0,DI!C!B!B!E!J!R!!!CT!P!!/!MY!EP,dW!HH;ke;ke,dW!HH;ke;RI!!!!!!I/!KQ,2231,3417,4834,StreamAOD,8840A3A5-8BEF-4342-9D4F-56E649D2FE90,POOLContainer(DataHeader),D82968A1-CF91-4320-B2DD-E0F739CBC7E6,00000202,000001FE-000002B4,StreamESD,1CCE58BF-134A-6445-9E37-850CBAEA4663,POOLContainer(DataHeader),D82968A1-CF91-4320-B2DD-E0F739CBC7E6,00000202,0000027A-0000001F,StreamRAW,FAF87BB2-D981-E511-9188-02163E00E205,00000000,00000000-0000-0000-0000-000000000000,00001000,00000000-1B14FDF0,,,,,,
        SequenceFile.Reader reader = null;
        Configuration conf = new Configuration();
        int rowCnt = 0;
        
        stmt = conn.prepareStatement(" INSERT INTO ATLAS_EVENTINDEX.H2O_STAGING_TABLE ( LUMIBLOCKN, BUNCHID, GUID_TYPE0, GUID0, "
                                     + "GUID_TYPE1, GUID1, GUID_TYPE2, GUID2, EVENTNUMBER,  DATASET_ID) VALUES (?,?,?,?,?,?,?,?,?,?) ");
        // System.out.println(proj_name + "  " + run_num + "  " + stream +
        // "  " + prod_step + "  " + data_type + "   " + ami_tag);
        URI fileURI = URI.create(file);
        
        Path path = new Path(fileURI.getPath());
        FileSystem fs = FileSystem.get(fileURI, new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        
        reader = new SequenceFile.Reader(conf, Reader.file(path), Reader.bufferSize(4096), Reader.start(0));
        Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
        
        while (reader.next(key, value)) {
            String k = key.toString();
            String[] arr = k.split("-");
            
            if (runNum != 0 && Long.parseLong(arr[0]) != runNum) {
                throw new H2OException("RUN NUMBER IS DIFFERENT! \n Expected >>>> " + runNum + " Found >>>> " +
                                       Long.parseLong(arr[0]) + "\n Fisrt apperance is on row #" + (rowCnt+1) + ". May have more for this dataset!");
            }
            evntNum = Long.parseLong(arr[1]);
            
            String cLine = value.toString();
            String[] vals = cLine.split(",", -1);
            // System.out.println(" \n ROW: " + index);
            
            for (int i = 0; i < index.size(); i++) {
                stmt.setString(i + 1, vals[index.get(i)]);
                // System.out.println(vals[index.get(i)]);
            }
            
            stmt.setLong(9, evntNum);
            stmt.setLong(10, datasetId);
            stmt.addBatch();
            
            rowCnt++;
            if (rowCnt % 1000 == 0) // devisible on 1000
                stmt.executeBatch();
        }
        
        stmt.executeBatch();
        reader.close();
        stmt.close();
        br.close();
        // System.out.println("COUNT ROWS: >>>>>>> " + rowCnt);
        InsertFileInfo(file, rowCnt, fs.getFileStatus(path).getLen());
        gRowCnt +=  Long.valueOf(rowCnt);
    }
    
    /** Step 4: Insert details about the datafiles
     * @param file - file name
     * @param row_num - number of rows in the file
     * @param file_size - size of the file in bytes
     * @throws Exception
     */
    private void InsertFileInfo(String file, long row_num, long file_size) throws Exception {
        stmt = conn.prepareStatement("INSERT INTO ATLAS_EVENTINDEX.H2O_DATAFILE_INFO "
                                     + "(DATASET_ID, FILE_NAME, ROWS_COUNT, FILESIZE ) " + "VALUES (?,?,?,?)");
        
        stmt.setLong(1, datasetId);
        stmt.setString(2, fileName);
        stmt.setLong(3, row_num);
        stmt.setLong(4, file_size);
        
        stmt.addBatch();
        stmt.executeBatch();
        stmt.close();
    }
    
    /** Step 5: Insert meta information about the dataset being imported
     * @throws Exception
     */
    private void InsertDatasetInfo() throws Exception {
	double ins_rate = gRowCnt/((endTime.getTime() - startTime.getTime())/1000)/1000.000;
        BigDecimal bd = new BigDecimal(ins_rate);
        bd = bd.round(new MathContext(4));
        ins_rate = bd.doubleValue();
	
	String status = "DONE";
        if (retried)
            status = "OVERWRITTEN";

        System.out.println("insertion rate >>>>> " + ins_rate);
        stmt = conn
	    .prepareStatement("INSERT INTO ATLAS_EVENTINDEX.H2O_DATASET_INFO (DATASET_ID, H2O_STATUS, FILES_COUNT, "
                          + "H2O_START_TIME, H2O_END_TIME, HADOOP_MODIF_TIME, SCHEMA_INFO, HADOOP_IMP_TIME, H2O_ROWS_COUNT, H2O_LOADRATE_KHZ, IMPORT_PROCESS_STARTED_AT, H2O_INFO) "
			      + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)");

        stmt.setLong(1, datasetId);
        stmt.setString(2, status);
        stmt.setLong(3, fileURIs.size());
        stmt.setTimestamp(4, startTime);
        stmt.setTimestamp(5, endTime);
        stmt.setTimestamp(6, modTime);
        stmt.setString(7, schemaInfo);
        stmt.setTimestamp(8, hadoopImpTime);
        stmt.setLong(9, gRowCnt);
        stmt.setDouble(10, ins_rate);
	stmt.setTimestamp(11, importProcessStartTime);
	stmt.setString(12, datasetPath);

        stmt.addBatch();
        stmt.executeBatch();
        stmt.close();
    }    

    private void OpenConnection() throws Exception {
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
    }
    
    private void CloseConnection() throws Exception {
        System.out.println(" START TIME: >>> " + startTime + " END TIME >>>> " + endTime);
	System.out.println("Number of files in the dataset: " + fileURIs.size());
        System.out.println("Total number of imported rows: " + gRowCnt);
	if (stmt != null)
            stmt.close();
        conn.commit();
        conn.close();
    }
    
    /**
     * This method takes the thrown exception as an argument and sends the error
     * messages to Oracle
     * */
    private void SendErrorInfo(Exception ex) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        ex.printStackTrace(pw);
        String errMsg = sw.toString();
        if (errMsg.length() > 4000)
            errMsg = errMsg.substring(0, 4000);
	System.out.println(errMsg);
        
        try {
            conn.rollback();
            stmt = conn
            .prepareStatement("INSERT /* addBatch insert */ INTO ATLAS_EVENTINDEX.H2O_DATASET_INFO "
                              + "(DATASET_ID, HADOOP_MODIF_TIME, HADOOP_IMP_TIME, H2O_START_TIME, H2O_END_TIME, FILES_COUNT, "
                              + "H2O_STATUS, SCHEMA_INFO, H2O_MESSAGE, H2O_INFO, IMPORT_PROCESS_STARTED_AT) "
                              + "VALUES (?,?,?,?,?,?,?,?,?,?,?)");
            
            stmt.setLong(1, datasetId);
            stmt.setTimestamp(2, modTime);
            stmt.setTimestamp(3, hadoopImpTime);
            stmt.setTimestamp(4, startTime);
            stmt.setTimestamp(5, endTime);
            stmt.setInt(6, fileURIs.size());
            stmt.setString(7, "FAILED");
            stmt.setString(8, schemaInfo);
            stmt.setString(9, errMsg);
            stmt.setString(10, datasetPath);
	    stmt.setTimestamp(11, importProcessStartTime);
            
            stmt.addBatch();
            stmt.executeBatch();
            conn.commit();
            
        } catch (Exception e) {
            System.out.println("Exception: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                stmt.close();
            } catch (Exception e) { /* ignored */
            }
            try {
                conn.close();
            } catch (Exception e) { /* ignored */
            }
            System.exit(1);
        }
    }
}

class H2OException extends Exception {
    private static final long serialVersionUID = 1L;
    
    public H2OException(String message) {
        super(message);
    }
    
}
