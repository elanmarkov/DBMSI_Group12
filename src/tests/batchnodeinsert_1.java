/* Batch Node Insertion By Shalmali Bhoir
 * 
 * Accepts data file name and graph database name from command line
 * Creates database with given name
 * Reads data file given and inserts the nodes/edges from the file in the database
 */

package tests;
import java.io.*;
import java.util.*;

import global.*;
import diskmgr.*;
import heap.*;

class BatchNodeInsertDriver extends TestDriver implements GlobalConst {

	private final static boolean OK = true;
	private final static boolean FAIL = false;

	private int choice;
	private final static int reclen = 32;

	public BatchNodeInsertDriver () {
		super("batchnodeinserttest");
		choice = 530;  
	}


	public boolean runTests(String dbpath, String NodeFile) throws FileNotFoundException, IOException, SpaceNotAvailableException, HFBufMgrException, InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException {

		System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");

		SystemDefs sysdef = new SystemDefs(dbpath,1000,100,"Clock");

		// Kill anything that might be hanging around
		String newdbpath;
		String newlogpath;
		String remove_logcmd;
		String remove_dbcmd;
		String remove_cmd = "/bin/rm -rf ";

		newdbpath = dbpath;
		newlogpath = logpath;

		remove_logcmd = remove_cmd + logpath;
		remove_dbcmd = remove_cmd + dbpath;

		// Commands here is very machine dependent.  We assume
		// user are on UNIX system here
		try {
	      Runtime.getRuntime().exec(remove_logcmd);
	      Runtime.getRuntime().exec(remove_dbcmd);
	    }
	    catch (IOException e) {
	      System.err.println ("IO error: "+e);
	    }

	    remove_logcmd = remove_cmd + newlogpath;
	    remove_dbcmd = remove_cmd + newdbpath;

	    try {
	      Runtime.getRuntime().exec(remove_logcmd);
	      Runtime.getRuntime().exec(remove_dbcmd);
	    }
	    catch (IOException e) {
	      System.err.println ("IO error: "+e);
	    }

		//Run the tests. Return type different from C++
		boolean _pass = test1(NodeFile);

		//Clean up again
		/*try {
	      Runtime.getRuntime().exec(remove_logcmd);
	      Runtime.getRuntime().exec(remove_dbcmd);
	    }
	    catch (IOException e) {
	      System.err.println ("IO error: "+e);
	    }*/

		System.out.print ("\n" + "..." + testName() + " tests ");
		System.out.print (_pass==OK ? "completely successfully" : "failed");
		System.out.print (".\n\n");

		return _pass;
	}

	public boolean test1(String nodefilename) throws FileNotFoundException, IOException, SpaceNotAvailableException, HFBufMgrException, InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException
	{
		boolean status = OK;
		String line;
		String nodelabel;
		NID n = new NID();
		NID nodeid = new NID(); 
		int[] descVal = new int[5];
		byte[] nodeByteArray;
		NodeHeapFile nodeHeapFile;
		AttrType[] ntype = new AttrType[2];
		ntype[1] = new AttrType(AttrType.attrString);
		ntype[0] = new AttrType(AttrType.attrDesc);

		// Create the db
		//graphDB graphdb = new graphDB(/*number of ztree index*/);
		//graphdb.openDB("graphdbname");
		try
		{
			nodeHeapFile = new NodeHeapFile("file_1");	//graphdb.nodes;
		}
		catch(Exception e)
		{
			status = FAIL;
			System.err.println ("*** Error in creating node heap file\n");
			e.printStackTrace();
			return status;
		}

		//Creating a page counter
		//PCounter pc = new PCounter();
		BufferedReader br = new BufferedReader(new FileReader(System.getProperty("user.dir")
				+ "/src/tests/" + nodefilename + ".txt"));


		while ((line = br.readLine()) != null)
		{
			String[] splited = line.split("\\s+");
			nodelabel = splited[0];

			// Set the node fields
			Node newnode = new Node();
			newnode.setLabel(nodelabel);

			Descriptor nodedesc = new Descriptor();
			nodedesc.set(Integer.parseInt(splited[1]), Integer.parseInt(splited[2]), 
					Integer.parseInt(splited[3]), Integer.parseInt(splited[4]), 
					Integer.parseInt(splited[5]));
			newnode.setDesc(nodedesc);
			System.out.println("BatchNodeInsertDriver.test1() Nodelbl"+newnode.getLabel() +"Desc"+nodedesc.toString());
			nodeByteArray = newnode.getNodeByteArray();
			Node nd= new Node(nodeByteArray, 0);
			System.out.println("BatchNodeInsertDriver.test1() "+nodeByteArray.length);
			System.out.println("BatchNodeInsertDriver.test1() "+nd.getNodeByteArray().length);
			nodeid = nodeHeapFile.insertNode(nd.getNodeByteArray());

		}
		

		System.out.println("Rec count " + nodeHeapFile.getRecCnt());
		Nscan scan = null;

		if ( status == OK ) {	
			System.out.println ("  - Scan the records just inserted\n");

			try {
				scan = nodeHeapFile.openScan();
			}
			catch (Exception e) {
				status = FAIL;
				System.err.println ("*** Error opening scan\n");
				e.printStackTrace();
			}

			if ( status == OK &&  SystemDefs.JavabaseBM.getNumUnpinnedBuffers() 
					== SystemDefs.JavabaseBM.getNumBuffers() ) {
				System.err.println ("*** The heap-file scan has not pinned the first page\n");
				status = FAIL;
			}
		}	
		

		int len, i = 0;
		DummyRecord rec = null;
		Node node = new Node();

		boolean done = false;
		while(!done){
		try {
				node = scan.getNext(n);
				if(node != null){
					System.out.println("BatchNodeInsertDriver.test1() "+node.getLength());
				}
			}
			catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}
			if(node == null){
				done = true;
				break;
			}
			node.print(ntype);
		}
		scan.closescan();

			// Output node count
			System.out.println("Node Count after batch insertion on graph database: " /*+ graphDB.getNodeCnt()*/);

			// Output edge count
			System.out.println("Edge Count after batch insertion on graph database: " /*+ graphDB.getEdgeCnt()*/);

			// Output no of disk pages read
			System.out.println("No. of disk pages read during batch insertion on graph database: " /*+ pc.rcounter*/);

			// Output no of disk pages written
			System.out.println("No. of disk pages written during batch insertion on graph database: " /*+ pc.wcounter*/);

			if ( status == OK )
				System.out.println ("  Test completed successfully.\n");
			return status; 
		}

	}

	public class batchnodeinsert_1 {

		public static void main (String argv[]) throws FileNotFoundException, IOException, SpaceNotAvailableException, HFBufMgrException, InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException {

			BatchNodeInsertDriver hd = new BatchNodeInsertDriver();
			boolean dbstatus;

			dbstatus = hd.runTests(argv[1], argv[0]);

			if (dbstatus != true) {
				System.err.println ("Error encountered during buffer manager tests:\n");
				Runtime.getRuntime().exit(1);
			}

			Runtime.getRuntime().exit(0);
		}
	}




