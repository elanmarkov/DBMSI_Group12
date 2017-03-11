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

class BatchEdgeInsertDriver extends TestDriver implements GlobalConst {

	private final static boolean OK = true;
	private final static boolean FAIL = false;

	private int choice;
	private final static int reclen = 32;

	public BatchEdgeInsertDriver () {
		super("BatchEdgeInsertDriver");
		choice = 530;  
	}


	public boolean runTests(String dbpath, String edgeFile) throws FileNotFoundException, IOException, SpaceNotAvailableException, HFBufMgrException, InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException {

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
		boolean _pass = test1(edgeFile);

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

	public boolean test1(String edgeFileName) throws FileNotFoundException, IOException, SpaceNotAvailableException, HFBufMgrException, InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException
	{
		boolean status = OK;
		String line;
		String nodelabel;
		EID eId = new EID();
		EID edgeId = new EID(); 
		byte[] nodeByteArray;
		NodeHeapFile nodeHeapFile;
		EdgeHeapFile edgeHeapFile;
		
		
		// Create the db
		//graphDB graphdb = new graphDB(/*number of ztree index*/);
		//graphdb.openDB("graphdbname");
		
		nodeHeapFile = new NodeHeapFile("file_1");
		
		edgeHeapFile = new EdgeHeapFile("file_2");	//graphdb.nodes;
		

		//Creating a page counter
		//PCounter pc = new PCounter();
		BufferedReader br = new BufferedReader(new FileReader(System.getProperty("user.dir")
				+ "/src/tests/" + edgeFileName + ".txt"));


		while ((line = br.readLine()) != null)
		{
			String[] splited = line.split("\\s+");
			String srcLabel = splited[0];
			String destLabel = splited[1];
			String edgeLabel = splited[2];
			int weight = Integer.parseInt(splited[3]);
			// Set the node fields
			NID sourceNid = new NID(new PageId(), 20);
			NID desNid = new NID(new PageId(), 40);
			Edge newEdge = new Edge();
			newEdge.setLabel(edgeLabel);
			newEdge.setSource(sourceNid);
			newEdge.setDestination(desNid);
			newEdge.setWeight(weight);
			nodeByteArray = newEdge.getEdgeByteArray();
			edgeId = edgeHeapFile.insertEdge(nodeByteArray);

		}
		

		System.out.println("Rec count " + edgeHeapFile.getRecCnt());
		Escan scan = null;

		if ( status == OK ) {	
			System.out.println ("  - Scan the records just inserted\n");

			try {
				scan = edgeHeapFile.openScan();
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
		Edge edge = new Edge();

		boolean done = false;
		while(!done){
		try {
			edge = scan.getNext(eId);
				if(edge != null){
					System.out.println("BatchNodeInsertDriver.test1() "+edge.getLength());
				}
			}
			catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}
			if(edge == null){
				done = true;
				break;
			}
			edge.print(null);
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

	public class batchEdgeinsert {

		public static void main (String argv[]) throws FileNotFoundException, IOException, SpaceNotAvailableException, HFBufMgrException, InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException {

			BatchEdgeInsertDriver hd = new BatchEdgeInsertDriver();
			boolean dbstatus;

			dbstatus = hd.runTests(argv[1], argv[0]);

			if (dbstatus != true) {
				System.err.println ("Error encountered during buffer manager tests:\n");
				Runtime.getRuntime().exit(1);
			}

			Runtime.getRuntime().exit(0);
		}
	}




