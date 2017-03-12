/* Batch Node Insertion By Shalmali Bhoir
 * 
 * Accepts node data file name and graph database name from command line
 * Creates database with given name
 * Reads data file given and inserts the nodes/edges from the file in the database
 */
package tests;

import java.io.*;
import java.util.Objects;

import global.*;
import heap.*;
import diskmgr.*;

class BatchNodeInsertDriver extends TestDriver implements GlobalConst {

	private final static boolean OK = true;
	private final static boolean FAIL = false;

	public BatchNodeInsertDriver () {
		super("batchnodeinserttest");
	}


	public boolean runTests(String dbpath, String NodeFile, PCounter pc) throws 
	FileNotFoundException, IOException, SpaceNotAvailableException, HFBufMgrException, Exception,
	InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException {
		
		System.out.println ("\nRunning Batch Node Insert tests....\n");
		
		SystemDefs sysdef = new SystemDefs(dbpath,0,100,"Clock");
		SystemDefs.JavabaseDB.init();
		
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
		/*try {
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
	    }*/

		//Run the tests. Return type different from C++
		boolean _pass = test1(NodeFile, sysdef, pc);

		//Clean up again
		/*try {
	      Runtime.getRuntime().exec(remove_logcmd);
	      Runtime.getRuntime().exec(remove_dbcmd);
	    }
	    catch (IOException e) {
	      System.err.println ("IO error: "+e);
	    }*/

		System.out.print ("\n" + "... Batch Node Insert tests ");
		System.out.print (_pass==OK ? "completely successfully" : "failed");
		System.out.print (".\n\n");

		return _pass;
	}

	public boolean test1(String nodefilename, SystemDefs sysdef, PCounter pc)
			throws FileNotFoundException, IOException, SpaceNotAvailableException, 
			HFBufMgrException, InvalidSlotNumberException, InvalidTupleSizeException, 
			HFException, HFDiskMgrException, Exception
	{
		boolean status = OK;
		String line, nodelabel;
		NID n = new NID();
		NID nodeid = new NID(); 
		NodeHeapFile nodeHeapFile;
		int[] descVal = new int[5];
		byte[] nodeByteArray;
		AttrType[] ntype = new AttrType[2];
		ntype[1] = new AttrType(AttrType.attrString);
		ntype[0] = new AttrType(AttrType.attrDesc);
		
		try
		{
			nodeHeapFile = sysdef.JavabaseDB.getNodes();//new NodeHeapFile("file_1");
		}
		catch(Exception e)
		{
			System.err.println ("*** Error in creating node heap file\n");
			e.printStackTrace();
			return FAIL;
		}
		

		BufferedReader br = new BufferedReader(new FileReader(System.getProperty("user.dir")
				+ "/tests/" + nodefilename + ".txt"));

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
			nodeByteArray = newnode.getNodeByteArray();
			nodeid = nodeHeapFile.insertNode(nodeByteArray);

		}
		br.close();

		System.out.println("Rec count " + nodeHeapFile.getRecCnt());

		// Output relevant statistics
		int rcount = pc.rcounter;
		int wcount = pc.wcounter;
		System.out.println("Node Count after batch insertion on graph database: " + sysdef.JavabaseDB.getNodeCnt());
		System.out.println("Edge Count after batch insertion on graph database: " + sysdef.JavabaseDB.getEdgeCnt());
		System.out.println("No. of disk pages read during batch insertion on graph database: " + rcount);
		System.out.println("No. of disk pages written during batch insertion on graph database: " + rcount);
		
			if ( status == OK )
				System.out.println ("  Test completed successfully.\n");
			return status; 
		
		/*NodeHeapFile nodeHeapFile = sysdef.JavabaseDB.getNodes();
		System.out.println("rec count" + nodeHeapFile.getNodeCnt());
		Nscan scan = null;

		AttrType[] ntype = new AttrType[2];
		ntype[1] = new AttrType(AttrType.attrString);
		ntype[0] = new AttrType(AttrType.attrDesc);
		NID n = new NID();
		boolean status = OK;
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
        node = scan.getNext(n);
		boolean done = false;
		while(!done){
		try {
			System.out.println("in scan");
				node = scan.getNext(n);
				if(node != null){
					System.out.println("BatchNodeInsertDriver.test1() "+node.getLength());
				}
			}
			catch (Exception e) {
				//status = FAIL;
				e.printStackTrace();
			}
			if(node == null){
				done = true;
				break;
			}
			node.print(ntype);
		}
		scan.closescan();

		return true;*/
		}
}
	
	public class batchnodeinsert {
		
		public static void main (String argv[]) 
				throws FileNotFoundException, IOException, SpaceNotAvailableException,  Exception,
				HFBufMgrException, InvalidSlotNumberException, InvalidTupleSizeException, 
				HFException, HFDiskMgrException {
			PCounter pc = new PCounter();
			pc.initialize();
			BatchNodeInsertDriver bn = new BatchNodeInsertDriver();
			boolean dbstatus;

			dbstatus = bn.runTests(argv[1], argv[0], pc);

			if (dbstatus != true) {
				System.err.println ("Error encountered during batch node insert tests:\n");
				Runtime.getRuntime().exit(1);
			}
			Runtime.getRuntime().exit(0);
		}
	}




