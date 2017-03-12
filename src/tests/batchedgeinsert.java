/* Batch Edge Insertion By Shalmali Bhoir
 * 
 * Accepts edge data file name and graph database name from command line
 * Creates database with given name
 * Reads data file given and inserts the edges from the file in the database
 */
package tests;

import java.io.*;
import java.util.*;

import btree.AddFileEntryException;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import global.*;
import diskmgr.*;
import heap.*;

class BatchEdgeInsertDriver extends TestDriver implements GlobalConst {

	private final static boolean OK = true;
	private final static boolean FAIL = false;
	public static final String LABEL_CONSTANT = "A";
	public static final int LABEL_MAX_LENGTH =6; 

	public BatchEdgeInsertDriver () {
		super("BatchEdgeInsertDriver");
	}


	public boolean runTests(String dbpath, String edgeFile) throws FileNotFoundException, IOException, SpaceNotAvailableException, HFBufMgrException, InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException, GetFileEntryException, ConstructPageException, AddFileEntryException {

		System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");

		SystemDefs sysdef = new SystemDefs(dbpath,1000,100,"Clock");
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
		boolean _pass = test1(edgeFile, sysdef);

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

	public boolean test1(String edgeFileName, SystemDefs sysdef) 
			throws FileNotFoundException, IOException, SpaceNotAvailableException, 
			HFBufMgrException, InvalidSlotNumberException, InvalidTupleSizeException, 
			HFException, HFDiskMgrException
	{
		boolean status = OK;
		String line;
		String nodelabel;
		EID eId = new EID();
		EID edgeId = new EID();
		Node scanned_node;
		byte[] edgeByteArray;
		NodeHeapFile nodeHeapFile;
		EdgeHeapFile edgeHeapFile;
		
		nodeHeapFile = sysdef.JavabaseDB.getNodes();
		edgeHeapFile = sysdef.JavabaseDB.getEdges();
		
		BufferedReader br = new BufferedReader(new FileReader(System.getProperty("user.dir")
				+ "/tests/" + edgeFileName + ".txt"));


		while ((line = br.readLine()) != null)
		{
			String[] splited = line.split("\\s+");
			String srcLabel = splited[0];
			String destLabel = splited[1];
			String edgeLabel = splited[2];
			int weight = Integer.parseInt(splited[3]);
			NID sourceNid = new NID();
			NID desNid = new NID();
			
			Nscan scan = nodeHeapFile.openScan();
			NID nid = new NID();
			scanned_node = scan.getNext(nid); 
			boolean srcFound = false;
			boolean destFound = false;
			
			do
			{
				if (scanned_node == null)
				{
					srcFound = true;
					destFound = true;
				}
				else
				{
					if(Objects.equals(scanned_node.getLabel(),getFixedLengthLable(srcLabel)))
					{
						System.out.println("Source Node found");
						sourceNid = nid; 
						srcFound = true;
					}
					if (Objects.equals(scanned_node.getLabel(),getFixedLengthLable(destLabel)))
					{
						System.out.println("Dest found");
						desNid = nid;
						destFound = true;
					}
					scanned_node = scan.getNext(nid);
				}
			}while (!srcFound || !destFound);
			
			Edge newEdge = new Edge();
			newEdge.setLabel(edgeLabel);
			newEdge.setSource(sourceNid);
			newEdge.setDestination(desNid);
			newEdge.setWeight(weight);
			edgeByteArray = newEdge.getEdgeByteArray();
			edgeId = edgeHeapFile.insertEdge(edgeByteArray);
		}
		
		System.out.println("Rec count " + edgeHeapFile.getRecCnt());
		
		// Output releavant statistics
		System.out.println("Node Count after batch insertion on graph database: " + sysdef.JavabaseDB.getNodeCnt());
		System.out.println("Edge Count after batch insertion on graph database: " + sysdef.JavabaseDB.getEdgeCnt());
		System.out.println("No. of disk pages read during batch insertion on graph database: " /*+ sysdef.JavabaseDB.pageRW.rcounter*/);
		System.out.println("No. of disk pages written during batch insertion on graph database: " /*+ sysdef.JavabaseDB.pageRW.wcounter*/);

		if ( status == OK )
			System.out.println ("  Test completed successfully.\n");
		return status; 
		}
	
	public String getFixedLengthLable(String label) {
		if(label.length() >LABEL_MAX_LENGTH){
			return label.substring(0,LABEL_MAX_LENGTH);
		}else{
			StringBuffer sb = new StringBuffer();
			int len = label.length();
			while(len<LABEL_MAX_LENGTH){
				sb.append(LABEL_CONSTANT);
				len++;
			}
			sb.append(label);
			return sb.toString();
		}
	}

	}

	public class batchedgeinsert {

		public static void main (String argv[]) throws FileNotFoundException, IOException, 
		SpaceNotAvailableException, HFBufMgrException, InvalidSlotNumberException, 
		InvalidTupleSizeException, HFException, HFDiskMgrException, GetFileEntryException, 
		ConstructPageException, AddFileEntryException {

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