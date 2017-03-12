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
		BatchEdgeInsertHandler queries = sysdef.getBatchEdgeInsertHandler();
		//Run the tests. Return type different from C++
		boolean _pass = queries.test1(edgeFile);

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
