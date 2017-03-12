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
		BatchNodeInsertHandler queries = sysdef.getBatchNodeInsertHandler();
		//Run the tests. Return type different from C++
		boolean _pass = queries.test1(NodeFile, pc);

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




