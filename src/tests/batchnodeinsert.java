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

/** Class for the batch node insert test.
Passes parameters to handler and runs program. */
	public class batchnodeinsert {
	/** Passes parameters to handler for current DB and outputs result of test. */
		public boolean runTests(String NodeFile) throws
		FileNotFoundException, IOException, SpaceNotAvailableException, HFBufMgrException, Exception,
		InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException {
			System.out.println ("\nRunning Batch Node Insert tests....\n");
			BatchNodeInsertHandler queries = SystemDefs.JavabaseDB.getBatchNodeInsertHandler();
			boolean _pass = queries.test1(NodeFile);
			System.out.print ("\n" + "... Batch Node Insert tests ");
			System.out.print (_pass==true ? "completed successfully." : "failed.");
			System.out.print (".\n\n");
			return _pass;
		}
	}
