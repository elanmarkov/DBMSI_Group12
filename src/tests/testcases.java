/*
Test Driver for Graph Database test cases by ____.
CSE 510 Project, Group 12.
 */
package tests;

import global.SystemDefs;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import diskmgr.PCounter;

/** Driver class for the test cases for the Graph Database. */
public class testcases {
	/** Prints the read/write count on the DB as of the last operation. */
	private static void flushPages() {
		try {
			if(SystemDefs.JavabaseDB != null)
				SystemDefs.JavabaseDB.closeAllFiles();
			if(SystemDefs.JavabaseDB != null)
				SystemDefs.JavabaseBM.flushAllPages();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	private static void printReadWriteCount() {
		System.out.println("\nNo. of pages read : " + PCounter.getRCount());
		System.out.println("No. of pages write : " + PCounter.getWCount());
	}
	/** Full menu for graph tests. Gives input format of all queries.*/
	private static void printLongMenu() {
		System.out.println("Graph Database Test Cases ");
		System.out.println("Format of command line input:\n");

		System.out.println("Batch Node Insert (Task 10 Query):");
		System.out.println("batchnodeinsert NODEFILENAME GRAPHDBNAME");
		System.out.println("NODEFILENAME should be a file in tests folder\n");

		System.out.println("Batch Edge Insert (Task 11 Query):");
		System.out.println("batchedgeinsert EDGEFILENAME GRAPHDBNAME");
		System.out.println("EDGEFILENAME should be a file in tests folder\n");

		System.out.println("Batch Node Delete (Task 12 Query):");
		System.out.println("batchnodedelete NODEFILENAME GRAPHDBNAME");
		System.out.println("NODEFILENAME should be a file in tests folder\n");

		System.out.println("Batch Edge Delete (Task 13 Query):");
		System.out.println("batchnodeinsert EDGEFILENAME GRAPHDBNAME");
		System.out.println("EDGEFILENAME should be a file in tests folder\n");

		System.out.println("Simple Node Query (Task 14 Query):");
		System.out.println("nodequery GRAPHDBNAME NUMBUF QTYPE INDEX [QUERYOPTIONS]\n");

		System.out.println("Simple Edge Query (Task 15 Query):");
		System.out.println("edgequery GRAPHDBNAME NUMBUF QTYPE INDEX [QUERYOPTIONS]\n\n");
	}
	/** Short menu for graph tests. Gives list of possible entries.*/
	private static void printShortMenu() {
		System.out.println("Enter menu to print the menu, exit to exit, or a command line input to execute:");
	}
	/** Main test driver for Graph Database tests. Runs any of the six tests as specified by command line input.
	Also displays menus and read/write statistics. 
	 * @throws FileNotFoundException */
	public static void main(String args[]) throws FileNotFoundException
	{
		SystemDefs sysdef;
		boolean exit = false;
		String graphDB = null;
		int numBuf = 5000;
		printLongMenu();
		do {
			printShortMenu();
			PCounter.initialize();
			Scanner sc = new Scanner(System.in);
			String line = sc.nextLine();
			String[] splited = line.split("\\s+");
			if(splited[0].equals("batchnodeinsert") || splited[0].equals("batchedgeinsert") ||
					splited[0].equals("batchnodedelete") || splited[0].equals("batchedgedelete")) {
				graphDB = splited[2];
			} else if(splited[0].equals("nodequery") || splited[0].equals("edgequery") || splited[0].equals("sortMergeJoin") || splited[0].equals("triangleQuery")) {
				graphDB = splited[1];
				numBuf = Integer.parseInt(splited[2]);
			}
			File f = new File(graphDB);
			if(f.exists()) {
				SystemDefs.MINIBASE_RESTART_FLAG = true;
			} else {
				SystemDefs.MINIBASE_RESTART_FLAG = false;
			}
			try {
				sysdef = new SystemDefs(graphDB,5000,numBuf,"Clock");
				SystemDefs.JavabaseDB.init();
			} catch (Exception e) {
				e.printStackTrace();
			}

			if(splited[0].equals("batchnodeinsert")) {
				batchnodeinsert insertObj = new batchnodeinsert();
				try {
					insertObj.runTests(splited[1]);
				} catch (Exception e) {
					e.printStackTrace();
				}
				flushPages();
			} else if(splited[0].equals("batchedgeinsert")) {
				batchedgeinsert insertObj = new batchedgeinsert();
				try {
					insertObj.runTests(splited[1]);
				} catch(Exception e) {
					e.printStackTrace();
				}
				flushPages();
			} else if(splited[0].equals("batchnodedelete")) {
				BatchNodeDelete deleteObj = new BatchNodeDelete();
				deleteObj.runDeleteNode(splited);
				flushPages();
			} else if(splited[0].equals("batchedgedelete")) {
				BatchEdgeDelete deleteObj = new BatchEdgeDelete();
				try {
					deleteObj.batchedgedeletefunction(splited);
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				flushPages();
			} else if(splited[0].equals("nodequery")) {
				String[] newSplited = new String[splited.length-1];
				System.arraycopy(splited, 1, newSplited, 0, newSplited.length);
				nodequery nQuery = new nodequery();
				nQuery.runTests(newSplited);
			} else if(splited[0].equals("edgequery")) {
				String[] newSplited = new String[splited.length-1];
				System.arraycopy(splited, 1, newSplited, 0, newSplited.length);
				EdgeQuery eQuery = new EdgeQuery();
				eQuery.runTests(newSplited);
			} else if(splited[0].equals("exit")) {
				exit = true;
				sc.close();
			} else if(splited[0].equals("menu")) {
				printLongMenu();
			} else if(splited[0].equals("sortMergeJoin")) {
				if(splited.length == 4)
					SMJoinEdge.performSortMergeJoin(splited[3]);
				else 
					SMJoinEdge.performSortMergeJoin(null);
				flushPages();
			} else if(splited[0].equals("triangleQuery")) {
				if(splited.length > 3) {
					TriangleQueryTest tq = new TriangleQueryTest(splited[3]);
					tq=null;
				}
				else {
					System.out.println("Queries not formatted properly.");
				}
				flushPages();
			}
			printReadWriteCount();
		} while(!exit);
	}
}
