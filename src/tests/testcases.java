/*
Test Driver for Graph Database test cases by ____.
CSE 510 Project, Group 12.
*/
package tests;

import global.GlobalConst;
import global.SystemDefs;

import java.io.FileNotFoundException;
import java.util.Scanner;
import diskmgr.PCounter;

/** Driver class for the test cases for the Graph Database. */
public class testcases {
	/** Prints the read/write count on the DB as of the last operation. */
	private static void printReadWriteCount() {
		System.out.println("\nNo. of pages read : " + PCounter.rcounter);
		System.out.println("No. of pages write : " + PCounter.wcounter);
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
	Also displays menus and read/write statistics. */
	public static void main(String args[])
	{
		SystemDefs sysdef; 
		boolean exit = false;
		boolean graphExists = false;
		PCounter pc = new PCounter();
		printLongMenu();
		do {	
			printShortMenu();
			Scanner sc = new Scanner(System.in);
			String line = sc.nextLine();
			String[] splited = line.split("\\s+");
			
			if(splited[0].equals("batchnodeinsert")) {
				if(!graphExists){
					sysdef = new SystemDefs(splited[2],1000,GlobalConst.NUMBUF+100,"Clock");
					try {
					SystemDefs.JavabaseDB.init();
					} catch (Exception e) {
						e.printStackTrace();
					}
					graphExists = true;
				}
				batchnodeinsert insertObj = new batchnodeinsert();
				try {
					insertObj.runTests(splited[1],pc);
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			} else if(splited[0].equals("batchedgeinsert")) {
				if(!graphExists) {
					System.out.println("Graph DB does not exist");
				} else {
					batchedgeinsert insertObj = new batchedgeinsert();
					try {
					insertObj.runTests(splited[1],pc);
					} catch(Exception e) {
						e.printStackTrace();
					}
				}
			} else if(splited[0].equals("batchnodedelete")) {
				if(!graphExists) {
					System.out.println("Graph DB does not exist");
				} else {
					BatchNodeDelete deleteObj = new BatchNodeDelete();
					deleteObj.runDeleteNode(splited);
				}
			} else if(splited[0].equals("batchedgedelete")) {
				if(!graphExists) {
					System.out.println("Graph DB does not exist");
				} else {
					BatchEdgeDelete deleteObj = new BatchEdgeDelete();
					try {
						deleteObj.batchedgedeletefunction(splited);
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}
			} else if(splited[0].equals("nodequery")) {
				try {
					//SystemDefs.JavabaseBM.resetNumBuffers(100);
				} catch (Exception e) {
					e.printStackTrace();
				}
				PCounter.initialize();
				String[] newSplited = new String[splited.length-1];
				System.arraycopy(splited, 1, newSplited, 0, newSplited.length);
				if(!graphExists) {
					System.out.println("Graph DB does not exist");
				} else {
					nodequery nQuery = new nodequery();
					boolean _pass = nQuery.runTests(newSplited);
					if(_pass) {
						printReadWriteCount();
					}
				}
			} else if(splited[0].equals("edgequery")) {
				PCounter.initialize();
				String[] newSplited = new String[splited.length-1];
				System.arraycopy(splited, 1, newSplited, 0, newSplited.length);
				if(!graphExists) {
					System.out.println("Graph DB does not exist");
				} else {
					EdgeQuery eQuery = new EdgeQuery();
					boolean _pass = eQuery.runTests(newSplited);
					if(_pass) {
						printReadWriteCount();
					}
				}
			} else if(splited[0].equals("exit")) {
				exit = true;
				sc.close();
			} else if(splited[0].equals("menu")) {
				printLongMenu();
			}
			
		} while(!exit);
	}
}
