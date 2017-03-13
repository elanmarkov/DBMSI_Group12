package tests;

import global.SystemDefs;

import java.io.FileNotFoundException;
import java.util.Scanner;
import diskmgr.PCounter;


public class testcases {
	private static void printReadWriteCount() {
		System.out.println("\nNo. of pages read : " + PCounter.rcounter);
		System.out.println("No. of pages write : " + PCounter.wcounter);
	}
	public static void main(String args[])
	{
		SystemDefs sysdef; 
		boolean exit = false;
		boolean graphExists = false;
		PCounter pc = new PCounter();
		do {
			Scanner sc = new Scanner(System.in);
			String line = sc.nextLine();
			String[] splited = line.split("\\s+");
			
			if(splited[0].equals("batchnodeinsert")) {
				if(!graphExists){
					sysdef = new SystemDefs(splited[2],1000,100,"Clock");
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
			}
			
		} while(!exit);
	}
}
