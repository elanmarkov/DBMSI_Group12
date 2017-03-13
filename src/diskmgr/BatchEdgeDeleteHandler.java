/*Author Harshdeep*/
package diskmgr;

import java.io.*;
import java.util.*;
import java.lang.*;
import heap.*;
import bufmgr.*;
import diskmgr.*;
import global.*;
import btree.*;
import zindex.*;


public class BatchEdgeDeleteHandler implements GlobalConst{
	NodeHeapFile nodes;
	EdgeHeapFile edges;
	BTreeFile nodeLabels;
	ZCurve nodeDesc;
	BTreeFile edgeLabels;
	BTreeFile edgeWeights;
	graphDB db;
	public BatchEdgeDeleteHandler(NodeHeapFile nodes, EdgeHeapFile edges, BTreeFile nodeLabels, 
	ZCurve nodeDesc, BTreeFile edgeLabels, BTreeFile edgeWeights, graphDB db) {

		this.nodes = nodes;
		this.edges = edges;
		this.nodeLabels = nodeLabels;
		this.nodeDesc = nodeDesc;
		this.edgeLabels = edgeLabels;
		this.edgeWeights = edgeWeights;
		this.db = db;
	}
	private final static boolean OK   = true;
	private final static boolean FAIL = false;
/*
	public void runbatchedgedelete(String dbname, String filename){

		SystemDefs   sysdefs  = new SystemDefs(dbname,100,100,"Clock");
		EdgeHeapFile edgeheap = sysdefs.JavabaseDB.edges;
		NodeHeapFile nodeheap = sysdefs.JavabaseDB.nodes;
		PCounter         pcounter = new PCounter();
		int       pages_read  = pcounter.rcounter;
		int 	  pages_write = pcounter.wcounter;
		File 	  file;
		Scanner   inputFile;
		try{
			file = new File(filename);
			inputFile = new Scanner(file);
		}
		catch(Exception e){
			System.out.println("Could not open the InputFile.");
			FAIL = true;
		}

		
		// Read lines from the file until no more are left.

		if(!FAIL){

			while (inputFile.hasNext()){		//Loop for reading the inputFile.

				// Read the next name.
				String   nextinput   = inputFile.nextLine();
				String[] edgeinput   = nextinput.split("\\s+");
				Escan    escan       = edgeheap.openScan();
				Edge     edge        = new Edge();
				EID      eid         = new EID();


				edge = escan.getNext(eid);

				if(edge==null) {

					System.out.println("No edges in Database.");
					OK = false;
				};
				while(edge!=null && OK)	
				{
					NID sourcenid = edge.getSource();
					NID desnid = edge.getDestination();
					String edgelabel = edge.getLabel();

					if(Objects.equals(edgeinput[2],edgelabel)){	// Edge Found with given edge Label.
						Node sourcenode = nodeheap.getNode(sourcenid); 
						Node desnode    = nodeheap.getNode(desnid);

						if(Objects.equals(desnode.getLabel(),edgeinput[1]) && Objects.equals(sourcenode.getLabel(),edgeinput[0])){   // Checking if the edge is the one we are looking for.
							sysdefs.JavabaseDB.deleteEdge(eid);
							OK = false;
						}

					} 

					edge = escan.getNext(eid);
				}

				escan.closescan();

			}

			inputFile.close();
			pages_read  = pcounter.rcounter - pages_read;
			pages_write = pcounter.wcounter - pages_write;
			System.out.println("Number of Pages Read: "+pages_read+" Number of Page writes performed: "+pages_write);
			int edgecnt = sysdefs.JavabaseDB.getEdgeCnt();
			int nodecnt = sysdefs.JavabaseDB.getNodeCnt();
			System.out.println("Total Edge Count: "+ edgecnt + "Total node Count: "+ nodecnt);

		}*/


}
