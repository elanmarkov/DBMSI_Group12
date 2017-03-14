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

	public void runbatchedgedelete(String dbname, String filename) throws InvalidSlotNumberException, HFException, HFDiskMgrException, HFBufMgrException, Exception{
        boolean status = OK;
		EdgeHeapFile edgeheap = edges;
		NodeHeapFile nodeheap = nodes;
		PCounter         pcounter = new PCounter();
		int       pages_read  = pcounter.rcounter;
		int 	  pages_write = pcounter.wcounter;
		File 	  file;
		Scanner   inputFile=null;
		try{
			file = new File(System.getProperty("user.dir")
					+ "/" + filename);
			inputFile = new Scanner(file);
		}
		catch(Exception e){
			System.out.println("Could not open the InputFile.");
			
		}

		
		// Read lines from the file until no more are left.

		if(status){

			while (inputFile.hasNext()){		//Loop for reading the inputFile.

				// Read the next name.
				String   nextinput   = inputFile.nextLine();
				String[] edgeinput   = nextinput.split("\\s+");
				Escan escan=null;
				try {
					escan = edgeheap.openScan();
				} catch (InvalidTupleSizeException | IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				Edge     edge        = new Edge();
				EID      eid         = new EID();


				edge = escan.getNext(eid);

				if(Objects.equals(edge,null)) {

					System.out.println("No edges in Database.");
				    status = FAIL;
				};
				while(!Objects.equals(edge,null))	
				{
					NID sourcenid = edge.getSource();
					NID desnid = edge.getDestination();
					String edgelabel = edge.getLabel();
					Edge tempedge = new Edge();
					tempedge.setLabel(edgeinput[2]);
					Node node1 = new Node();
					Node node2 = new Node();
					node2.setLabel(edgeinput[1]);
					node1.setLabel(edgeinput[0]);
					

					if(Objects.equals(tempedge.getLabel(),edgelabel)){	// Edge Found with given edge Label.
						Node sourcenode = nodeheap.getNode(sourcenid); 
						Node desnode    = nodeheap.getNode(desnid);

						if(Objects.equals(desnode.getLabel(),node2.getLabel()) && Objects.equals(sourcenode.getLabel(),node1.getLabel())){   // Checking if the edge is the one we are looking for.
							db.deleteEdge(eid);
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
			int edgecnt = db.getEdgeCnt();
			int nodecnt = db.getNodeCnt();
			System.out.println("Total Edge Count: "+ edgecnt + "Total node Count: "+ nodecnt);

		}
	}

}
