/*Batch Node Delete by Harshdeep Singh Sandhu*/
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

public class BatchNodeDeleteHandler{
	NodeHeapFile nodes;
	EdgeHeapFile edges;
	BTreeFile nodeLabels;
	ZCurve nodeDesc;
	BTreeFile edgeLabels;
	BTreeFile edgeWeights;
	graphDB db;
	public BatchNodeDeleteHandler(NodeHeapFile nodes, EdgeHeapFile edges, BTreeFile nodeLabels,
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

	public void runbatchnodedelete(String dbname, String filename) throws Exception{
		boolean status = OK;
		//  BatchNodeDelete Start
		NodeHeapFile nodeheap    = nodes;
		File         file = null;

		try{
			file = new File(System.getProperty("user.dir")	//Opening the NODEFILENAME File.
					+ "/" + filename + ".txt");
		}
		catch(Exception e){
			System.out.println("Could not open the InputFile.");
			status = FAIL;
		}
		System.out.println("Its Running");
		Scanner      inputFile    = new Scanner(file);
		EdgeHeapFile     edgeheap     = edges;

		// Read lines from the file until no more are left.
		if(status){
			System.out.println("BatchNodeDeleteHandler.runbatchnodedelete() 1");
			while (inputFile.hasNext())	//while 01 for going through the BatchNodeFile
			{
				System.out.println("BatchNodeDeleteHandler.runbatchnodedelete() 2");
				// Read the next name.
				String inputnodelabel = inputFile.nextLine();
				Nscan           nscan = nodeheap.openScan();
				Node             node = new Node();
				NID               nid = new NID();
				node = nscan.getNext(nid);
				Node          tempNode = new Node();
				tempNode.setLabel(inputnodelabel);

				while(node!=null){	//while 02 for going through the nodeheapfile looking for the particular node

					String label = node.getLabel();
					if(Objects.equals(label,tempNode.getLabel())){	// nid with the given nodelabel found
						System.out.println(node.equals(null));
						Escan escan = edgeheap.openScan();
						EID eid = new EID();
						Edge edge = new Edge();
						edge = escan.getNext(eid);
						while(!Objects.equals(edge,null)){		// trying to find the edges with destination and source node
							if(edge.getSource().equals(nid) || edge.getDestination().equals(nid)){
								try {
									System.out.println(edge.getLabel());
									db.deleteEdge(eid);
								}
								catch(Exception e) {
									System.out.println("Error during deleting the Edge");
									e.printStackTrace();
									status = FAIL;
								}
							}

							edge = escan.getNext(eid);
						}
						escan.closescan();

						try {
							System.out.println(node.getLabel());
							db.deleteNode(nid);
						}
						catch(Exception e) {
							System.out.println("Error during node deletion.");
							e.printStackTrace();
							status = FAIL;
						}
					}
					node = nscan.getNext(nid);
				}

				nscan.closescan();
			}

			inputFile.close();
		}
		// Taking care of Output now.

		if(status) {

		System.out.println("..............Batch Node Deletion Performed successfully............");}
		else {
			System.out.println("..............Batch Node Delete Not Successful................");
		}

		System.out.println("Number of Total Nodes in the Database are:"+SystemDefs.JavabaseDB.getNodeCnt() + "Number of Total Edges in the Database are"+SystemDefs.JavabaseDB.getEdgeCnt());


	}

}
