package diskmgr;

import java.io.*;
import bufmgr.*;
import global.*;
import btree.*;
import zindex.*;
import heap.*;
import iterator.*;
import index.*;

public class BatchEdgeInsertHandler {
	private final static boolean OK = true;
	private final static boolean FAIL = false;
	public static final String LABEL_CONSTANT = "A";
	public static final int LABEL_MAX_LENGTH =6; 
	NodeHeapFile nodes;
	EdgeHeapFile edges;
	BTreeFile nodeLabels;
	ZCurve nodeDesc;
	BTreeFile edgeLabels;
	BTreeFile edgeWeights;
	public BatchEdgeInsertHandler(NodeHeapFile nodes, EdgeHeapFile edges, BTreeFile nodeLabels, 
		ZCurve nodeDesc, BTreeFile edgeLabels, BTreeFile edgeWeights) {
		this.nodes = nodes;
		this.edges = edges;
		this.nodeLabels = nodeLabels;
		this.nodeDesc = nodeDesc;
		this.edgeLabels = edgeLabels;
		this.edgeWeights = edgeWeights;
	}
	public boolean test1(String edgeFileName) 
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
		
		nodeHeapFile = nodes;
		edgeHeapFile = edges;
		
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
					if(scanned_node.getLabel().equals(getFixedLengthLable(srcLabel)))
					{
						System.out.println("Source Node found");
						sourceNid = nid; 
						srcFound = true;
					}
					if (scanned_node.getLabel().equals(getFixedLengthLable(destLabel)))
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
		System.out.println("Node Count after batch insertion on graph database: " + nodes.getNodeCnt());
		System.out.println("Edge Count after batch insertion on graph database: " + edges.getEdgeCnt());
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
