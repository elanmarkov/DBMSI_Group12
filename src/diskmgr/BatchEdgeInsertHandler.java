/* Batch Edge Insert Handler by Shalmali Bhoir
 * 
 */
package diskmgr;

import java.io.*;
import java.util.Objects;

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
	public static final String LABEL_CONSTANT = "0";
	public static final int LABEL_MAX_LENGTH =6; 
	NodeHeapFile nodes;
	EdgeHeapFile edges;
	BTreeFile nodeLabels, edgeLabels, edgeWeights;
	ZCurve nodeDesc;
	graphDB db;
	
	public BatchEdgeInsertHandler(NodeHeapFile nodes, EdgeHeapFile edges, BTreeFile nodeLabels, 
	ZCurve nodeDesc, BTreeFile edgeLabels, BTreeFile edgeWeights, graphDB db) {

		this.nodes = nodes;
		this.edges = edges;
		this.nodeLabels = nodeLabels;
		this.nodeDesc = nodeDesc;
		this.edgeLabels = edgeLabels;
		this.edgeWeights = edgeWeights;
		this.db = db;
	}
	
	public boolean test1(String edgeFileName,PCounter pc) 
			throws FileNotFoundException, IOException, SpaceNotAvailableException, 
			HFBufMgrException, InvalidSlotNumberException, InvalidTupleSizeException, 
			HFException, HFDiskMgrException {
		boolean status = OK;
		String line, nodelabel;
		EID eId = new EID();
		EID edgeId = new EID();
		Node scanned_node;

		int rcount = pc.rcounter;
		int wcount = pc.wcounter;
		
		NodeHeapFile nodeHeapFile = nodes;
		
		BufferedReader br = new BufferedReader(new FileReader(System.getProperty("user.dir")
				+ "/" +  edgeFileName));
		Nscan scan =null;
		while ((line = br.readLine()) != null)
		{
			String[] splited = line.split("\\s+");
			String srcLabel = Tuple.getFixedLengthLable(splited[0]);
			String destLabel = Tuple.getFixedLengthLable(splited[1]);
			String edgeLabel = splited[2];
			int weight = Integer.parseInt(splited[3]);
			NID sourceNid = new NID();
			NID desNid = new NID();
			
			boolean srcFound = false;
			boolean destFound = false;
			
			scan = nodeHeapFile.openScan();
			NID nid = new NID();
			scanned_node = scan.getNext(nid); 
			do
			{	
				if (Objects.equals(scanned_node,null))
				{
					srcFound = true;
					destFound = true;
				}
				else
				{
					if(scanned_node.getLabel().equals(srcLabel))
					{
						sourceNid = new NID(new PageId(nid.pageNo.pid),nid.slotNo); 
						try {
							Node n = nodeHeapFile.getNode(nid);
						} catch (Exception e) {
							e.printStackTrace();
						}
						srcFound = true;
					}
					if (scanned_node.getLabel().equals(destLabel))
					{
						desNid = new NID(new PageId(nid.pageNo.pid),nid.slotNo);
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
			try{
				db.insertEdge(newEdge);
			} catch (Exception e) {
				e.printStackTrace();
			}
			if(scan!=null){
				scan.closescan();
			}
		}
		
		try {
			edgeLabels.close();
			edgeWeights.close();
		} catch (PageUnpinnedException | InvalidFrameNumberException | HashEntryNotFoundException
				| ReplacerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Output releavant statistics
		rcount = pc.rcounter - rcount;
		wcount = pc.wcounter - wcount;
		System.out.println("Node Count after batch insertion on graph database: " + nodes.getNodeCnt());
		System.out.println("Edge Count after batch insertion on graph database: " + edges.getEdgeCnt());
		System.out.println("No. of disk pages read during batch insertion on graph database: " + pc.rcounter);
		System.out.println("No. of disk pages written during batch insertion on graph database: " + pc.wcounter);

		return status; 
		}
}
