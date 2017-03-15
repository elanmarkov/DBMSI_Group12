/* Batch Node Insert Handler by Shalmali bhoir
 * 
 */

package diskmgr;

import java.io.*;

import global.*;
import btree.*;
import zindex.*;
import heap.*;

public class BatchNodeInsertHandler {
	private final static boolean OK = true;
	private final static boolean FAIL = false;
	NodeHeapFile nodes;
	EdgeHeapFile edges;
	BTreeFile nodeLabels, edgeLabels, edgeWeights;
	ZCurve nodeDesc;
	graphDB db;
	
	public BatchNodeInsertHandler(NodeHeapFile nodes, EdgeHeapFile edges, BTreeFile nodeLabels, 
	ZCurve nodeDesc, BTreeFile edgeLabels, BTreeFile edgeWeights, graphDB db) {

		this.nodes = nodes;
		this.edges = edges;
		this.nodeLabels = nodeLabels;
		this.nodeDesc = nodeDesc;
		this.edgeLabels = edgeLabels;
		this.edgeWeights = edgeWeights;
		this.db = db;
	}
	
	public boolean test1(String nodefilename, PCounter pc)
			throws FileNotFoundException, IOException, SpaceNotAvailableException, 
			HFBufMgrException, InvalidSlotNumberException, InvalidTupleSizeException, 
			HFException, HFDiskMgrException, Exception
	{
		boolean status = OK;
		String line, nodelabel;
		NID n = new NID();
		NID nodeid = new NID(); 
		NodeHeapFile nodeHeapFile;
		
		int rcount = pc.rcounter;
		int wcount = pc.wcounter;
		
		// Access NodeHeapFile in the graph database
		try
		{
			nodeHeapFile = nodes;
		}
		catch(Exception e)
		{
			System.err.println ("*** Error in creating node heap file\n");
			e.printStackTrace();
			return FAIL;
		}
		

		BufferedReader br = new BufferedReader(new FileReader(System.getProperty("user.dir")
				+ "/" + nodefilename));

		while ((line = br.readLine()) != null)
		{
			String[] splited = line.split("\\s+");
			nodelabel = splited[0];

			// Set the node fields and insert the node
			Node newnode = new Node();
			newnode.setLabel(nodelabel);
			
			Descriptor nodedesc = new Descriptor();
			nodedesc.set(Integer.parseInt(splited[1]), Integer.parseInt(splited[2]), 
					Integer.parseInt(splited[3]), Integer.parseInt(splited[4]), 
					Integer.parseInt(splited[5]));
			
			newnode.setDesc(nodedesc);
			
			try{
				db.insertNode(newnode);
			} catch(Exception e){
				e.printStackTrace();
			}
		}
		br.close();
		
		// Output relevant statistics
		rcount = pc.rcounter - rcount;
		wcount = pc.wcounter - wcount;
		System.out.println("Node Count after batch insertion on graph database: " + nodes.getNodeCnt());
		System.out.println("Edge Count after batch insertion on graph database: " + edges.getEdgeCnt());
		System.out.println("No. of disk pages read during batch insertion on graph database: " + rcount);
		System.out.println("No. of disk pages written during batch insertion on graph database: " + wcount);
		
		return status; 
	}
}

