package diskmgr;

import java.io.*;
import bufmgr.*;
import global.*;
import btree.*;
import zindex.*;
import heap.*;
import iterator.*;
import index.*;

public class BatchNodeInsertHandler {
	private final static boolean OK = true;
	private final static boolean FAIL = false;
	NodeHeapFile nodes;
	EdgeHeapFile edges;
	BTreeFile nodeLabels;
	ZCurve nodeDesc;
	BTreeFile edgeLabels;
	BTreeFile edgeWeights;
	public BatchNodeInsertQueryHandler(NodeHeapFile nodes, EdgeHeapFile edges, BTreeFile nodeLabels, 
		ZCurve nodeDesc, BTreeFile edgeLabels, BTreeFile edgeWeights) {
		this.nodes = nodes;
		this.edges = edges;
		this.nodeLabels = nodeLabels;
		this.nodeDesc = nodeDesc;
		this.edgeLabels = edgeLabels;
		this.edgeWeights = edgeWeights;
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
		int[] descVal = new int[5];
		byte[] nodeByteArray;
		AttrType[] ntype = new AttrType[2];
		ntype[1] = new AttrType(AttrType.attrString);
		ntype[0] = new AttrType(AttrType.attrDesc);
		
		try
		{
			nodeHeapFile = nodes;//new NodeHeapFile("file_1");
		}
		catch(Exception e)
		{
			System.err.println ("*** Error in creating node heap file\n");
			e.printStackTrace();
			return FAIL;
		}
		

		BufferedReader br = new BufferedReader(new FileReader(System.getProperty("user.dir")
				+ "/tests/" + nodefilename + ".txt"));

		while ((line = br.readLine()) != null)
		{
			String[] splited = line.split("\\s+");
			nodelabel = splited[0];

			// Set the node fields
			Node newnode = new Node();
			newnode.setLabel(nodelabel);
			Descriptor nodedesc = new Descriptor();
			nodedesc.set(Integer.parseInt(splited[1]), Integer.parseInt(splited[2]), 
					Integer.parseInt(splited[3]), Integer.parseInt(splited[4]), 
					Integer.parseInt(splited[5]));
			newnode.setDesc(nodedesc);
			nodeByteArray = newnode.getNodeByteArray();
			nodeid = nodeHeapFile.insertNode(nodeByteArray);

		}
		br.close();

		System.out.println("Rec count " + nodeHeapFile.getRecCnt());

		// Output relevant statistics
		int rcount = pc.rcounter;
		int wcount = pc.wcounter;
		System.out.println("Node Count after batch insertion on graph database: " + sysdef.JavabaseDB.getNodeCnt());
		System.out.println("Edge Count after batch insertion on graph database: " + sysdef.JavabaseDB.getEdgeCnt());
		System.out.println("No. of disk pages read during batch insertion on graph database: " + rcount);
		System.out.println("No. of disk pages written during batch insertion on graph database: " + rcount);
		
			if ( status == OK )
				System.out.println ("  Test completed successfully.\n");
			return status; 
		
		/*NodeHeapFile nodeHeapFile = sysdef.JavabaseDB.getNodes();
		System.out.println("rec count" + nodeHeapFile.getNodeCnt());
		Nscan scan = null;

		AttrType[] ntype = new AttrType[2];
		ntype[1] = new AttrType(AttrType.attrString);
		ntype[0] = new AttrType(AttrType.attrDesc);
		NID n = new NID();
		boolean status = OK;
		if ( status == OK ) {	
			System.out.println ("  - Scan the records just inserted\n");

			try {
				scan = nodeHeapFile.openScan();
			}
			catch (Exception e) {
				status = FAIL;
				System.err.println ("*** Error opening scan\n");
				e.printStackTrace();
			}

			if ( status == OK &&  SystemDefs.JavabaseBM.getNumUnpinnedBuffers() 
					== SystemDefs.JavabaseBM.getNumBuffers() ) {
				System.err.println ("*** The heap-file scan has not pinned the first page\n");
				status = FAIL;
			}
		}	
		

		int len, i = 0;
		DummyRecord rec = null;
		Node node = new Node();
        node = scan.getNext(n);
		boolean done = false;
		while(!done){
		try {
			System.out.println("in scan");
				node = scan.getNext(n);
				if(node != null){
					System.out.println("BatchNodeInsertDriver.test1() "+node.getLength());
				}
			}
			catch (Exception e) {
				//status = FAIL;
				e.printStackTrace();
			}
			if(node == null){
				done = true;
				break;
			}
			node.print(ntype);
		}
		scan.closescan();

		return true;*/
		}
}
}
