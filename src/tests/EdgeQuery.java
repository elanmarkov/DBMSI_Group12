package tests;

import java.io.IOException;

import btree.ConstructPageException;
import btree.IndexFileScan;
import btree.IteratorException;
import btree.KeyDataEntry;
import btree.KeyNotMatchException;
import btree.PinPageException;
import btree.ScanIteratorException;
import btree.UnpinPageException;
import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import diskmgr.graphDB;
import global.AttrType;
import global.Descriptor;
import global.EID;
import global.GlobalConst;
import global.IndexType;
import global.NID;
import global.RID;
import global.SystemDefs;
import global.TupleOrder;
import heap.Edge;
import heap.EdgeHeapFile;
import heap.Escan;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.Node;
import heap.NodeHeapFile;
import heap.Nscan;
import heap.Tuple;
import index.IndexException;
import index.IndexScan;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.Sort;
import iterator.UnknownKeyTypeException;
import zindex.ZCurve;

class NQDriver extends TestDriver implements GlobalConst
{

	private final static boolean OK = true;
	private final static boolean FAIL = false;

	public NQDriver () {
		super("nodequerytest");      
	}
	private void sortNodes(Node nodes[]) {
		Node temp;
		AttrType [] jtype = new AttrType[2];
		jtype[1] = new AttrType (AttrType.attrString);
		jtype[1] = new AttrType (AttrType.attrDesc);
		for (int i = 0; i < nodes.length; i++) 
		{
			for (int j = i + 1; j < nodes.length; j++) 
			{
				if (nodes[i].getLabel().compareTo(nodes[j].getLabel()) > 0) 
				{
					temp = nodes[i];
					nodes[i] = nodes[j];
					nodes[j] = temp;
				}
			}
		}
		for(int i = 0; i < nodes.length; i++) {
			try {
				nodes[i].print(jtype);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private void sortEdges(Edge edges[]) {
		Edge temp;
		AttrType [] jtype = new AttrType[2];
		jtype[1] = new AttrType (AttrType.attrString);
		jtype[1] = new AttrType (AttrType.attrDesc);
		for (int i = 0; i < edges.length; i++) 
		{
			for (int j = i + 1; j < edges.length; j++) 
			{
				if (edges[i].getLabel().compareTo(edges[j].getLabel()) > 0) 
				{
					temp = edges[i];
					edges[i] = edges[j];
					edges[j] = temp;
				}
			}
		}
		for(int i = 0; i < edges.length; i++) {
			try {
				edges[i].print(jtype);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

       private void sortWeights(Edge edges[]) {
		Edge temp;
		AttrType [] jtype = new AttrType[2];
		jtype[1] = new AttrType (AttrType.attrString);
		jtype[1] = new AttrType (AttrType.attrDesc);
		for (int i = 0; i < edges.length; i++) 
		{
			for (int j = i + 1; j < edges.length; j++) 
			{
				if (edges[i].getWeight()>edges[j].getWeight()) 
				{
					temp = edges[i];
					edges[i] = edges[j];
					edges[j] = temp;
				}
			}
		}
		for(int i = 0; i < edges.length; i++) {
			try {
				edges[i].print(jtype);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	private void sortNodes1(Node nodes[], String argv[]) {
		int length = nodes.length;
		double[] distance = new double[length];
		Descriptor target = new Descriptor();
		double tempDis;
		Node temp;
		AttrType [] jtype = new AttrType[2];
		jtype[1] = new AttrType (AttrType.attrString);
		jtype[1] = new AttrType (AttrType.attrDesc);
		target.set(Integer.parseInt(argv[4]), Integer.parseInt(argv[5]),Integer.parseInt(argv[6]),Integer.parseInt(argv[7]),Integer.parseInt(argv[8]));
		for(int i = 0; i < distance.length; i++) {
			distance[i] = nodes[i].getDesc().distance(target);
		}
		for (int i = 0; i < nodes.length; i++) 
		{
			for (int j = i + 1; j < nodes.length; j++) 
			{
				if (distance[i] > distance[j]) 
				{
					temp = nodes[i];
					nodes[i] = nodes[j];
					nodes[j] = temp;
					tempDis = distance[i];
					distance[i] = distance[j];
					distance[j] = tempDis;
				}
			}
		}
		for(int i = 0; i < nodes.length; i++) {
			try {
				nodes[i].print(jtype);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
	private boolean nodeIndexTest0(graphDB database, String argv[]){

		ZCurve zcurve = database.nodeDesc;
		IndexFileScan indexScan = null;
		try {
			IndexScan iscan=new IndexScan(IndexType.Z_Index, relName, indName, types, str_sizes, noInFlds, noOutFlds, outFlds, selects, fldNum, indexOnly);
			iscan.get_next();	
			indexScan = zcurve.newZFileScan(null, null);
		} catch (KeyNotMatchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IteratorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ConstructPageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (PinPageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnpinPageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		boolean done = false;
		while(!done) {
			KeyDataEntry entry = null;
			try {
				entry = indexScan.get_next();
			} catch (ScanIteratorException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if(entry == null) {
				done = true;
				break;
			}

		}

		return true;
	}
	private boolean edgeIndexTest1(graphDB database, String argv[]){
		
		boolean status = OK;
		AttrType[] attrType = new AttrType[4];
	    attrType[0] = new AttrType(AttrType.attrString);
	    attrType[1] = new AttrType(AttrType.attrInteger);
	    attrType[2] = new AttrType(AttrType.attrInteger);
	    attrType[3] = new AttrType(AttrType.attrInteger);
	    FldSpec[] projlist = new FldSpec[4];
	    RelSpec rel = new RelSpec(RelSpec.outer); 
	    projlist[0] = new FldSpec(rel, 1);
	    projlist[1] = new FldSpec(rel, 2);
	    projlist[2] = new FldSpec(rel, 3);
	    projlist[3] = new FldSpec(rel, 4);
	    short[] attrSize = new short[4];
	    attrSize[0] = 8;
	    attrSize[1] = 8;
	    attrSize[2] = 8;
	    attrSize[3] = 4;
		IndexScan iscan = null;
		String filename = database.getNodes().getFileName();
		int edgeCount = 0, i = 0;
		try {
			edgeCount = database.getEdgeCnt();
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		Edge edges[] = new Edge[edgeCount];
		//need to change test1.in to actual rel name
	    try {
	      iscan = new IndexScan(new IndexType(IndexType.B_Index), filename, "BTreeIndex", attrType, attrSize, 4, 4, projlist, null, 1, false);
	    }
	    catch (Exception e) {
	      status = FAIL;
	      e.printStackTrace();
	    }
		boolean done = false;
		if(status == OK) {
			while(!done) {
				Tuple t = new Tuple();
				try {
					t = iscan.get_next();
				} catch (IndexException e) {
					// TODO Auto-generated catch block
					status = FAIL;
					e.printStackTrace();
				} catch (UnknownKeyTypeException e) {
					// TODO Auto-generated catch block
					status = FAIL;
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					status = FAIL;
					e.printStackTrace();
					
				}
				if(t == null) {
					done = true;
					break;
				}
				nodes[i] = new Node(t);
				i++;
			}
			sortNodes(nodes);
		
		return true;
	}
	private boolean nodeIndexTest2(graphDB database, String argv[]){
		return true;
	}
	private boolean nodeIndexTest3(graphDB database, String argv[]){
		return true;
	}
	private boolean nodeIndexTest4(graphDB database, String argv[]){
		return true;
	}
	private boolean nodeIndexTest5(graphDB database, String argv[]){
		return true;
	}
	private boolean edgeHeapTest0(graphDB database, String argv[]){
		boolean status = OK;
		EID eid = new EID();
		EdgeHeapFile f = database.getEdges();

		AttrType [] jtype = new AttrType[2];
		jtype[0] = new AttrType (AttrType.attrString);
		jtype[1] = new AttrType (AttrType.attrDesc);
		try {
			//f = new NodeHeapFile("priyekant");
		}
		catch (Exception e) {
			status = FAIL;
			System.err.println ("*** Could not create heap file\n");
			e.printStackTrace();
		}
		Escan scan = null;
		if ( status == OK ) {	
			System.out.println ("  - Scan the records\n");
			try {
				scan = f.openScan();
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

		if ( status == OK ) {
			Edge edge = new Edge();

			boolean done = false;
			while (!done) { 
				try {
					edge = scan.getNext(eid);
					if (edge == null) {
						done = true;
						break;
					}
					edge.print(jtype);
				}
				catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
			}
		}
		return status;
	}
	private boolean edgHeapTest1(graphDB database, String argv[]){
		boolean status = OK;
		int i = 0;
		EID eid = new EID();
		NID nid = new NID();
		EdgeHeapFile f        = database.getEdges();
		NodeHeapFile nodeheap = database.getNodes();
		int edgeCount = 0;
		try {
			edgeCount = database.getEdgeCnt();
		} catch (HFBufMgrException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InvalidSlotNumberException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InvalidTupleSizeException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		Node[] nodes = new Node[edgeCount];

		AttrType [] jtype = new AttrType[1];
		jtype[0] = new AttrType (AttrType.attrString);
		jtype[1] = new AttrType (AttrType.attrDesc);
		try {
			//f = new NodeHeapFile("priyekant");
		}
		catch (Exception e) {
			status = FAIL;
			System.err.println ("*** Could not create heap file\n");
			e.printStackTrace();
		}
		Escan scan = null;
		if ( status == OK ) {	
			System.out.println ("  - Scan the records\n");
			try {
				scan = f.openScan();
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

		if ( status == OK ) {
			Edge edge = new Edge();
			
			boolean done = false;
			while (!done) { 
				try {
					edge = scan.getNext(eid);
					nid  = eid.getSource(eid)	
					if (edge == null) {
						done = true;
						break;
					}
					nodes[i] = f.getNode(nid);
					i++;
				}
				catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
			}
			sortNodes(nodes);
		}

		return status;
	}
	private boolean edgeHeapTest2(graphDB database, String argv[]){
		boolean status = OK;
		int i = 0;
		EID eid = new EID();
		NID nid = new NID();
		EdgeHeapFile f        = database.getEdges();
		NodeHeapFile nodeHeapFile = database.getNodes();
		int edgeCount = 0;
		try {
			edgeCount = database.getEdgeCnt();
		} catch (HFBufMgrException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InvalidSlotNumberException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InvalidTupleSizeException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		Node[] nodes = new Node[edgeCount];

		AttrType [] jtype = new AttrType[1];
		jtype[0] = new AttrType (AttrType.attrString);
		jtype[1] = new AttrType (AttrType.attrDesc);
		try {
			//f = new NodeHeapFile("priyekant");
		}
		catch (Exception e) {
			status = FAIL;
			System.err.println ("*** Could not create heap file\n");
			e.printStackTrace();
		}
		Escan scan = null;
		if ( status == OK ) {	
			System.out.println ("  - Scan the records\n");
			try {
				scan = f.openScan();
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

		if ( status == OK ) {
			Edge edge = new Edge();
			
			boolean done = false;
			while (!done) { 
				try {
					edge = scan.getNext(eid);
					nid  = eid.getDestination(eid)	
					if (edge == null) {
						done = true;
						break;
					}
					nodes[i] = nid.getNode();
					i++;
				}
				catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
			}
			sortNodes(nodes);
		}

		return status;
	}
	private boolean nodeHeapTest3(graphDB database, String argv[]){
		boolean status = OK;
		int i = 0;
		EID eid = new EID();
		EdgeHeapFile f = database.getEdges();
		int edgeCount  = 0;
		try {
			edgeCount = database.getEdgeCnt();
		} catch (HFBufMgrException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InvalidSlotNumberException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InvalidTupleSizeException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		Edge[] edges = new Edge[edgeCount];

		AttrType [] jtype = new AttrType[1];
		jtype[0] = new AttrType (AttrType.attrString);
		jtype[1] = new AttrType (AttrType.attrDesc);
		try {
			//f = new NodeHeapFile("priyekant");
		}
		catch (Exception e) {
			status = FAIL;
			System.err.println ("*** Could not create heap file\n");
			e.printStackTrace();
		}
		Escan scan = null;
		if ( status == OK ) {	
			System.out.println ("  - Scan the records\n");
			try {
				scan = f.openScan();
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

		if ( status == OK ) {
			Edge edge = new Edge();
			
			boolean done = false;
			while (!done) { 
				try {
					edge = scan.getNext(eid);	
					if (edge == null) {
						done = true;
						break;
					}
					edges[i] = edge;
					i++;
				}
				catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
			}
			sortEdges(edges);
		}

		return status;
	}
	private boolean nodeHeapTest4(graphDB database, String argv[]){
		boolean status = OK;
		int i = 0;
		EID eid = new EID();
		EdgeHeapFile f = database.getEdges();
		int edgeCount  = 0;
		try {
			edgeCount = database.getEdgeCnt();
		} catch (HFBufMgrException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InvalidSlotNumberException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InvalidTupleSizeException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		Edge[] edges = new Edge[edgeCount];

		AttrType [] jtype = new AttrType[1];
		jtype[0] = new AttrType (AttrType.attrString);
		jtype[1] = new AttrType (AttrType.attrDesc);
		try {
			//f = new NodeHeapFile("priyekant");
		}
		catch (Exception e) {
			status = FAIL;
			System.err.println ("*** Could not create heap file\n");
			e.printStackTrace();
		}
		Escan scan = null;
		if ( status == OK ) {	
			System.out.println ("  - Scan the records\n");
			try {
				scan = f.openScan();
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

		if ( status == OK ) {
			Edge edge = new Edge();
			
			boolean done = false;
			while (!done) { 
				try {
					edge = scan.getNext(eid);	
					if (edge == null) {
						done = true;
						break;
					}
					edges[i] = edge;
					i++;
				}
				catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
			}
			sortWeights(edges);
		}

		return status;

	}
	private boolean edgeHeapTest5(graphDB database, String argv[]){
                
                boolean status = OK;
		int i = 0;
		EID eid = new EID();
		EdgeHeapFile f = database.getEdges();
		System.out.println("Enter the lower bound of Weights:");
                BufferedReader in = new BufferedReader (new InputStreamReader(System.in));
		int  lowerbound, upperbound;
		try {
      		 lowerbound = Integer.parseInt(in.readLine());
                 }
                catch (NumberFormatException e) {
                 lowerbound = 0;
                 }
                catch (IOException e) {
                 lowerbound = 0;
                 }
		System.out.println("Enter the upper bound of Weights:");
		try {
      		 upperbound = Integer.parseInt(in.readLine());
                 }
                catch (NumberFormatException e) {
                 upperbound = 0;
                 }
                catch (IOException e) {
                 upperbound = 0;
                 }
		int edgeCount  = 0;
		try {
			edgeCount = database.getEdgeCnt();
		} catch (HFBufMgrException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InvalidSlotNumberException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InvalidTupleSizeException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		Edge[] edges = new Edge[edgeCount];

		AttrType [] jtype = new AttrType[1];
		jtype[0] = new AttrType (AttrType.attrString);
		jtype[1] = new AttrType (AttrType.attrDesc);
		try {
			//f = new NodeHeapFile("priyekant");
		}
		catch (Exception e) {
			status = FAIL;
			System.err.println ("*** Could not create heap file\n");
			e.printStackTrace();
		}
		Escan scan = null;
		if ( status == OK ) {	
			System.out.println ("  - Scan the records\n");
			try {
				scan = f.openScan();
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

		if ( status == OK ) {
			Edge edge = new Edge();
			
			boolean done = false;
			while (!done) { 
				try {
					edge = scan.getNext(eid);	
					if (edge == null) {
						done = true;
						break;
					}
					if(lowerbound<=edge.getWeight()&&edge.getWeight()<=upperbound){
					 edge.print(jtype);						
					}
				}
				catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
			}
			
		}

		return status;

	}


       private boolean edgeHeapTest6(graphDB database, String argv[]){
                
                boolean status = OK;
		int i = 0;
		EID eid = new EID();
		EdgeHeapFile f = database.getEdges();
		int edgeCount  = 0;
		try {
			edgeCount = database.getEdgeCnt();
		} catch (HFBufMgrException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InvalidSlotNumberException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InvalidTupleSizeException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		RID edges[edgeCount][2];
		AttrType [] jtype = new AttrType[1];
		jtype[0] = new AttrType (AttrType.attrString);
		jtype[1] = new AttrType (AttrType.attrDesc);
		try {
			//f = new NodeHeapFile("priyekant");
		}
		catch (Exception e) {
			status = FAIL;
			System.err.println ("*** Could not create heap file\n");
			e.printStackTrace();
		}
		Escan scan = null;
		if ( status == OK ) {	
			System.out.println ("  - Scan the records\n");
			try {
				scan = f.openScan();
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

		if ( status == OK ) {
			
			NID source,destination;
			EID eid;
			Edge edge = new Edge();
			int i=0;
			edge = scan.getNext(eid);
			while(edge!=null){
			     
			   try{
			    edges[i][0] = eid;
		            edges[i][1] = edge.getSource();
			    edges[i][2] = edge.getDestination();
			    }
			   catch(Exception e){
			    System.err.println(""+e);
			    }
			   i++; 
			 }
			i=0;
			while(i<edgeCount){
			 int j=i+1;
			 while(j<edgeCount){
			  if(Objects.equals(edges[i][1],edges[j][2]) || Objects.equals(edges[i][2],edges[j][1])){
			    
			    Edge edge1 = f.getEdge(edges[i][0]);
			    Edge edge2 = f.getEdge(edges[j][0]);
			    
			    try {
			     edge1.print(jtype);
			     edge2.print(jtype);	
			     } 
                            catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			     }			    

			   }
			 }						
			}
			  
	         }
	        scan.closescan();
	}
		
		

		return status;

      }
	public boolean runTests(String argv[]) {
		// Kill anything that might be hanging around
		//System.out.println(argv[0]);
		dbpath = argv[0];
		SystemDefs sysdef = new SystemDefs(dbpath,1000,Integer.parseInt(argv[1]),"Clock");
		graphDB database = SystemDefs.JavabaseDB;
		try {
			database = new graphDB(0);
		} catch (InvalidSlotNumberException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (InvalidTupleSizeException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (HFException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (HFBufMgrException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (HFDiskMgrException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (Exception e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		String newdbpath;
		String newlogpath;
		String remove_logcmd;
		String remove_dbcmd;
		String remove_cmd = "/bin/rm -rf ";

		newdbpath = dbpath;
		newlogpath = logpath;

		remove_logcmd = remove_cmd + logpath;
		remove_dbcmd = remove_cmd + dbpath;

		// Commands here is very machine dependent.  We assume
		// user are on UNIX system here
		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		}
		catch (IOException e) {
			System.err.println ("IO error: "+e);
		}

		remove_logcmd = remove_cmd + newlogpath;
		remove_dbcmd = remove_cmd + newdbpath;

		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		}
		catch (IOException e) {
			System.err.println ("IO error: "+e);
		}

		/////////////////////////////////
		boolean _pass = false;
		switch(Integer.parseInt(argv[2])) {
		case 0:
			if(Integer.parseInt(argv[3]) == 1) {
				_pass = nodeIndexTest0(database,argv);
			} else {
				_pass = nodeHeapTest0(database,argv);
			}
			break;
		case 1:
			if(Integer.parseInt(argv[3]) == 1) {
				_pass = nodeIndexTest1(database,argv);
			} else {
				_pass = nodeHeapTest1(database,argv);
			}
			break;
		case 2:
			if(Integer.parseInt(argv[3]) == 1) {
				_pass = nodeIndexTest2(database,argv);
			} else {
				_pass = nodeHeapTest2(database,argv);
			}
			break;
		case 3:
			if(Integer.parseInt(argv[3]) == 1) {
				_pass = nodeIndexTest3(database,argv);
			} else {
				_pass = nodeHeapTest3(database,argv);
			}
			break;
		case 4:
			if(Integer.parseInt(argv[3]) == 1) {
				_pass = nodeIndexTest4(database,argv);
			} else {
				_pass = nodeHeapTest4(database,argv);
			}
			break;
		case 5:
			if(Integer.parseInt(argv[3]) == 1) {
				_pass = nodeIndexTest5(database,argv);
			} else {
				_pass = nodeHeapTest5(database,argv);
			}
			break;
		default:
			System.out.println("Not supported");
		}
		//////////////////////////////////
		//Clean up again
		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		}
		catch (IOException e) {
			System.err.println ("IO error: "+e);
		}
		return _pass;
	}

}
public class edgequery {
	public static void main(String argv[]) {
		NQDriver hd = new NQDriver();
		boolean dbstatus;

		dbstatus = hd.runTests(argv);

		if (dbstatus != true) {
			System.err.println ("Error encountered during nodequery tests:\n");
			Runtime.getRuntime().exit(1);
		}

		Runtime.getRuntime().exit(0);
	}

}

