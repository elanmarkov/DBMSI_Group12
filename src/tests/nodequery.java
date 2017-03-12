package tests;

import java.io.IOException;

import btree.AddFileEntryException;
import btree.ConstructPageException;
import btree.GetFileEntryException;
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
		boolean status = OK;
		AttrType[] attrType = new AttrType[2];
	    attrType[0] = new AttrType(AttrType.attrString);
	    attrType[1] = new AttrType(AttrType.attrDesc);
	    FldSpec[] projlist = new FldSpec[2];
	    RelSpec rel = new RelSpec(RelSpec.outer); 
	    projlist[0] = new FldSpec(rel, 1);
	    projlist[1] = new FldSpec(rel, 2);
	    short[] attrSize = new short[2];
	    attrSize[0] = 8;
	    attrSize[1] = 10;
		IndexScan iscan = null;
		String filename = database.getNodes()._fileName;
		//need to change test1.in to actual rel name
	    try {
	      iscan = new IndexScan(new IndexType(IndexType.Z_Index), filename, "ZTreeIndex", attrType, attrSize, 2, 2, projlist, null, 2, false);
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
				} catch (IndexException e1) {
					// TODO Auto-generated catch block
					status = FAIL;
					e1.printStackTrace();
				} catch (UnknownKeyTypeException e1) {
					// TODO Auto-generated catch block
					status = FAIL;
					e1.printStackTrace();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					status = FAIL;
					e1.printStackTrace();
				}
				if(t == null) {
					done = true;
					break;
				}
				Node n = new Node(t);
				try {
					n.print(attrType);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					status = FAIL;
					e.printStackTrace();
				}
			}
		}
		return true;
	}
	private boolean nodeIndexTest1(graphDB database, String argv[]){
		boolean status = OK;
		AttrType[] attrType = new AttrType[2];
	    attrType[0] = new AttrType(AttrType.attrString);
	    attrType[1] = new AttrType(AttrType.attrDesc);
	    FldSpec[] projlist = new FldSpec[2];
	    RelSpec rel = new RelSpec(RelSpec.outer); 
	    projlist[0] = new FldSpec(rel, 1);
	    projlist[1] = new FldSpec(rel, 2);
	    short[] attrSize = new short[2];
	    attrSize[0] = 8;
	    attrSize[1] = 10;
		IndexScan iscan = null;
		String filename = database.getNodes()._fileName;
		int nodeCount = 0, i = 0;
		try {
			nodeCount = database.getNodeCnt();
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		Node nodes[] = new Node[nodeCount];
		//need to change test1.in to actual rel name
	    try {
	      iscan = new IndexScan(new IndexType(IndexType.B_Index), filename, "BTreeIndex", attrType, attrSize, 2, 2, projlist, null, 1, false);
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
		}
		return status;
	}
	private boolean nodeIndexTest2(graphDB database, String argv[]){
		boolean status = OK;
		AttrType[] attrType = new AttrType[2];
	    attrType[0] = new AttrType(AttrType.attrString);
	    attrType[1] = new AttrType(AttrType.attrDesc);
	    FldSpec[] projlist = new FldSpec[2];
	    RelSpec rel = new RelSpec(RelSpec.outer); 
	    projlist[0] = new FldSpec(rel, 1);
	    projlist[1] = new FldSpec(rel, 2);
	    short[] attrSize = new short[2];
	    attrSize[0] = 8;
	    attrSize[1] = 10;
		IndexScan iscan = null;
		String filename = database.getNodes()._fileName;
		int nodeCount = 0, i = 0;
		try {
			nodeCount = database.getNodeCnt();
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		Node nodes[] = new Node[nodeCount];
		//need to change test1.in to actual rel name
	    try {
	      iscan = new IndexScan(new IndexType(IndexType.B_Index), filename, "BTreeIndex", attrType, attrSize, 2, 2, projlist, null, 1, false);
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
			sortNodes1(nodes,argv);
		}
		return status;
	}
	private boolean nodeIndexTest3(graphDB database, String argv[]){
		Descriptor desc = new Descriptor();
		desc.set(Integer.parseInt(argv[4]), Integer.parseInt(argv[5]), Integer.parseInt(argv[6]), Integer.parseInt(argv[7]), Integer.parseInt(argv[8]));
		double distance = Double.parseDouble(argv[9]);
		boolean status = OK;
		AttrType[] attrType = new AttrType[2];
	    attrType[0] = new AttrType(AttrType.attrString);
	    attrType[1] = new AttrType(AttrType.attrDesc);
	    FldSpec[] projlist = new FldSpec[2];
	    RelSpec rel = new RelSpec(RelSpec.outer); 
	    projlist[0] = new FldSpec(rel, 1);
	    projlist[1] = new FldSpec(rel, 2);
	    short[] attrSize = new short[2];
	    attrSize[0] = 8;
	    attrSize[1] = 10;
		IndexScan iscan = null;
		String filename = database.getNodes()._fileName;
		//need to change test1.in to actual rel name
	    try {
	      iscan = new IndexScan(new IndexType(IndexType.Z_Index), filename, "ZTreeIndex", attrType, attrSize, 2, 2, projlist, null, 2, false);
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
				} catch (IndexException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (UnknownKeyTypeException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				if(t == null) {
					done = true;
					break;
				}
				Node n = new Node(t);
				if(n.getDesc().distance(desc) == distance)
					System.out.println(n.getLabel());
			}
		}
		return status;
	}
	private boolean nodeIndexTest4(graphDB database, String argv[]){
		return true;
	}
	private boolean nodeIndexTest5(graphDB database, String argv[]){
		return true;
	}
	private boolean nodeHeapTest0(graphDB database, String argv[]){
		boolean status = OK;
		NID nid = new NID();
		NodeHeapFile f = database.getNodes();

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
		Nscan scan = null;
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
			Node node = new Node();

			boolean done = false;
			while (!done) { 
				try {
					node = scan.getNext(nid);
					if (node == null) {
						done = true;
						break;
					}
					node.print(jtype);
				}
				catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
			}
			scan.closescan();
		}
		return status;
	}
	private boolean nodeHeapTest1(graphDB database, String argv[]){
		boolean status = OK;
		int i = 0;
		NID nid = new NID();
		NodeHeapFile f = database.getNodes();
		int nodeCount = 0;
		try {
			nodeCount = database.getNodeCnt();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		Node[] nodes = new Node[nodeCount];

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
		Nscan scan = null;
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
			Node node = new Node();

			boolean done = false;
			while (!done) { 
				try {
					node = scan.getNext(nid);
					if (node == null) {
						done = true;
						break;
					}
					nodes[i] = node;
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
	private boolean nodeHeapTest2(graphDB database, String argv[]){
		boolean status = OK;
		int i = 0;
		NID nid = new NID();
		NodeHeapFile f = database.getNodes();
		int nodeCount = 0;
		try {
			nodeCount = database.getNodeCnt();
		} catch (Exception e1) {
			e1.printStackTrace();
		} 
		Node[] nodes = new Node[nodeCount];
		
		Nscan scan = null;
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
			Node node = new Node();
			boolean done = false;
			while (!done) { 
				try {
					node = scan.getNext(nid);
					if (node == null) {
						done = true;
						break;
					}
					nodes[i] = node;
					i++;
				}
				catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
			}
			scan.closescan();
			sortNodes1(nodes,argv);
		}
		return status;
	}
	private boolean nodeHeapTest3(graphDB database, String argv[]){
		Descriptor desc = new Descriptor();
		desc.set(Integer.parseInt(argv[4]), Integer.parseInt(argv[5]), Integer.parseInt(argv[6]), Integer.parseInt(argv[7]), Integer.parseInt(argv[8]));
		double distance = Double.parseDouble(argv[9]);
		boolean status = OK;
		NID nid = new NID();
		NodeHeapFile f = database.getNodes();

		AttrType [] jtype = new AttrType[2];
		jtype[0] = new AttrType (AttrType.attrString);
		jtype[1] = new AttrType (AttrType.attrDesc);
		
		Nscan scan = null;
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
			Node node = new Node();

			boolean done = false;
			while (!done) { 
				try {
					node = scan.getNext(nid);
					if (node == null) {
						done = true;
						break;
					}
					if(node.getDesc().distance(desc) == distance)
						System.out.println(node.getLabel());
				}
				catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
			}
			scan.closescan();
		}
		return status;
	}
	private boolean nodeHeapTest4(graphDB database, String argv[]){
		boolean status = OK;
		NID nid = new NID();
		NodeHeapFile f = database.getNodes();
		boolean nodeExists = false;
		String nodeLabel = argv[5];
		Node refNode = new Node();
		refNode.setLabel(nodeLabel);
		String incomingEdges[] = null;
		String outgoingEdges[] = null;
		int incomingEdgeCount = 0;
		int outgoingEdgeCount = 0;
		AttrType [] jtype = new AttrType[2];
		jtype[0] = new AttrType (AttrType.attrString);
		jtype[1] = new AttrType (AttrType.attrDesc);
		Nscan scan = null;
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
			Node node = new Node();

			boolean done = false;
			while (!done) { 
				try {
					node = scan.getNext(nid);
					if (node == null) {
						done = true;
						break;
					}
					if(node.getLabel().equals(refNode.getLabel())) {
						nodeExists = true;
						refNode = node;
						break;
					}
				}
				catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
			}
			scan.closescan();
		}
		int edgeCount = 0;
		try {
			edgeCount = database.getEdgeCnt();
		} catch (Exception e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		incomingEdges = new String[edgeCount];
		outgoingEdges = new String[edgeCount];
		if(nodeExists) {
			status = OK;
			EID eid = new EID();
			EdgeHeapFile f1 = database.getEdges();

			Escan escan = null;
			if ( status == OK ) {
				System.out.println ("  - Scan the records\n");
				try {
					escan = f1.openScan();
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
						edge = escan.getNext(eid);
						if (edge == null) {
							done = true;
							break;
						}
						if(f.getNode(edge.getSource()).getLabel().equals(refNode.getLabel())) {
							outgoingEdges[outgoingEdgeCount] = edge.getLabel();
							outgoingEdgeCount++;
						} else if(f.getNode(edge.getDestination()).getLabel().equals(refNode.getLabel())) {
							incomingEdges[incomingEdgeCount] = edge.getLabel();
							incomingEdgeCount++;
						}
					}
					catch (Exception e) {
						status = FAIL;
						e.printStackTrace();
					}
				}
				escan.closescan();
				try {
					refNode.print(jtype);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("Incoming Edges are:");
				for(int i = 0; i < incomingEdgeCount; i++) {
					System.out.print(incomingEdges[i] + "	");
				}
				System.out.println("Outgoing Edges are:");
				for(int i = 0; i < outgoingEdgeCount; i++) {
					System.out.print(outgoingEdges[i] + "	");
				}
			}
		} else {
			System.out.println("Entered node label does not exist");
		}
		return status;
	}
	private boolean nodeHeapTest5(graphDB database, String argv[]){
		Descriptor desc = new Descriptor();
		desc.set(Integer.parseInt(argv[4]), Integer.parseInt(argv[5]), Integer.parseInt(argv[6]), Integer.parseInt(argv[7]), Integer.parseInt(argv[8]));
		double distance = Double.parseDouble(argv[9]);
		boolean status = OK;
		NID nid = new NID();
		NodeHeapFile f = database.getNodes();
		int nodeCount = 0;
		int i =0;
		String outgoingEdges[][] = null;
		String incomingEdges[][] = null;
		int outgoingEdgeCount[] = null;
		int incomingEdgeCount[] = null;
		try{
			nodeCount = database.getNodeCnt();
		} catch (Exception e) {
			e.printStackTrace();
		}
		Node[] nodes = new Node[nodeCount];

		AttrType [] jtype = new AttrType[2];
		jtype[0] = new AttrType (AttrType.attrString);
		jtype[1] = new AttrType (AttrType.attrDesc);

		Nscan scan = null;
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
			Node node = new Node();

			boolean done = false;
			while (!done) { 
				try {
					node = scan.getNext(nid);
					if (node == null) {
						done = true;
						break;
					}
					if(node.getDesc().distance(desc) == distance) {
						nodes[i] = node;
						i++;
					}
				}
				catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
			}
			scan.closescan();
		}
		incomingEdges = new String[nodes.length][];
		outgoingEdges = new String[nodes.length][];
		incomingEdgeCount = new int[nodes.length];
		outgoingEdgeCount = new int[nodes.length];
		if(i > 0) {
			status = OK;
			EID eid = new EID();
			EdgeHeapFile f1 = database.getEdges();

			Escan escan = null;
			if ( status == OK ) {
				System.out.println ("  - Scan the records\n");
				try {
					escan = f1.openScan();
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
						edge = escan.getNext(eid);
						if (edge == null) {
							done = true;
							break;
						}
						for(int j = 0; j < nodes.length; j++) {
							if(f.getNode(edge.getSource()).getLabel().equals(nodes[i].getLabel())) {
								outgoingEdges[j][outgoingEdgeCount[j]] = edge.getLabel();
								outgoingEdgeCount[j]++;
							} else if(f.getNode(edge.getDestination()).getLabel().equals(nodes[i].getLabel())) {
								incomingEdges[j][incomingEdgeCount[j]] = edge.getLabel();
								incomingEdgeCount[j]++;
							}
						}
					}
					catch (Exception e) {
						status = FAIL;
						e.printStackTrace();
					}
				}
				escan.closescan();
				for(int j = 0;j < nodes.length; j++) {
					try {
						nodes[j].print(jtype);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					System.out.println("Incoming Edges are:");
					for(int i1 = 0; i1 < incomingEdgeCount[j]; i1++) {
						System.out.print(incomingEdges[j][i1] + "	");
					}
					System.out.println("Outgoing Edges are:");
					for(int i1 = 0; i1 < outgoingEdgeCount[j]; i1++) {
						System.out.print(outgoingEdges[j][i1] + "	");
					}
				}
			}
		} else {
			System.out.println("There is no node which has the given distance from the target descriptor");
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
			database.init();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
		/*try {
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
		}*/

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
		/*try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		}
		catch (IOException e) {
			System.err.println ("IO error: "+e);
		}*/
		return _pass;
	}

}
public class nodequery {
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
