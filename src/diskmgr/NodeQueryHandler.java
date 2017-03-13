package diskmgr;

import java.io.*;
import bufmgr.*;
import global.*;
import btree.*;
import zindex.*;
import heap.*;
import iterator.*;
import index.*;

public class NodeQueryHandler {
	private final static boolean OK = true;
	private final static boolean FAIL = false;
	NodeHeapFile nodes;
	EdgeHeapFile edges;
	BTreeFile nodeLabels;
	ZCurve nodeDesc;
	BTreeFile edgeLabels;
	BTreeFile edgeWeights;
	graphDB db;
	public NodeQueryHandler(NodeHeapFile nodes, EdgeHeapFile edges, BTreeFile nodeLabels, 
		ZCurve nodeDesc, BTreeFile edgeLabels, BTreeFile edgeWeights, graphDB db) {
		this.nodes = nodes;
		this.edges = edges;
		this.nodeLabels = nodeLabels;
		this.nodeDesc = nodeDesc;
		this.edgeLabels = edgeLabels;
		this.edgeWeights = edgeWeights;
		this.db = db;
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
	public boolean nodeIndexTest0(String argv[]){
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
		String filename = nodes.getFileName();
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
	public boolean nodeIndexTest1(String argv[]){
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
		String filename = nodes.getFileName();
		int nodeCount = 0, i = 0;
		try {
			nodeCount = nodes.getNodeCnt();
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
	public boolean nodeIndexTest2(String argv[]){
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
		String filename = nodes.getFileName();
		int nodeCount = 0, i = 0;
		try {
			nodeCount = nodes.getNodeCnt();
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
	public boolean nodeIndexTest3(String argv[]){
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
		String filename = nodes.getFileName();
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
	public boolean nodeIndexTest4(String argv[]){
		boolean status = OK;
		NID nid = new NID();
		NodeHeapFile f = nodes;
		boolean nodeExists = false;
		String nodeLabel = argv[5];
		Node refNode = new Node();
		refNode.setLabel(nodeLabel);
		
		AttrType[] attrType = new AttrType[2];
	    attrType[0] = new AttrType(AttrType.attrString);
	    attrType[1] = new AttrType(AttrType.attrDesc);
	    FldSpec[] projlist = new FldSpec[2];
	    RelSpec rel = new RelSpec(RelSpec.outer); 
	    projlist[0] = new FldSpec(rel, 1);
	    projlist[1] = new FldSpec(rel, 2);
	    
	    short[] attrSize = new short[4];
	    attrSize[0] = 8;
	    attrSize[1] = 10;
	    
	    AttrType[] EattrType = new AttrType[4];
	    attrType[0] = new AttrType(AttrType.attrString);
	    attrType[1] = new AttrType(AttrType.attrInteger);
	    attrType[2] = new AttrType(AttrType.attrInteger);
	    attrType[3] = new AttrType(AttrType.attrString);
	    FldSpec[] Eprojlist = new FldSpec[2];
	    RelSpec Erel = new RelSpec(RelSpec.outer); 
	    Eprojlist[0] = new FldSpec(rel, 1);
	    Eprojlist[1] = new FldSpec(rel, 2);
	    short[] EattrSize = new short[4];
	    EattrSize[0] = 4;
	    EattrSize[1] = 8;
	    EattrSize[2] = 8;
	    EattrSize[3] = 8;
	    
		String incomingEdges[] = null;
		String outgoingEdges[] = null;
		int incomingEdgeCount = 0;
		int outgoingEdgeCount = 0;
		String Nfilename = nodes.getFileName();
		String Efilename = edges.getFileName();
		IndexScan isscan = null;
		IndexScan eiscan = null;
		
		try {
		      isscan = new IndexScan(new IndexType(IndexType.B_Index), Nfilename, "BTreeIndex", attrType, attrSize, 2, 2, projlist, null, 2, false);
		    }
		    catch (Exception e) {
		      status = FAIL;
		      e.printStackTrace();
		    }

		if ( status == OK ) {
			Tuple t = new Tuple();

			boolean done = false;
			while (!done) { 
					
					try {
						t = isscan.get_next();
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
					if(n.getLabel().equals(refNode.getLabel())) {
						nodeExists = true;
						refNode = n;
						break;
					}
					
				}
				
		return status;	
			
		}

		try {
			incomingEdges = new String[edges.getEdgeCnt()];
			outgoingEdges = new String[edges.getEdgeCnt()];
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
		} catch (Exception e) {
			e.printStackTrace();
		}
		if(nodeExists) {
			
			try {
			      eiscan = new IndexScan(new IndexType(IndexType.B_Index), Efilename, "BTreeIndex", EattrType, EattrSize, 2, 2, Eprojlist, null, 2, false);
			    }
			    catch (Exception e) {
			      status = FAIL;
			      e.printStackTrace();
			    }

			if ( status == OK ) {
				Tuple et = new Tuple();
				boolean done = false;
				while (!done) { 
					try {
						et = eiscan.get_next();
						if (et == null) {
							done = true;
							break;
						}
						Edge e = new Edge(et);
						if(f.getNode(e.getSource()).getLabel().equals(refNode.getLabel())) {
							outgoingEdges[outgoingEdgeCount] = e.getLabel();
							outgoingEdgeCount++;
						} else if(f.getNode(e.getDestination()).getLabel().equals(refNode.getLabel())) {
							incomingEdges[incomingEdgeCount] = e.getLabel();
							incomingEdgeCount++;
						}
					}
					catch (Exception e) {
						status = FAIL;
						e.printStackTrace();
					}
				}
				
				try {
					refNode.print(attrType);
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
	public boolean nodeIndexTest5(String argv[]){
		
		Descriptor desc = new Descriptor();
		desc.set(Integer.parseInt(argv[4]), Integer.parseInt(argv[5]), Integer.parseInt(argv[6]), Integer.parseInt(argv[7]), Integer.parseInt(argv[8]));
		double distance = Double.parseDouble(argv[9]);
		boolean status = OK;
		NID nid = new NID();
		NodeHeapFile f = nodes;
		int nodeCount = 0;
		int i =0;
		String outgoingEdges[][] = null;
		String incomingEdges[][] = null;
		int outgoingEdgeCount[] = null;
		int incomingEdgeCount[] = null;
		String Nfilename = nodes.getFileName();
		String Efilename = edges.getFileName();
		
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
	    
	    AttrType[] EattrType = new AttrType[4];
	    attrType[0] = new AttrType(AttrType.attrString);
	    attrType[1] = new AttrType(AttrType.attrInteger);
	    attrType[2] = new AttrType(AttrType.attrInteger);
	    attrType[3] = new AttrType(AttrType.attrString);
	    FldSpec[] Eprojlist = new FldSpec[2];
	    RelSpec Erel = new RelSpec(RelSpec.outer); 
	    Eprojlist[0] = new FldSpec(rel, 1);
	    Eprojlist[1] = new FldSpec(rel, 2);
	    short[] EattrSize = new short[4];
	    EattrSize[0] = 4;
	    EattrSize[1] = 8;
	    EattrSize[2] = 8;
	    EattrSize[3] = 8;
		
		try{
			nodeCount = nodes.getNodeCnt();
		} catch (Exception e) {
			e.printStackTrace();
		}
		Node[] nodesArray = new Node[nodeCount];

		IndexScan iscan = null;
		IndexScan eiscan = null;
		try {
		      iscan = new IndexScan(new IndexType(IndexType.Z_Index), Nfilename, "ZTreeIndex", attrType, attrSize, 2, 2, projlist, null, 2, false);
		    }
		    catch (Exception e) {
		      status = FAIL;
		      e.printStackTrace();
		    }

		if ( status == OK ) {
			Tuple t = new Tuple();

			boolean done = false;
			while (!done) { 
				try {
					t = iscan.get_next();
					if (t == null) {
						done = true;
						break;
					}
					Node n = new Node(t);
					if(n.getDesc().distance(desc) == distance) {
						nodesArray[i] = n;
						i++;
					}
				}
				catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
			}
			
		}
		incomingEdges = new String[nodesArray.length][];
		outgoingEdges = new String[nodesArray.length][];
		incomingEdgeCount = new int[nodesArray.length];
		outgoingEdgeCount = new int[nodesArray.length];
		if(i > 0) {
			status = OK;
			EID eid = new EID();
			EdgeHeapFile f1 = edges;

			if ( status == OK ) {
				try {
				      eiscan = new IndexScan(new IndexType(IndexType.B_Index), Efilename, "BTreeIndex", EattrType, EattrSize, 2, 2, Eprojlist, null, 2, false);
				    }
				    catch (Exception e) {
				      status = FAIL;
				      e.printStackTrace();
				    }
				}

			}

			if ( status == OK ) {
				Tuple t = new Tuple();
				boolean done = false;
				while (!done) { 
					try {
						t = eiscan.get_next();
						if (t == null) {
							done = true;
							break;
						}
						Edge e = new Edge(t);
						
						for(int j = 0; j < nodesArray.length; j++) {
							if(f.getNode(e.getSource()).getLabel().equals(nodesArray[i].getLabel())) {
								outgoingEdges[j][outgoingEdgeCount[j]] = e.getLabel();
								outgoingEdgeCount[j]++;
							} else if(f.getNode(e.getDestination()).getLabel().equals(nodesArray[i].getLabel())) {
								incomingEdges[j][incomingEdgeCount[j]] = e.getLabel();
								incomingEdgeCount[j]++;
							}
						}
					}
					catch (Exception e) {
						status = FAIL;
						e.printStackTrace();
					}
				}
				
				for(int j = 0;j < nodesArray.length; j++) {
					try {
						nodesArray[j].print(attrType);
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
			else {
				System.out.println("There is no node which has the given distance from the target descriptor");
				} 
			return status;
			}
	

	public boolean nodeHeapTest0(String argv[]){
		boolean status = OK;
		NID nid = new NID();
		NodeHeapFile f = nodes;

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
	public boolean nodeHeapTest1(String argv[]){
		boolean status = OK;
		int i = 0;
		NID nid = new NID();
		NodeHeapFile f = nodes;
		int nodeCount = 0;
		try {
			nodeCount = nodes.getNodeCnt();
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
		} catch (Exception e) {
			e.printStackTrace();
		}
		Node[] nodesArray = new Node[nodeCount];

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
					nodesArray[i] = node;
					i++;
				}
				catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
			}
			sortNodes(nodesArray);
		}

		return status;
	}
	public boolean nodeHeapTest2(String argv[]){
		boolean status = OK;
		int i = 0;
		NID nid = new NID();
		NodeHeapFile f = nodes;
		int nodeCount = 0;
		try {
			nodeCount = nodes.getNodeCnt();
		} catch (Exception e1) {
			e1.printStackTrace();
		} 
		Node[] nodesArray = new Node[nodeCount];
		
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
					nodesArray[i] = node;
					i++;
				}
				catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
			}
			scan.closescan();
			sortNodes1(nodesArray,argv);
		}
		return status;
	}
	public boolean nodeHeapTest3(String argv[]){
		Descriptor desc = new Descriptor();
		desc.set(Integer.parseInt(argv[4]), Integer.parseInt(argv[5]), Integer.parseInt(argv[6]), Integer.parseInt(argv[7]), Integer.parseInt(argv[8]));
		double distance = Double.parseDouble(argv[9]);
		boolean status = OK;
		NID nid = new NID();
		NodeHeapFile f = nodes;

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
	public boolean nodeHeapTest4(String argv[]){
		boolean status = OK;
		NID nid = new NID();
		NodeHeapFile f = nodes;
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

		try {
			incomingEdges = new String[edges.getEdgeCnt()];
			outgoingEdges = new String[edges.getEdgeCnt()];
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
		} catch (Exception e) {
			e.printStackTrace();
		}
		if(nodeExists) {
			status = OK;
			EID eid = new EID();
			EdgeHeapFile f1 = edges;

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
	public boolean nodeHeapTest5(String argv[]){
		Descriptor desc = new Descriptor();
		desc.set(Integer.parseInt(argv[4]), Integer.parseInt(argv[5]), Integer.parseInt(argv[6]), Integer.parseInt(argv[7]), Integer.parseInt(argv[8]));
		double distance = Double.parseDouble(argv[9]);
		boolean status = OK;
		NID nid = new NID();
		NodeHeapFile f = nodes;
		int nodeCount = 0;
		int i =0;
		String outgoingEdges[][] = null;
		String incomingEdges[][] = null;
		int outgoingEdgeCount[] = null;
		int incomingEdgeCount[] = null;
		try{
			nodeCount = nodes.getNodeCnt();
		} catch (Exception e) {
			e.printStackTrace();
		}
		Node[] nodesArray = new Node[nodeCount];

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
						nodesArray[i] = node;
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
		incomingEdges = new String[nodesArray.length][];
		outgoingEdges = new String[nodesArray.length][];
		incomingEdgeCount = new int[nodesArray.length];
		outgoingEdgeCount = new int[nodesArray.length];
		if(i > 0) {
			status = OK;
			EID eid = new EID();
			EdgeHeapFile f1 = edges;

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
						for(int j = 0; j < nodesArray.length; j++) {
							if(f.getNode(edge.getSource()).getLabel().equals(nodesArray[i].getLabel())) {
								outgoingEdges[j][outgoingEdgeCount[j]] = edge.getLabel();
								outgoingEdgeCount[j]++;
							} else if(f.getNode(edge.getDestination()).getLabel().equals(nodesArray[i].getLabel())) {
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
				for(int j = 0;j < nodesArray.length; j++) {
					try {
						nodesArray[j].print(jtype);
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

}
