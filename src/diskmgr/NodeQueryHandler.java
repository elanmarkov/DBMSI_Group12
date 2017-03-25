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
		jtype[0] = new AttrType (AttrType.attrString);
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
		jtype[0] = new AttrType (AttrType.attrString);
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
		attrSize[0] = Node.LABEL_MAX_LENGTH;
		attrSize[1] = 10;
		IndexScan iscan = null;
		String filename = nodes.getFileName();
		//need to change test1.in to actual rel name
		try {
			iscan = new IndexScan(new IndexType(IndexType.Z_Index), filename, "GraphDB0NODEDESC", attrType, attrSize, 2, 2, projlist, null, 2, false);
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
				} catch (Exception e1) {
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
			try {
				iscan.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return status;
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
		attrSize[0] = Node.LABEL_MAX_LENGTH;
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
			iscan = new IndexScan(new IndexType(IndexType.B_Index), filename, "GraphDB0NODELABEL" , attrType, attrSize, 2, 2, projlist, null, 1, false);
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
				} catch (Exception e) {
					// TODO Auto-generated catch block
					status = FAIL;
					e.printStackTrace();
				}
				if(t == null) {
					done = true;
					break;
				}
				nodes[i] = new Node(t);
				try {
					nodes[i].print(attrType);
				} catch(Exception e) {
					e.printStackTrace();
				}
				i++;
			}
			//sortNodes(nodes);
			try {
				iscan.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
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
		attrSize[0] = Node.LABEL_MAX_LENGTH;
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
			iscan = new IndexScan(new IndexType(IndexType.B_Index), filename, "GraphDB0NODELABEL", attrType, attrSize, 2, 2, projlist, null, 1, false);
		}
		catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		Tuple t = null;
		boolean done = false;
		if(status == OK) {
			while(!done) {
				
				try {
					Tuple check = iscan.get_next();
					if (check == null){
						done = true;
						break;
					}
					t = new Tuple(check); 
				} catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
				nodes[i] = new Node(t);
				/*AttrType [] jtype = new AttrType[2];
				jtype[0] = new AttrType (AttrType.attrString);
				jtype[1] = new AttrType (AttrType.attrDesc);
				try {
					nodes[i].print(jtype);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}*/
				i++;
			}
			sortNodes1(nodes,argv);
			/*AttrType [] jtype = new AttrType[2];
			jtype[0] = new AttrType (AttrType.attrString);
			jtype[1] = new AttrType (AttrType.attrDesc);
			for(int as = 0; as < nodes.length; as++) {
				try {
					System.out.println("asd\n");
					nodes[as].print(jtype);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}*/
			try {
				iscan.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
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
		attrSize[0] = Node.LABEL_MAX_LENGTH;
		attrSize[1] = 10;
		IndexScan iscan = null;
		CondExpr[] expr = new CondExpr[2];
	    expr[0] = new CondExpr();
	    expr[0].op = new AttrOperator(AttrOperator.aopLE);
	    expr[0].operand1.desc = desc;
	    expr[0].type1 = new AttrType(AttrType.attrDesc);
	    expr[0].type2 = new AttrType(AttrType.attrSymbol);
	    expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
	    expr[0].distance = distance;
	    expr[0].next = null;
	    expr[1] = null;
		String filename = nodes.getFileName();
		try {
			iscan = new IndexScan(new IndexType(IndexType.Z_Index), filename, "GraphDB0NODEDESC", attrType, attrSize, 2, 2, projlist, expr, 2, false);
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
				} catch (Exception e1) {
					e1.printStackTrace();
				} 
				if(t == null) {
					done = true;
					break;
				}
				Node n = new Node(t);
				//if(n.getDesc().distance(desc) == distance)
					System.out.println(n.getLabel());
			}
			try {
				iscan.close();
			} catch (IndexException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				iscan.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return status;
	}
	
	public boolean nodeIndexTest4(String argv[]){
		boolean status = OK;
		NID nid = new NID();
		NodeHeapFile f = nodes;
		boolean nodeExists = false;
		String nodeLabel = argv[4];
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
		attrSize[0] = Node.LABEL_MAX_LENGTH;
		attrSize[1] = 10;

		AttrType[] EattrType = new AttrType[6];
		EattrType[0] = new AttrType(AttrType.attrString);
		EattrType[1] = new AttrType(AttrType.attrInteger);
		EattrType[2] = new AttrType(AttrType.attrInteger);
		EattrType[3] = new AttrType(AttrType.attrInteger);
		EattrType[4] = new AttrType(AttrType.attrInteger);
		EattrType[5] = new AttrType(AttrType.attrInteger);
		FldSpec[] Eprojlist = new FldSpec[6];
		RelSpec erel = new RelSpec(RelSpec.outer); 
		Eprojlist[0] = new FldSpec(erel, 1);
		Eprojlist[1] = new FldSpec(erel, 2);
		Eprojlist[2] = new FldSpec(erel, 3);
		Eprojlist[3] = new FldSpec(erel, 4);
		Eprojlist[4] = new FldSpec(erel, 5);
		Eprojlist[5] = new FldSpec(erel, 6);
		short[] EattrSize = new short[6];
		EattrSize[0] = Node.LABEL_MAX_LENGTH;
		EattrSize[1] = 4;
		EattrSize[2] = 4;
		EattrSize[3] = 4;
		EattrSize[4] = 4;
		EattrSize[5] = 4;

		String incomingEdges[] = null;
		String outgoingEdges[] = null;
		int incomingEdgeCount = 0;
		int outgoingEdgeCount = 0;
		String Nfilename = nodes.getFileName();
		String Efilename = edges.getFileName();
		IndexScan isscan = null;
		IndexScan eiscan = null;

		try {
			isscan = new IndexScan(new IndexType(IndexType.B_Index), Nfilename, "GraphDB0NODELABEL", attrType, attrSize, 2, 2, projlist, null, 1, false);
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
				} catch (Exception e1) {
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
			try {
				isscan.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		try {
			incomingEdges = new String[nodes.getNodeCnt()];
			outgoingEdges = new String[nodes.getNodeCnt()];
		} catch (Exception e1) {
			e1.printStackTrace();
		} 
		if(nodeExists) {

			try {
				eiscan = new IndexScan(new IndexType(IndexType.B_Index), Efilename, "GraphDB0EDGELABEL", EattrType, EattrSize, 6, 6, Eprojlist, null, 1, false);
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
						et = eiscan.getNextEdge();
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
				try {
					eiscan.close();
				} catch (Exception e) {
					e.printStackTrace();
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
		short[] attrSize = new short[6];
		attrSize[0] = Node.LABEL_MAX_LENGTH;
		attrSize[1] = 10;

		AttrType[] EattrType = new AttrType[6];
		EattrType[0] = new AttrType(AttrType.attrString);
		EattrType[1] = new AttrType(AttrType.attrInteger);
		EattrType[2] = new AttrType(AttrType.attrInteger);
		EattrType[3] = new AttrType(AttrType.attrInteger);
		EattrType[4] = new AttrType(AttrType.attrInteger);
		EattrType[5] = new AttrType(AttrType.attrInteger);
		FldSpec[] Eprojlist = new FldSpec[6];
		RelSpec erel = new RelSpec(RelSpec.outer); 
		Eprojlist[0] = new FldSpec(erel, 1);
		Eprojlist[1] = new FldSpec(erel, 2);
		Eprojlist[2] = new FldSpec(erel, 3);
		Eprojlist[3] = new FldSpec(erel, 4);
		Eprojlist[4] = new FldSpec(erel, 5);
		Eprojlist[5] = new FldSpec(erel, 6);
		short[] EattrSize = new short[6];
		EattrSize[0] = Node.LABEL_MAX_LENGTH;
		EattrSize[1] = 4;
		EattrSize[2] = 4;
		EattrSize[3] = 4;
		EattrSize[4] = 4;
		EattrSize[5] = 4;

		try{
			nodeCount = nodes.getNodeCnt();
		} catch (Exception e) {
			e.printStackTrace();
		}
		Node[] nodesArray = new Node[nodeCount];
		CondExpr[] expr = new CondExpr[2];
		expr[0] = new CondExpr();
	    expr[0].op = new AttrOperator(AttrOperator.aopLE);
	    expr[0].operand1.desc = desc;
	    expr[0].type1 = new AttrType(AttrType.attrDesc);
	    expr[0].type2 = new AttrType(AttrType.attrSymbol);
	    expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
	    expr[0].distance = distance;
	    expr[0].next = null;
	    expr[1] = null;
	    
		IndexScan iscan = null;
		IndexScan eiscan = null;
		try {
			iscan = new IndexScan(new IndexType(IndexType.Z_Index), Nfilename, "GraphDB0NODEDESC", attrType, attrSize, 2, 2, projlist, expr, 2, false);
		}
		catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		
		if ( status == OK ) {
			Tuple t = null;
			boolean done = false;
			while (!done) {
				try {
					Tuple check = iscan.get_next();
					if (check == null) {
						done = true;
						break;
					}
					t = new Tuple(check);
					Node n = new Node(t);
					//if(n.getDesc().distance(desc) == distance) {
						nodesArray[i] = n;
						i++;
					//}
					n.print(attrType);
				}
				catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
			}
			try {
				iscan.close();
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		incomingEdges = new String[nodesArray.length][nodesArray.length];
		outgoingEdges = new String[nodesArray.length][nodesArray.length];
		incomingEdgeCount = new int[nodesArray.length];
		outgoingEdgeCount = new int[nodesArray.length];
		if(i > 0) {
			status = OK;
			if ( status == OK ) {
				try {
					eiscan = new IndexScan(new IndexType(IndexType.B_Index), Efilename, "GraphDB0EDGELABEL", EattrType, EattrSize, 6, 6, Eprojlist, null, 1, false);
				}
				catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
			}

			if ( status == OK ) {
				Tuple t = new Tuple();
				boolean done = false;
				while (!done) { 
					try {
						t = eiscan.getNextEdge();
						if (t == null) {
							done = true;
							break;
						}
						Edge e = new Edge(t);

						for(int j = 0; j < i; j++) {
							if(f.getNode(e.getSource()).getLabel().equals(nodesArray[j].getLabel())) {
								outgoingEdges[j][outgoingEdgeCount[j]] = e.getLabel();
								outgoingEdgeCount[j]++;
							} else if(f.getNode(e.getDestination()).getLabel().equals(nodesArray[j].getLabel())) {
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

				for(int j = 0;j < i; j++) {
					try {
						System.out.println("\n");
						nodesArray[j].print(attrType);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					System.out.println("\nIncoming Edges are:");
					for(int i1 = 0; i1 < incomingEdgeCount[j]; i1++) {
						System.out.print(incomingEdges[j][i1] + "	");
					}
					System.out.println("\nOutgoing Edges are:");
					for(int i1 = 0; i1 < outgoingEdgeCount[j]; i1++) {
						System.out.print(outgoingEdges[j][i1] + "	");
					}
				}
				try {
					eiscan.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} else {
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
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			status = FAIL;
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
			sortNodes(nodesArray);
			scan.closescan();
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
					if(node.getDesc().distance(desc) <= distance)
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
		String nodeLabel = argv[4];
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
						Node n = f.getNode(edge.getSource());
						Node d = f.getNode(edge.getDestination());
						if(n.getLabel().equals(refNode.getLabel())) {
							outgoingEdges[outgoingEdgeCount] = edge.getLabel();
							outgoingEdgeCount++;
						} else if(d.getLabel().equals(refNode.getLabel())) {
							incomingEdges[incomingEdgeCount] = edge.getLabel();
							incomingEdgeCount++;
						}
						//edge.print(null);
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
		int i = 0;
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
			//System.out.println ("  - Scan the records\n");
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
					if(node.getDesc().distance(desc) <= distance) {
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
		incomingEdges = new String[nodesArray.length][nodesArray.length];
		outgoingEdges = new String[nodesArray.length][nodesArray.length];
		incomingEdgeCount = new int[nodesArray.length];
		outgoingEdgeCount = new int[nodesArray.length];
		if(i > 0) {
			status = OK;
			EID eid = new EID();
			EdgeHeapFile f1 = edges;

			Escan escan = null;
			if ( status == OK ) {
				//System.out.println ("  - Scan the records\n");
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
						for(int j = 0; j < i; j++) {
							Node sNode = f.getNode(edge.getSource());
							Node dNode = f.getNode(edge.getDestination());
							if(sNode.getLabel().equals(nodesArray[j].getLabel())) {
								outgoingEdges[j][outgoingEdgeCount[j]] = edge.getLabel();
								outgoingEdgeCount[j]++;
							} else if(dNode.getLabel().equals(nodesArray[j].getLabel())) {
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
				for(int j = 0;j < i; j++) {
					try {
						System.out.println("\n");
						nodesArray[j].print(jtype);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					System.out.println("\nIncoming Edges are:");
					for(int i1 = 0; i1 < incomingEdgeCount[j]; i1++) {
						System.out.print(incomingEdges[j][i1] + "	");
					}
					System.out.println("\nOutgoing Edges are:");
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
