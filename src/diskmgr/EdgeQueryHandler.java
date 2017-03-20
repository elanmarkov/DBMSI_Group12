/*Edge Queries program by Harshdeep Sandhu adopted from Task 14 and implemented for Edges.
 * */package diskmgr;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import btree.BTreeFile;
import global.AttrType;
import global.EID;
import global.IndexType;
import global.NID;
import global.SystemDefs;
import heap.Edge;
import heap.EdgeHeapFile;
import heap.Escan;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Node;
import heap.NodeHeapFile;
import heap.Tuple;
import index.IndexException;
import index.IndexScan;
import index.UnknownIndexTypeException;
import iterator.FldSpec;
import iterator.RelSpec;
import zindex.ZCurve;

public class EdgeQueryHandler {
	private final static boolean OK = true;
	private final static boolean FAIL = false;
	NodeHeapFile nodes;
	EdgeHeapFile edges;
	BTreeFile nodeLabels;
	ZCurve nodeDesc;
	BTreeFile edgeLabels;
	BTreeFile edgeWeights;
	graphDB db;
	public EdgeQueryHandler(NodeHeapFile nodes, EdgeHeapFile edges, BTreeFile nodeLabels, 
			ZCurve nodeDesc, BTreeFile edgeLabels, BTreeFile edgeWeights, graphDB db) {

		this.nodes = nodes;
		this.edges = edges;
		this.nodeLabels = nodeLabels;
		this.nodeDesc = nodeDesc;
		this.edgeLabels = edgeLabels;
		this.edgeWeights = edgeWeights;
		this.db = db;
	}
	public static void print(Edge e,NodeHeapFile nodes) throws IOException
	{
		Node source = null,destination = null;
		try {
			source = nodes.getNode(e.getSource());
			destination = nodes.getNode(e.getDestination());
		} catch(Exception e1) {
			e1.printStackTrace();
		}
		System.out.print("[");
		System.out.println(" source label : "+source.getLabel());
		System.out.println(" destination label : "+destination.getLabel());
		System.out.print(" edge label : "+e.getLabel());
		System.out.print(" weight : "+e.getWeight());
		System.out.println(" ]");
	}

	private void sortEdgesold(Edge edgesArray[],int sortParameter) 
	{
		Edge temp;
		Node sourceNode1 = null;
		Node sourceNode2 = null;
		Node destinationNode1 = null;
		Node destinationNode2 = null;
		for (int i = 0; i < edgesArray.length; i++) 
		{
			for (int j = i + 1; j < edgesArray.length; j++) 
			{
				if(sortParameter == 0) {
					try{
						sourceNode1 = nodes.getNode(edgesArray[i].getSource());
						sourceNode2 = nodes.getNode(edgesArray[j].getSource());
					} catch(Exception e) {
						e.printStackTrace();
					}
					if (sourceNode1.getLabel().compareTo(sourceNode2.getLabel()) > 0) 
					{
						temp = edgesArray[i];
						edgesArray[i] = edgesArray[j];
						edgesArray[j] = temp;
					}
				} else if(sortParameter == 1) {
					System.out.println("EdgeQueryHandler.sortEdges() i: "+i +" j : "+j);
					try{
						destinationNode1 = nodes.getNode(edgesArray[i].getDestination());
						destinationNode2 = nodes.getNode(edgesArray[j].getDestination());
					} catch(Exception e) {
						e.printStackTrace();
					}
					if (destinationNode1.getLabel().compareTo(destinationNode2.getLabel()) > 0) 
					{
						temp = edgesArray[i];
						edgesArray[i] = edgesArray[j];
						edgesArray[j] = temp;
					}
				} else if(sortParameter == 2) {
					if (edgesArray[i].getLabel().compareTo(edgesArray[j].getLabel()) > 0) 
					{
						temp = edgesArray[i];
						edgesArray[i] = edgesArray[j];
						edgesArray[j] = temp;
					}
				}
			}
		}
		System.out.println("EdgeQueryHandler.sortEdges() done sorting ");
		for(int i = 0; i < edgesArray.length; i++) {
			try {
				print(edgesArray[i],nodes);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("EdgeQueryHandler.sortEdges() done printing");
	}

	private void sortEdges(Edge edgesArray[],int sortParameter) 
	{
		Edge temp;
		Node sourceNode1 = null;
		Node sourceNode2 = null;
		Node destinationNode1 = null;
		Node destinationNode2 = null;
		Map<String, Edge> map = new TreeMap<String, Edge>();
		for (int i = 0; i < edgesArray.length; i++) 
		{
			String key = null;
			switch (sortParameter) {
			case 0:
				try {
					sourceNode1 = nodes.getNode(edgesArray[i].getSource());
					key = sourceNode1.getLabel();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				break;
			case 1:
				try {
					Node destNode = nodes.getNode(edgesArray[i].getDestination());
					key = destNode.getLabel();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				break;
			default:
				key = edgesArray[i].getLabel();
				break;
			}
			map.put(key, edgesArray[i]);
		}
		
		for (Map.Entry<String, Edge> entry : map.entrySet()) {
            try {
				print(entry.getValue(),nodes);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
	}
	
	private void sortWeights(Edge edgesArray[]) 
	{
		Edge temp;
		for (int i = 0; i < edgesArray.length; i++) 
		{
			for (int j = i + 1; j < edgesArray.length; j++) 
			{
				if (edgesArray[i].getWeight() >= edgesArray[j].getWeight()) 
				{
					temp = edgesArray[i];
					edgesArray[i] = edgesArray[j];
					edgesArray[j] = temp;
				}
			}
		}
		for(int i = 0; i < edgesArray.length; i++) {
			try {
				print(edgesArray[i],nodes);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
    private IndexScan runIndexScan(int scanon) {
    	
		AttrType[] attrType = new AttrType[6];				//Initiating the Index Scan......
		attrType[0] = new AttrType(AttrType.attrString);
		attrType[1] = new AttrType(AttrType.attrInteger);
		attrType[2] = new AttrType(AttrType.attrInteger);
		attrType[3] = new AttrType(AttrType.attrInteger);
		attrType[4] = new AttrType(AttrType.attrInteger);
		attrType[5] = new AttrType(AttrType.attrInteger);
		FldSpec[] projlist = new FldSpec[6];
		RelSpec rel = new RelSpec(RelSpec.outer); 
		projlist[0] = new FldSpec(rel, 1);
		projlist[1] = new FldSpec(rel, 2);
		projlist[2] = new FldSpec(rel, 3);
		projlist[3] = new FldSpec(rel, 4);
		projlist[4] = new FldSpec(rel, 5);
		projlist[5] = new FldSpec(rel, 6);
		short[] attrSize = new short[6];
		attrSize[0] = Tuple.LABEL_MAX_LENGTH;
		attrSize[1] = 4;
		attrSize[2] = 4;
		attrSize[3] = 4;
		attrSize[4] = 4;
		attrSize[5] = 4;

		IndexScan iscan = null;
		String filename = edges.getFileName();
		try {
			iscan = new IndexScan(new IndexType(IndexType.B_Index), filename, "GraphDB0EDGELABEL", attrType, attrSize, 6, 6, projlist, null, scanon, false);
		} catch (IndexException | InvalidTypeException | InvalidTupleSizeException | UnknownIndexTypeException
				| IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return iscan;
		
    }
	public boolean edgeIndexTest0(String argv[])
	{
		boolean status = OK;

		IndexScan iscan = null;
		
		Tuple t = null;
		try {
			iscan = this.runIndexScan(1);//IndexScan Initiated
		}
		catch(Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		boolean done = false;

		if(status == OK) {
			while(!done) {
				try {
					t = iscan.getNextEdge();
				}
				catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}

				if(t == null) {
					done = true;
					break;
				}
				Edge edge = new Edge(t);
				try {
					print(edge,nodes);
				} catch (IOException e) {
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
	public boolean edgeIndexTest1(String argv[])
	{
		boolean status = OK;
		IndexScan iscan = null;
		
		Tuple t = null;
		int edgeCount = 0;
		try {
			edgeCount = edges.getEdgeCnt();
		} catch (Exception e1) {
			status = FAIL;
			e1.printStackTrace();
		}
		int i = 0;

		Edge[] edges = new Edge[edgeCount];
		try {
			iscan = runIndexScan(1);//IndexScan Initiated
		}
		catch(Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		boolean done = false;
		if(status == OK) {
			while(!done) {
				t = new Tuple();
				try {
					t = iscan.getNextEdge();
				} catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
				if(t == null) {
					done = true;
					break;
				}
				Edge edge = new Edge(t);
				edges[i] = edge;
				i++;
			}
			sortEdges(edges,0);
			try {
				iscan.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return status;	
	}
	public boolean edgeIndexTest2(String argv[]){
		boolean status = OK;
		IndexScan iscan = null;
		Tuple t = null;
		int edgeCount = 0;
		try {
			edgeCount = edges.getEdgeCnt();
		} catch (Exception e1) {
			status = FAIL;
			e1.printStackTrace();
		}
		int i = 0;

		Edge[] edges = new Edge[edgeCount];
		try {
			iscan = runIndexScan(1);//IndexScan Initiated
		}
		catch(Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		boolean done = false;
		if(status == OK) {
			while(!done) {
				t = new Tuple();
				try {
					t = iscan.getNextEdge();
				} catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
				if(t == null) {
					done = true;
					break;
				}
				Edge edge = new Edge(t);
				edges[i] = edge;
				i++;
			}
			sortEdges(edges,1);
			try {
				iscan.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return status;	
	}
	public boolean edgeIndexTest3(String argv[]){
		boolean status = OK;						//Initiating the Index Scan......
		IndexScan iscan = null;
		Tuple t = null;
		int edgeCount = 0;
		try {
			edgeCount = edges.getEdgeCnt();
		} catch (Exception e1) {
			status = FAIL;
			e1.printStackTrace();
		}
		int i = 0;

		Edge[] edges = new Edge[edgeCount];
		try {
			iscan = runIndexScan(1);//IndexScan Initiated
		}
		catch(Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		boolean done = false;
		if(status == OK) {
			while(!done) {
				t = new Tuple();
				try {
					t = iscan.getNextEdge();
				} catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
				if(t == null) {
					done = true;
					break;
				}
				Edge edge = new Edge(t);
				edges[i] = edge;
				i++;
			}
			sortEdges(edges,2);
			try {
				iscan.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return status;	
	}
	public  boolean edgeIndexTest4(String argv[]){
		boolean status = OK;						//Initiating the Index Scan......
		IndexScan iscan = null;
		Tuple t = null;
		int edgeCount = 0;
		try {
			edgeCount = edges.getEdgeCnt();
		} catch (Exception e1) {
			status = FAIL;
			e1.printStackTrace();
		}
		int i = 0;

		Edge[] edges = new Edge[edgeCount];
		try {
			iscan = runIndexScan(6);//IndexScan Initiated
		}
		catch(Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		boolean done = false;
		if(status == OK) {
			while(!done) {
				t = new Tuple();
				try {
					t = iscan.getNextEdge();	//Getting the next Edge
				} catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
				if(t == null) {
					done = true;
					break;
				}
				Edge edge = new Edge(t);
				edges[i] = edge;
				i++;
			}
			sortWeights(edges);				//Passing the edges with weights to the sortWeights Function.
			try {
				iscan.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return status;		
	}
	public boolean edgeIndexTest5(String argv[]){
		boolean status = OK;
		IndexScan iscan = null;
		Tuple t = null;
		int  lowerbound = Integer.parseInt(argv[4]), upperbound = Integer.parseInt(argv[5]);
	
		try {
			iscan = runIndexScan(6);//IndexScan Initiated
		}
		catch(Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		boolean done = false;
		if(status == OK) {
			while(!done) {
				t = new Tuple();
				try {
					t = iscan.getNextEdge();
				} catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
				if(t == null) {
					done = true;
					break;
				}
				Edge edge = new Edge(t);
				if(edge.getWeight() >= lowerbound && edge.getWeight() <= upperbound){
					try{
						print(edge,nodes);					
					} catch(Exception e) {
						status = FAIL;
						e.printStackTrace();
					}
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
	public boolean edgeIndexTest6(String argv[]) throws InvalidTupleSizeException{


		boolean status = OK;
		IndexScan iscan = null;
		int i = 0;
		int edgeCount  = 0;

		try {
			edgeCount = edges.getEdgeCnt();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		NID[][] nodesArray = new NID[edgeCount][2];




		if ( status == OK ) {
			try {
				iscan = runIndexScan(1);//IndexScan Initiated
			}
			catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}
			Tuple t = null;
			String[] EdgeLabels = new String[edgeCount];
			i=0;

			try {
				t = iscan.getNextEdge();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			while(t!=null){
				Edge edge = new Edge(t);
				try{
					EdgeLabels[i] = edge.getLabel();
				
					
					nodesArray[i][0] = edge.getSource();
					nodesArray[i][1] = edge.getDestination();
				}
				catch(Exception e){
					System.err.println(""+e);
				}
				i++; 
       
				try {
					t = iscan.getNextEdge();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			
			}
			edgeCount=i;
			i=0;
			while(i<edgeCount){
				int j=i+1;
				while(j<edgeCount){
					if(nodesArray[i][0].equals(nodesArray[j][1]) || nodesArray[i][1].equals(nodesArray[j][0])
							|| nodesArray[i][1].equals(nodesArray[j][1]) || nodesArray[i][0].equals(nodesArray[j][0])){    //Check whether the two edges are incident edges.


						try {

							System.out.println("["+EdgeLabels[i]+","+EdgeLabels[j]+"]");
						} catch (Exception e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						} 



					}			    
					j++;
				}
				i++;
			}	
			try {
				iscan.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}



		return status;


	}
	public boolean edgeHeapTest0(String argv[]){
		boolean status = OK;
		EID eid = new EID();
		EdgeHeapFile f = edges;
		Escan scan = null;
		if ( status == OK ) {
			System.out.println ("  - Scan the records\n");
			try {
				scan = f.openScan();		//Initiating the Escan
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
					print(edge,nodes);	//printing the edge.
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
	public boolean edgeHeapTest1(String argv[]){
		boolean status = OK;
		int i = 0;
		EID eid = new EID();
		EdgeHeapFile f = edges;
		int edgeCount = 0;
		try {
			edgeCount = edges.getEdgeCnt();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		Edge[] edgesArray = new Edge[edgeCount];

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
					if (edge == null) 
					{
						done = true;
						break;
					}
					edgesArray[i] = f.getEdge(eid);	// Populating the edgesArray Array with edges.
					i++;
				}
				catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
			}
			scan.closescan();
			sortEdges(edgesArray,0);	//Passing the Array of edges for sorting.
		}
		return status;
	}
	public boolean edgeHeapTest2(String argv[]){
		boolean status = OK;
		int i = 0;
		EID eid = new EID();
		EdgeHeapFile f = edges;
		int edgeCount = 0;
		try {
			edgeCount = edges.getEdgeCnt();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		Edge[] edgesArray = new Edge[edgeCount];
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
					edgesArray[i] = f.getEdge(eid); //Populating the edgesArray
					i++;
				}
				catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
			}
			scan.closescan();
			sortEdges(edgesArray,1);	//Edges sent for sorting.
			
		}
		return status;
	}
	public boolean edgeHeapTest3(String argv[]){
		boolean status = OK;
		int i = 0;
		EID eid = new EID();
		EdgeHeapFile f = edges;
		int edgeCount  = 0;
		try {
			edgeCount = edges.getEdgeCnt();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		Edge[] edgesArray = new Edge[edgeCount];
		Escan scan = null;
		if ( status == OK ) {	

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
					edgesArray[i] = edge;
					i++;
				}
				catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
			}
			scan.closescan();
			sortEdges(edgesArray,2);
		}
		return status;
	}
	public boolean edgeHeapTest4(String argv[]){
		boolean status = OK;
		int i = 0;
		EID eid = new EID();
		EdgeHeapFile f = edges;
		int edgeCount  = 0;
		try {
			edgeCount = edges.getEdgeCnt();
		} catch (Exception e) {
			e.printStackTrace();
		}
		Edge[] edgesArray = new Edge[edgeCount];
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
					edgesArray[i] = edge;
					i++;
				}
				catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
			}
			scan.closescan();
			sortWeights(edgesArray);
		}
		return status;
	}
	public boolean edgeHeapTest5(String argv[]){
		boolean status = OK;
		EID eid = new EID();
		EdgeHeapFile f = edges;
		int  lowerbound = Integer.parseInt(argv[4]), upperbound = Integer.parseInt(argv[5]);
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
					if(edge.getWeight() >= lowerbound && edge.getWeight() <= upperbound){
						print(edge,nodes);						
					}
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


	public boolean edgeHeapTest6(String argv[]){
		boolean status = OK;
		int i = 0;
		EID eid = new EID();
		EdgeHeapFile f = edges;
		int edgeCount  = 0;


		try {
			edgeCount = edges.getEdgeCnt();
		} catch (Exception e1) {
			e1.printStackTrace();
		}

		NID[][] nodesArray = new NID[edgeCount][2];
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
			String[] edgeLabel = new String[edgeCount];
			i=0;
			try {
				edge = scan.getNext(eid);
			} catch (Exception e2) {
				status = FAIL;
				e2.printStackTrace();
			}
			while(edge!=null && status){
				try{
					edgeLabel[i] = edge.getLabel();
					nodesArray[i][0] = edge.getSource();
					nodesArray[i][1] = edge.getDestination();
				}
				catch(Exception e){
					status = FAIL;
					System.err.println(""+e);
				}
				i++; 
				try {
					edge = scan.getNext(eid);
				} catch (InvalidTupleSizeException | IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			i=0;
			scan.closescan();

			while(i<edgeCount){
				int j=i+1;
				while(j<edgeCount){
					if(nodesArray[i][0].equals(nodesArray[j][1]) || nodesArray[i][1].equals(nodesArray[j][0])
							|| nodesArray[i][1].equals(nodesArray[j][1]) || nodesArray[i][0].equals(nodesArray[j][0])){


						try {

							System.out.println("[ "+ edgeLabel[i] + ", "+ edgeLabel[j] + " ]") ;
						} 
						catch (Exception e) {
							status = FAIL;
							e.printStackTrace();
						}			    

					}
					j++;
				}		
				i++;
			}

		}


		return status;
	}
}
