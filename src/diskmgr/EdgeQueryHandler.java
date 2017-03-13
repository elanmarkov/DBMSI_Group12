package diskmgr;

import java.io.*;
import java.util.Objects;
import java.util.Scanner;

import bufmgr.*;
import global.*;
import btree.*;
import zindex.*;
import heap.*;
import iterator.*;
import index.*;

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

	private void sortNodes(Node nodesArray[]) 
	{
		Node temp;
		AttrType [] jtype = new AttrType[2];
		jtype[1] = new AttrType (AttrType.attrString);
		jtype[1] = new AttrType (AttrType.attrDesc);
		for (int i = 0; i < nodesArray.length; i++) 
		{
			for (int j = i + 1; j < nodesArray.length; j++) 
			{
				if (nodesArray[i].getLabel().compareTo(nodesArray[j].getLabel()) > 0) 
				{
					temp = nodesArray[i];
					nodesArray[i] = nodesArray[j];
					nodesArray[j] = temp;
				}
			}
		}
		for(int i = 0; i < nodesArray.length; i++) {
			try {
				nodesArray[i].print(jtype);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private void sortEdges(Edge edgesArray[],int sortParameter) 
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
		for(int i = 0; i < edgesArray.length; i++) {
			try {
				print(edgesArray[i],nodes);
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

	public boolean edgeIndexTest0(String argv[])
	{

		boolean status = OK;
		AttrType[] attrType = new AttrType[6];
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
		attrSize[2] = 4;
		attrSize[3] = 4;
		IndexScan iscan = null;
		String filename = edges.getFileName();
		Tuple t = null;
		try {
			iscan = new IndexScan(new IndexType(IndexType.B_Index), filename, "GraphDB0EDGELABEL", attrType, attrSize, 6, 6, projlist, null, 1, false);
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
		}
		return status;
	}
	public boolean edgeIndexTest1(String argv[])
	{
		boolean status = OK;
		AttrType[] attrType = new AttrType[6];
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
		attrSize[2] = 4;
		attrSize[3] = 4;
		IndexScan iscan = null;
		String filename = edges.getFileName();
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
			iscan = new IndexScan(new IndexType(IndexType.B_Index), filename, "GraphDB0EDGELABEL", attrType, attrSize, 6, 6, projlist, null, 1, false);
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
		}
		return status;	
	}
	public boolean edgeIndexTest2(String argv[]){
		boolean status = OK;
		AttrType[] attrType = new AttrType[6];
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
		attrSize[2] = 4;
		attrSize[3] = 4;
		IndexScan iscan = null;
		String filename = edges.getFileName();
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
			iscan = new IndexScan(new IndexType(IndexType.B_Index), filename, "GraphDB0EDGELABEL", attrType, attrSize, 6, 6, projlist, null, 1, false);
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
		}
		return status;	
	}
	public boolean edgeIndexTest3(String argv[]){
		boolean status = OK;
		AttrType[] attrType = new AttrType[6];
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
		attrSize[2] = 4;
		attrSize[3] = 4;
		IndexScan iscan = null;
		String filename = edges.getFileName();
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
			iscan = new IndexScan(new IndexType(IndexType.B_Index), filename, "GraphDB0EDGELABEL", attrType, attrSize, 6, 6, projlist, null, 1, false);
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
		}
		return status;	
	}
	public  boolean edgeIndexTest4(String argv[]){
		boolean status = OK;
		AttrType[] attrType = new AttrType[6];
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
		attrSize[2] = 4;
		attrSize[3] = 4;
		IndexScan iscan = null;
		String filename = edges.getFileName();
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
			iscan = new IndexScan(new IndexType(IndexType.B_Index), filename, "GraphDB0EDGEWEIGHT", attrType, attrSize, 6, 6, projlist, null, 6, false);
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
			sortWeights(edges);
		}
		return status;		
	}
	public boolean edgeIndexTest5(String argv[]){
		boolean status = OK;
		AttrType[] attrType = new AttrType[6];
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
		attrSize[2] = 4;
		attrSize[3] = 4;
		IndexScan iscan = null;
		String filename = edges.getFileName();
		Tuple t = null;
		int edgeCount = 0;
		int  lowerbound = Integer.parseInt(argv[4]), upperbound = Integer.parseInt(argv[5]);
		try {
			edgeCount = edges.getEdgeCnt();
		} catch (Exception e1) {
			status = FAIL;
			e1.printStackTrace();
		}
		int i = 0;

		Edge[] edges = new Edge[edgeCount];
		try {
			iscan = new IndexScan(new IndexType(IndexType.B_Index), filename, "GraphDB0EDGEWEIGHT", attrType, attrSize, 6, 6, projlist, null, 6, false);
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
		}
		return status;	
	}
	public boolean edgeIndexTest6(String argv[]) throws InvalidTupleSizeException{


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
		IndexScan scan = null;
		String filename = nodes.getFileName();
		int i = 0;
		EID eid = new EID();
		EdgeHeapFile f = edges;
		int edgeCount  = 0;

		try {
			edgeCount = edges.getEdgeCnt();
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
		}catch (Exception e) {
			e.printStackTrace();
		}

		RID[][] edgesArray = new RID[edgeCount][3];
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



		if ( status == OK ) {
			try {
				scan = new IndexScan(new IndexType(IndexType.B_Index), filename, "BTreeIndex", attrType, attrSize, 4, 4, projlist, null, 3, false);
			}
			catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}
			Edge edge = new Edge();
			i=0;
			try {
				try {
					edge = (Edge)scan.get_next();
				} catch (IndexException | UnknownKeyTypeException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch (IOException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
			while(edge!=null){

				try{
					edgesArray[i][0] = eid;
					edgesArray[i][1] = edge.getSource();
					edgesArray[i][2] = edge.getDestination();
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
					if(Objects.equals(edgesArray[i][1],edgesArray[j][2]) || Objects.equals(edgesArray[i][2],edgesArray[j][1])){

						Edge edge1 = null;
						try {
							edge1 = f.getEdge((EID)edgesArray[i][0]);
							Edge edge2 = f.getEdge((EID)edgesArray[j][0]);
						} catch (InvalidSlotNumberException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						} catch (InvalidTupleSizeException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						} catch (HFException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						} catch (HFDiskMgrException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						} catch (HFBufMgrException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						} catch (Exception e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}


						try {
							edge1.print(jtype);

						} 
						catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}			    

					}
				}						
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
					print(edge,nodes);
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
					edgesArray[i] = f.getEdge(eid);
					i++;
				}
				catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
			}
			scan.closescan();
			sortEdges(edgesArray,0);
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
					edgesArray[i] = f.getEdge(eid);
					i++;
				}
				catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
			}
			scan.closescan();
			sortEdges(edgesArray,1);
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
		}
		return status;
	}


	public boolean edgeHeapTest6(String argv[]){
		boolean status = OK;
		int i = 0;
		EID eid = new EID();
		EdgeHeapFile f = edges;
		int edgeCount  = 0;
		Scanner  sc = new Scanner(System.in);
		sc.nextLine();

		try {
			edgeCount = edges.getEdgeCnt();
		} catch (Exception e1) {
			e1.printStackTrace();
		}

		RID[][] edgesArray = new RID[edgeCount][3];
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
		sc.nextLine();
		if ( status == OK ) {
			Edge edge = new Edge();
			i=0;
			try {
				edge = scan.getNext(eid);
			} catch (Exception e2) {
				status = FAIL;
				e2.printStackTrace();
			}
			while(edge!=null){
				try{
					edgesArray[i][0] = eid;
					edgesArray[i][1] = edge.getSource();
					edgesArray[i][2] = edge.getDestination();
				}
				catch(Exception e){
					status = FAIL;
					System.err.println(""+e);
				}
				i++; 
			}
			i=0;
			while(i<edgeCount){
				int j=i+1;
				while(j<edgeCount){
					if(Objects.equals(edgesArray[i][1],edgesArray[j][2]) || Objects.equals(edgesArray[i][2],edgesArray[j][1])){

						Edge edge1 = null,edge2 = null;
						try {
							edge1 = f.getEdge((EID)edgesArray[i][0]);
							edge2 = f.getEdge((EID)edgesArray[j][0]);
						} catch (Exception e1) {
							status = FAIL;
							e1.printStackTrace();
						}
						try {
							System.out.println("[ "+ edge1.getLabel() + ", "+ edge2.getLabel() + " ]") ;
						} 
						catch (Exception e) {
							status = FAIL;
							e.printStackTrace();
						}			    

					}
				}						
			}

		}
		sc.nextLine();
		scan.closescan();
		return status;
	}
}
