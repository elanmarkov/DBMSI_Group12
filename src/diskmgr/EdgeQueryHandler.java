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
	
	private void sortEdges(Edge edgesArray[]) 
	{
		Edge temp;
		AttrType [] jtype = new AttrType[2];
		jtype[1] = new AttrType (AttrType.attrString);
		jtype[1] = new AttrType (AttrType.attrDesc);
		for (int i = 0; i < edgesArray.length; i++) 
		{
			for (int j = i + 1; j < edgesArray.length; j++) 
			{
				if (edgesArray[i].getLabel().compareTo(edgesArray[j].getLabel()) > 0) 
				{
					temp = edgesArray[i];
					edgesArray[i] = edgesArray[j];
					edgesArray[j] = temp;
				}
			}
		}
		for(int i = 0; i < edgesArray.length; i++) {
			try {
				edgesArray[i].print(jtype);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

       private void sortWeights(Edge edgesArray[]) 
       {
		Edge temp;
		AttrType [] jtype = new AttrType[2];
		jtype[1] = new AttrType (AttrType.attrString);
		jtype[1] = new AttrType (AttrType.attrDesc);
		for (int i = 0; i < edgesArray.length; i++) 
		{
			for (int j = i + 1; j < edgesArray.length; j++) 
			{
				if (edgesArray[i].getWeight()>edgesArray[j].getWeight()) 
				{
					temp = edgesArray[i];
					edgesArray[i] = edgesArray[j];
					edgesArray[j] = temp;
				}
			}
		}
		for(int i = 0; i < edgesArray.length; i++) {
			try {
				edgesArray[i].print(jtype);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
		public boolean edgeIndexTest0(String argv[])
	{

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
		String filename = nodes.getFileName();
		int edgeCount = 0, i = 0;
		try {
			edgeCount = edges.getEdgeCnt();
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		IndexFileScan iScan = null;
		Tuple t = null;
		try {
			iscan = new IndexScan(new IndexType(IndexType.B_Index), filename, "BTreeIndex", attrType, attrSize, 4, 4, projlist, null, 1, false);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		boolean done = false;
		while(!done) {
			try {
					t = iscan.get_next();
				}
			catch (IndexException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			catch (UnknownKeyTypeException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			 
			if(t == null) {
				done = true;
				break;
			}
			Edge edge = (Edge)t;
			try {
				edge.print(attrType);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	

		return true;
	}
	public boolean edgeIndexTest1(String argv[])
	{
		
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
		String filename = nodes.getFileName();
		EdgeHeapFile edgeheap = edges;
		NodeHeapFile nodeheap = nodes;
		int edgeCount = 0;
		try {
			edgeCount = edges.getEdgeCnt();
		} catch (InvalidSlotNumberException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InvalidTupleSizeException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (HFDiskMgrException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (HFBufMgrException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		int nodeCount = 0, i = 0;
		try {
			nodeCount = nodes.getNodeCnt();
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		Node[] nodes = new Node[edgeCount];
		//need to change test1.in to actual rel name
	    try {
	      iscan = new IndexScan(new IndexType(IndexType.B_Index), filename, "BTreeIndex", attrType, attrSize, 4, 4, projlist, null, 0, false);
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
				Edge edge = (Edge)t;
				NID nid = edge.getSource();
				try {
					nodes[i] = nodeheap.getNode(nid);
				} catch (InvalidSlotNumberException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InvalidTupleSizeException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (HFException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (HFDiskMgrException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (HFBufMgrException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				i++;
			}
			sortNodes(nodes);
		}
		return status;	
		
	}
	public boolean edgeIndexTest2(String argv[]){
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
		String filename = nodes.getFileName();
		EdgeHeapFile edgeheap = edges;
		NodeHeapFile nodeheap = nodes;
		int edgeCount = 0;
		try {
			edgeCount = edges.getEdgeCnt();
		} catch (InvalidSlotNumberException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InvalidTupleSizeException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (HFDiskMgrException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (HFBufMgrException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		int i = 0;
		Node[] nodesArray = new Node[edgeCount];
		//need to change test1.in to actual rel name
	    try {
	      iscan = new IndexScan(new IndexType(IndexType.B_Index), filename, "BTreeIndex", attrType, attrSize, 4, 4, projlist, null, 0, false);
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
				Edge edge = (Edge)t;
				NID nid = edge.getDestination();
				try {
					nodesArray[i] = nodeheap.getNode(nid);
				} catch (InvalidSlotNumberException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InvalidTupleSizeException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (HFException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (HFDiskMgrException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (HFBufMgrException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				i++;
			}
			sortNodes(nodesArray);
		}
		return status;	
	}
	public boolean edgeIndexTest3(String argv[]){
				return this.edgeIndexTest0(argv);
	}
	public  boolean edgeIndexTest4(String argv[]){
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
		String filename = nodes.getFileName();
		EdgeHeapFile edgeheap = edges;
		NodeHeapFile nodeheap = nodes;
		int edgeCount = 0;
		try {
			edgeCount = edges.getEdgeCnt();
		} catch (InvalidSlotNumberException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InvalidTupleSizeException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (HFDiskMgrException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (HFBufMgrException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		int i = 0;
		Node[] nodes = new Node[edgeCount];
		//need to change test1.in to actual rel name
	    try {
	      iscan = new IndexScan(new IndexType(IndexType.B_Index), filename, "BTreeIndex", attrType, attrSize, 4, 4, projlist, null, 3, false);
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
				Edge edge = (Edge)t;
				try {
					edge.print(attrType);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				i++;
			}
		}
		return status;	
	}
	public boolean edgeIndexTest5(String argv[]){
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
		String filename = nodes.getFileName();
		EdgeHeapFile edgeheap = edges;
		NodeHeapFile nodeheap = nodes;
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

		int edgeCount = 0;
		try {
			edgeCount = edges.getEdgeCnt();
		} catch (InvalidSlotNumberException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InvalidTupleSizeException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (HFDiskMgrException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (HFBufMgrException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		//need to change test1.in to actual rel name
	    try {
	      iscan = new IndexScan(new IndexType(IndexType.B_Index), filename, "BTreeIndex", attrType, attrSize, 4, 4, projlist, null, 3, false);
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
				Edge edge = (Edge)t;
				if(edge.getWeight()>=lowerbound || edge.getWeight()<=upperbound) {
					try {
						edge.print(attrType);
					} catch (IOException e) {
						// TODO Auto-generated catch block
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

			NID source,destination;
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
	public boolean edgeHeapTest1(String argv[]){
		boolean status = OK;
		int i = 0;
		EID eid = new EID();
		NID nid = new NID();
		EdgeHeapFile f        = edges;
		NodeHeapFile nodeheap = nodes;
		int edgeCount = 0;
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
		} catch (Exception e) {
			e.printStackTrace();
		}
		Edge[] edgesArray = new Edge[edgeCount];

		AttrType [] jtype = new AttrType[1];
		jtype[0] = new AttrType (AttrType.attrString);
		jtype[1] = new AttrType (AttrType.attrDesc);
		
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
					nid  = edge.getSource();	
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
			sortEdges(edgesArray);
		}

		return status;
	}
	public boolean edgeHeapTest2(String argv[]){
		boolean status = OK;
		int i = 0;
		EID eid = new EID();
		NID nid = new NID();
		EdgeHeapFile f        = edges;
		NodeHeapFile nodeHeapFile = nodes;
		int edgeCount = 0;
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
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		Edge[] edgesArray = new Edge[edgeCount];

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
					nid  = edge.getDestination();
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
			sortEdges(edgesArray);
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
		} catch(Exception e) {
			e.printStackTrace();
		}
		Edge[] edgesArray = new Edge[edgeCount];

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
					edgesArray[i] = edge;
					i++;
				}
				catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
			}
			sortEdges(edgesArray);
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
		catch (Exception e) {
			e.printStackTrace();
		}
		Edge[] edgesArray = new Edge[edgeCount];

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
					edgesArray[i] = edge;
					i++;
				}
				catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}
			}
			sortWeights(edgesArray);
		}

		return status;

	}
	public boolean edgeHeapTest5(String argv[]){
                
                boolean status = OK;
		int i = 0;
		EID eid = new EID();
		EdgeHeapFile f = edges;
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
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		Edge[] edgesArray = new Edge[edgeCount];

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


       public boolean edgeHeapTest6(String argv[]){
                
                boolean status = OK;
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
		
			Edge edge = new Edge();
			i=0;
			try {
				edge = scan.getNext(eid);
			} catch (InvalidTupleSizeException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
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
	        scan.closescan();

		
		

		return status;

      }

}
