package diskmgr;

import java.io.IOException;
import java.util.ArrayList;

import btree.BTreeFile;
import bufmgr.PageNotReadException;
import global.AttrOperator;
import global.AttrType;
import global.Descriptor;
import global.ExpType;
import global.IndexType;
import global.SystemDefs;
import heap.EdgeHeapFile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Node;
import heap.NodeHeapFile;
import heap.Tuple;
import index.IndexException;
import index.IndexScan;
import index.UnknownIndexTypeException;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.Iterator;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.NestedIndexLoopJoin;
import iterator.NestedLoopException;
import iterator.PredEvalException;
import iterator.RelSpec;
import iterator.SortException;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;
import zindex.ZCurve;

public class PathExpression{
	private int amountOfMemoryForEachJoin = 10;
	private final static boolean OK = true;
	private final static boolean FAIL = false;
	NodeHeapFile nodes;
	EdgeHeapFile edges;
	BTreeFile nodeLabels;
	ZCurve nodeDesc;
	BTreeFile edgeLabels;
	BTreeFile edgeWeights;
	graphDB db;
	private Iterator[] nljArray ;
	Iterator am = null;
	

	public PathExpression(NodeHeapFile nodes, EdgeHeapFile edges, BTreeFile nodeLabels, ZCurve nodeDesc,
			BTreeFile edgeLabels, BTreeFile edgeWeights, graphDB db) {
		this.nodes = nodes;
		this.edges = edges;
		this.nodeLabels = nodeLabels;
		this.nodeDesc = nodeDesc;
		this.edgeLabels = edgeLabels;
		this.edgeWeights = edgeWeights;
		this.db = db;
	}

	private void Query2_CondExpr(CondExpr[] expr, CondExpr[] expr2) {

		expr[0].next = null;
		expr[0].op = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);
		expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
		expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 7);
		expr[1] = null;

		expr2[0].next = null;
		expr2[0].op = new AttrOperator(AttrOperator.aopEQ);
		expr2[0].type1 = new AttrType(AttrType.attrSymbol);
		expr2[0].type2 = new AttrType(AttrType.attrSymbol);
		expr2[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 5);
		expr2[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 7);

		expr2[1] = null;
	}

	public void query() {
		System.out.print("**********************Query2 strating *********************\n");
		System.out.println("PathExpression.query() buffers : "+SystemDefs.JavabaseBM.getNumBuffers()+" "
				+ "unpinned : "+SystemDefs.JavabaseBM.getNumUnpinnedBuffers());
		boolean status = OK;
		
		IndexType b_index = new IndexType(IndexType.B_Index);
		CondExpr[] outFilter = new CondExpr[3];
		outFilter[0] = new CondExpr();
		outFilter[1] = new CondExpr();
		outFilter[2] = new CondExpr();

		CondExpr[] outFilter2 = new CondExpr[2];
		outFilter2[0] = new CondExpr();
		outFilter2[1] = new CondExpr();

		Query2_CondExpr(outFilter, outFilter2);
		Tuple t =null;

		AttrType[] Ntypes = { new AttrType(AttrType.attrString), new AttrType(AttrType.attrDesc), };

		AttrType[] Ntypes2 = { new AttrType(AttrType.attrString), new AttrType(AttrType.attrDesc), };

		short[] Nsizes = new short[1];
		Nsizes[0] = Tuple.LABEL_MAX_LENGTH;
		AttrType[] Etypes = { new AttrType(AttrType.attrString), new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrString), new AttrType(AttrType.attrString), };

		short[] Esizes = new short[3];
		Esizes[0] = Tuple.LABEL_MAX_LENGTH;
		Esizes[1] = 4;
		Esizes[2] = 4;
		AttrType[] Ntypes3 = { new AttrType(AttrType.attrString), new AttrType(AttrType.attrDesc), };

		short[] Bsizes = new short[2];
		Bsizes[0] = 30;
		Bsizes[1] = 20;
		AttrType[] Jtypes = { new AttrType(AttrType.attrString), 
				new AttrType(AttrType.attrString),
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrString), 
				new AttrType(AttrType.attrString), };

		short[] Jsizes = new short[4];
		Jsizes[0] = Tuple.LABEL_MAX_LENGTH;
		Jsizes[1] = Tuple.LABEL_MAX_LENGTH;
		Jsizes[2] = 4;
		Jsizes[3] = 4;
		AttrType[] JJtype = { new AttrType(AttrType.attrString), };

		short[] JJsize = new short[1];
		JJsize[0] = 30;
		FldSpec[] proj1 = { new FldSpec(new RelSpec(RelSpec.outer), 1), 
				new FldSpec(new RelSpec(RelSpec.innerRel), 1),
				new FldSpec(new RelSpec(RelSpec.innerRel), 6), 
				new FldSpec(new RelSpec(RelSpec.innerRel), 7),
				new FldSpec(new RelSpec(RelSpec.innerRel), 8) }; // S.sname,
																	// R.bid

		FldSpec[] Sprojection = { new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.outer), 2),
				// new FldSpec(new RelSpec(RelSpec.outer), 3),
				// new FldSpec(new RelSpec(RelSpec.outer), 4)
		};

		CondExpr[] selects = new CondExpr[1];
		selects[0] = null;

		iterator.Iterator am = null;

		AttrType[] attrType = new AttrType[2];
		attrType[0] = new AttrType(AttrType.attrString);
		attrType[1] = new AttrType(AttrType.attrDesc);
		FldSpec[] projlist = new FldSpec[2];
		RelSpec rel = new RelSpec(RelSpec.outer);
		projlist[0] = new FldSpec(rel, 1);
		projlist[1] = new FldSpec(rel, 2);
		short[] attrSize = new short[1];
		attrSize[0] = Node.LABEL_MAX_LENGTH;
		CondExpr[] indexExpr = new CondExpr[2];
		indexExpr[0] = new CondExpr();
		indexExpr[0].op = new AttrOperator(AttrOperator.aopEQ);
		indexExpr[0].type1 = new AttrType(AttrType.attrSymbol);
		indexExpr[0].type2 = new AttrType(AttrType.attrString);
		indexExpr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 7);
		indexExpr[0].operand2.string = "373";
		indexExpr[0].next = null;
	    indexExpr[1] = null;
	    
		System.out.println("PathExpression.query() ");
		try {
			am = getIndexIteratorOnNodeLabel(attrType, projlist, attrSize, null);
			System.out.println("PathExpression.query() after btree buffers : "+SystemDefs.JavabaseBM.getNumBuffers()+" "
					+ "unpinned : "+SystemDefs.JavabaseBM.getNumUnpinnedBuffers());
		}
		
		catch (Exception e) {
			System.err.println("*** Error creating scan for Index scan");
			System.err.println("" + e);
			Runtime.getRuntime().exit(1);
		}

		/*FileScan fam=null;
		
		try {
			fam = new FileScan(nodes.getFileName(),attrType,
					attrSize, (short) 2, (short)2, projlist, null);
			System.out.println("PathExpression.query() after btree buffers : "+SystemDefs.JavabaseBM.getNumBuffers()+" "
					+ "unpinned : "+SystemDefs.JavabaseBM.getNumUnpinnedBuffers());
		}
		
		catch (Exception e) {
			System.err.println("*** Error creating scan for Index scan");
			System.err.println("" + e);
			Runtime.getRuntime().exit(1);
		}*/
		//NestedLoopsJoins nlj = null;
		FldSpec[] indexProjList = new FldSpec[8];
		RelSpec relation = new RelSpec(RelSpec.outer);
		indexProjList[0] = new FldSpec(relation, 1);
		indexProjList[1] = new FldSpec(relation, 2);
		indexProjList[2] = new FldSpec(relation, 3);
		indexProjList[3] = new FldSpec(relation, 4);
		indexProjList[4] = new FldSpec(relation, 5);
		indexProjList[5] = new FldSpec(relation, 6);
		indexProjList[6] = new FldSpec(relation, 7);
		indexProjList[7] = new FldSpec(relation, 8);
		NestedIndexLoopJoin nlj = null;
		try {
			nlj = getNestedIndexJoinOnSrouceLabel(Ntypes,2, Nsizes, indexExpr, Etypes,8, Esizes, indexProjList, am,
					outFilter, proj1,1);
			System.out.println("PathExpression.query() after nlj buffers : "+SystemDefs.JavabaseBM.getNumBuffers()+" "
					+ "unpinned : "+SystemDefs.JavabaseBM.getNumUnpinnedBuffers());
		} catch (Exception e) {
			System.err.println("*** Error preparing for nested_loop_join");
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		NestedIndexLoopJoin nlj2 = null;
		try {
			nlj2 = new NestedIndexLoopJoin(Jtypes, 5, Jsizes, Etypes, 8, Esizes, 10, nlj, "GraphDBEDGESRCLABEL", new IndexType(IndexType.B_Index),
					edges.getFileName(), indexProjList, 7, outFilter2, null, proj1, 5,5);
			/*nlj2 = new NestedLoopsJoins(Jtypes, 5, Jsizes, Etypes, 8, Esizes, 10, nlj, edges.getFileName(), outFilter2,
					null, proj1, 5);*/
		} catch (Exception e) {
			System.err.println("*** Error preparing for nested_loop_join");
			System.err.println("" + e);
			Runtime.getRuntime().exit(1);
		}
		
		
		
		try {
			while ((t = nlj2.get_next()) != null) {
				t.print(Jtypes);
			}
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		
		try {
			nlj.close();
			am.close();
			nlj2.close();
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		System.out.println("PathExpression.query() buffers : "+SystemDefs.JavabaseBM.getNumBuffers()+" "
				+ "unpinned : "+SystemDefs.JavabaseBM.getNumUnpinnedBuffers());
	}
	
	public Iterator evaluatePathExpression(ExpType[] inputAttrTypes, String[] values){
		AttrType[] nodeAttrTypes = new AttrType[2];
		nodeAttrTypes[0] = new AttrType(AttrType.attrString);
		nodeAttrTypes[1] = new AttrType(AttrType.attrDesc);
		FldSpec[] nodeProjList = new FldSpec[2];
		RelSpec rel = new RelSpec(RelSpec.outer);
		nodeProjList[0] = new FldSpec(rel, 1);
		nodeProjList[1] = new FldSpec(rel, 2);
		short[] nodeAttrStrSizes = new short[1];
		nodeAttrStrSizes[0] = Node.LABEL_MAX_LENGTH;
		
		ArrayList<String> labels = new ArrayList<>();
	    labels.add(values[0]);
	    
		// edges types
	    AttrType[] Etypes = { new AttrType(AttrType.attrString), new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrString), new AttrType(AttrType.attrString), };

		short[] Esizes = new short[3];
		Esizes[0] = Tuple.LABEL_MAX_LENGTH;
		Esizes[1] = 4;
		Esizes[2] = 4;
		
		FldSpec[] indexProjList = new FldSpec[8];
		RelSpec relation = new RelSpec(RelSpec.outer);
		indexProjList[0] = new FldSpec(relation, 1);
		indexProjList[1] = new FldSpec(relation, 2);
		indexProjList[2] = new FldSpec(relation, 3);
		indexProjList[3] = new FldSpec(relation, 4);
		indexProjList[4] = new FldSpec(relation, 5);
		indexProjList[5] = new FldSpec(relation, 6);
		indexProjList[6] = new FldSpec(relation, 7);
		indexProjList[7] = new FldSpec(relation, 8);
		
		
		AttrType[] Jtypes = { new AttrType(AttrType.attrString), 
				new AttrType(AttrType.attrString),
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrString), 
				new AttrType(AttrType.attrString), };

		short[] Jsizes = new short[4];
		Jsizes[0] = Tuple.LABEL_MAX_LENGTH;
		Jsizes[1] = Tuple.LABEL_MAX_LENGTH;
		Jsizes[2] = 4;
		Jsizes[3] = 4;
		
		CondExpr[] outFilter = new CondExpr[3];
		outFilter[0] = new CondExpr();
		outFilter[1] = new CondExpr();
		outFilter[2] = new CondExpr();

		CondExpr[] outFilter2 = new CondExpr[2];
		outFilter2[0] = new CondExpr();
		outFilter2[1] = new CondExpr();

		Query2_CondExpr(outFilter, outFilter2);
		
		FldSpec[] proj1 = { new FldSpec(new RelSpec(RelSpec.outer), 1), 
				new FldSpec(new RelSpec(RelSpec.innerRel), 1),
				new FldSpec(new RelSpec(RelSpec.innerRel), 6), 
				new FldSpec(new RelSpec(RelSpec.innerRel), 7),
				new FldSpec(new RelSpec(RelSpec.innerRel), 8) };
		
	    if(inputAttrTypes[0].expType==ExpType.expNodeLabel){
			CondExpr[] startNodeCondition = getConditionExprOnNodeLabels(labels,1);
			
			try {
				am = getIndexIteratorOnNodeLabel(nodeAttrTypes, nodeProjList, nodeAttrStrSizes, startNodeCondition);
			} catch (IndexException | InvalidTypeException | InvalidTupleSizeException | UnknownIndexTypeException
					| IOException e) {
				e.printStackTrace();
			}
		}else if(inputAttrTypes[0].expType == ExpType.expDesc){
			CondExpr[] startNodeCondition = getConditionExprForDescriptor(values[0],2);
			try {
				am = getIndexIteratorOnDescriptor(nodeAttrTypes, nodeProjList, nodeAttrStrSizes, startNodeCondition);
			} catch (IndexException | InvalidTypeException | InvalidTupleSizeException | UnknownIndexTypeException
					| IOException e) {
				e.printStackTrace();
			}
		}
		
		nljArray = new NestedIndexLoopJoin[inputAttrTypes.length-1];
		for(int i=1,j=0; i<inputAttrTypes.length;i++,j++){
			switch (inputAttrTypes[i].expType) {
			case ExpType.expDesc:
				CondExpr[] startNodeCondition = getConditionExprForDescriptor(values[i],2);
				Iterator zIndexIterator;
				ArrayList<String> nodelabels = new ArrayList<>();
				try {
					zIndexIterator = getIndexIteratorOnDescriptor(nodeAttrTypes, nodeProjList, nodeAttrStrSizes, startNodeCondition);
					Tuple t =null;
					while ((t = zIndexIterator.get_next()) != null) {
						String nodeLbl = t.getStrFld(1);
						nodelabels.add(nodeLbl);
					}
					zIndexIterator.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
				CondExpr[] rightFilter = getConditionExprOnNodeLabels(nodelabels, 8);
				if(i==1){
					try {
						nljArray[j] = getNestedIndexJoinOnSrouceLabel(nodeAttrTypes,2, nodeAttrStrSizes, rightFilter, Etypes,8, Esizes,
								indexProjList, am, outFilter, proj1,1);
					} catch (NestedLoopException | IOException e) {
						e.printStackTrace();
					}
				}else{
					try {
						nljArray[j] = getNestedIndexJoinOnSrouceLabel(Jtypes, 5,Jsizes, rightFilter, Etypes, 8,Esizes,
								indexProjList, nljArray[j-1], outFilter2, proj1,5);
					} catch (NestedLoopException | IOException e) {
						e.printStackTrace();
					}
				}
				break;
			case ExpType.expNodeLabel://node labels
				ArrayList<String> destLabels = new ArrayList<>();
				destLabels.add(values[i]);
				rightFilter = getConditionExprOnNodeLabels(destLabels, 8);
				if(i==1){
					try {
						nljArray[j] = getNestedIndexJoinOnSrouceLabel(nodeAttrTypes,2, nodeAttrStrSizes, rightFilter, Etypes,8, Esizes,
								indexProjList, am, outFilter, proj1,1);
					} catch (NestedLoopException | IOException e) {
						e.printStackTrace();
					}
				}else{
					try {
						nljArray[j] = getNestedIndexJoinOnSrouceLabel(Jtypes, 5,Jsizes, rightFilter, Etypes, 8,Esizes,
								indexProjList, nljArray[j-1], outFilter2, proj1,5);
					} catch (NestedLoopException | IOException e) {
						e.printStackTrace();
					}
				}
				break;
			case ExpType.expEdgeLabel:// for edge label
				ArrayList<String> edgeLabels = new ArrayList<>();
				edgeLabels.add(values[i]);
				rightFilter = getConditionExprOnNodeLabels(edgeLabels, 1);
				if(i==1){
					try {
						nljArray[j] = getNestedIndexJoinOnSrouceLabel(nodeAttrTypes,2, nodeAttrStrSizes, rightFilter, Etypes,8, Esizes,
								indexProjList, am, outFilter, proj1,1);
					} catch (NestedLoopException | IOException e) {
						e.printStackTrace();
					}
				}else{
					try {
						nljArray[j] = getNestedIndexJoinOnSrouceLabel(Jtypes, 5,Jsizes, rightFilter, Etypes, 8,Esizes,
								indexProjList, nljArray[j-1], outFilter2, proj1,5);
					} catch (NestedLoopException | IOException e) {
						e.printStackTrace();
					}
				}
				break;
			case ExpType.expWeight:// for edge weight
				rightFilter = getConditionExprOnEdgeWeight(Integer.parseInt(values[i]), 6);
				if(i==1){
					try {
						nljArray[j] = getNestedIndexJoinOnSrouceLabel(nodeAttrTypes,2, nodeAttrStrSizes, rightFilter, Etypes,8, Esizes,
								indexProjList, am, outFilter, proj1,1);
					} catch (NestedLoopException | IOException e) {
						e.printStackTrace();
					}
				}else{
					try {
						nljArray[j] = getNestedIndexJoinOnSrouceLabel(Jtypes, 5,Jsizes, rightFilter, Etypes, 8,Esizes,
								indexProjList, nljArray[j-1], outFilter2, proj1,5);
					} catch (NestedLoopException | IOException e) {
						e.printStackTrace();
					}
				}
				break;
			default:
				System.out.println("Attribute type "+inputAttrTypes[i].expType+ " not supported");
				break;
			}
			
		}
		/*try {
			Tuple t =null;
			while ((t = nljArray[nljArray.length-1].get_next()) != null) {
				t.print(Jtypes);
			}
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		
		try {
			am.close();
			for(int i = 0;i<nljArray.length;i++){
				nljArray[i].close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}*/
		
		return nljArray[nljArray.length-1];
	}

	private CondExpr[] getConditionExprOnEdgeWeight(int weight, int fldNum) {
		CondExpr[] startNodeCondition = new CondExpr[2];
		startNodeCondition[0] = new CondExpr();
		startNodeCondition[1] = null;
		startNodeCondition[0].op = new AttrOperator(AttrOperator.aopLE);
		startNodeCondition[0].type1 = new AttrType(AttrType.attrSymbol);
		startNodeCondition[0].type2 = new AttrType(AttrType.attrInteger);
		startNodeCondition[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), fldNum);
		startNodeCondition[0].operand2.integer = weight;
		startNodeCondition[0].next = null;
		return startNodeCondition;
	}

	private CondExpr[] getConditionExprForDescriptor(String descriptor, int fldNo) {
		Descriptor desc =new Descriptor();
		String[] desVals = descriptor.split(",");
		desc.set(Integer.parseInt(desVals[0]), Integer.parseInt(desVals[1]), Integer.parseInt(desVals[2]),
				Integer.parseInt(desVals[3]), Integer.parseInt(desVals[4]));
		CondExpr[] expr = new CondExpr[3];
		expr[0] = new CondExpr();
		expr[0].op = new AttrOperator(AttrOperator.aopEQ);
		expr[0].operand1.desc = desc;
		expr[0].type1 = new AttrType(AttrType.attrDesc);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);
		expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer), fldNo);
		expr[0].next = null;
		expr[1] = new CondExpr();
		expr[1].op = new AttrOperator(AttrOperator.aopEQ);
		expr[1].operand1.desc = desc;
		expr[1].type1 = new AttrType(AttrType.attrDesc);
		expr[1].type2 = new AttrType(AttrType.attrSymbol);
		expr[1].operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer), fldNo);
		expr[1].next = null;
		expr[2] = null;
		return expr;
	}

	private Iterator getIndexIteratorOnDescriptor(AttrType[] nodeAttrTypes, FldSpec[] nodeProjList,
			short[] nodeAttrStrSizes, CondExpr[] startNodeCondition) throws IndexException, InvalidTypeException, InvalidTupleSizeException, UnknownIndexTypeException, IOException {
		return new IndexScan(new IndexType(IndexType.Z_Index), nodes.getFileName(), "GraphDBNODEDESC", nodeAttrTypes, nodeAttrStrSizes, 2, 2, nodeProjList, startNodeCondition, 2, false);
	}

	private CondExpr[] getConditionExprOnNodeLabels(ArrayList<String> labels, int fldNum) {
		CondExpr[] startNodeCondition = new CondExpr[2];
		startNodeCondition[0] = new CondExpr();
		startNodeCondition[1] = null;
		CondExpr expr = startNodeCondition[0];
		CondExpr prev = null;
		for(int i=0;i<labels.size();i++){
			expr.op = new AttrOperator(AttrOperator.aopEQ);
			expr.type1 = new AttrType(AttrType.attrSymbol);
			expr.type2 = new AttrType(AttrType.attrString);
			expr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), fldNum);
			expr.operand2.string = labels.get(i);
			expr.next = new CondExpr();
			prev = expr;
			expr = expr.next;
		}
		if(prev !=null){
			prev.next = null;
		}
		return startNodeCondition;
	}

	private NestedIndexLoopJoin getNestedIndexJoinOnSrouceLabel(AttrType[] nodeAttrTypes,int numberOfAttrInRel1, short[] nodeAttrStrSizes,
			CondExpr[] indexExpr, AttrType[] Etypes,int numberOfAttrInRel2, short[] Esizes, FldSpec[] indexProjList, Iterator am,
			CondExpr[] outFilter, FldSpec[] proj1, int srcLblFldNo) throws IOException, NestedLoopException {
		NestedIndexLoopJoin nlj;
		nlj = new NestedIndexLoopJoin(nodeAttrTypes, 2, nodeAttrStrSizes, Etypes, 8, Esizes, amountOfMemoryForEachJoin, am, "GraphDBEDGESRCLABEL", new IndexType(IndexType.B_Index),
				edges.getFileName(), indexProjList, 7, outFilter, indexExpr, proj1, proj1.length, srcLblFldNo);
		return nlj;
	}

	private Iterator getIndexIteratorOnNodeLabel(AttrType[] nodeAttrTypes, 
			FldSpec[] nodeProjList, short[] nodeAttrStrSizes,
			CondExpr[] condExpr)
			throws IndexException, InvalidTypeException, InvalidTupleSizeException, UnknownIndexTypeException,
			IOException {
		return new IndexScan(new IndexType(IndexType.B_Index), nodes.getFileName(), "GraphDBNODELABEL", nodeAttrTypes,
				nodeAttrStrSizes, 2, 2, nodeProjList, condExpr, 1, false);
	}

	
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		
		return null;
	}

	
	public void close() throws IOException, JoinsException, SortException, IndexException {
		try {
			am.close();
			for(int i = 0;i<nljArray.length;i++){
				nljArray[i].close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
