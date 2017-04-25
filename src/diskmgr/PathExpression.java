package diskmgr;

import java.io.IOException;
import java.util.ArrayList;

import btree.BTreeFile;
import global.AttrOperator;
import global.AttrType;
import global.Descriptor;
import global.ExpType;
import global.IndexType;
import heap.EdgeHeapFile;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Node;
import heap.NodeHeapFile;
import heap.Tuple;
import index.IndexException;
import index.IndexScan;
import index.UnknownIndexTypeException;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.Iterator;
import iterator.JoinsException;
import iterator.NestedIndexLoopJoin;
import iterator.NestedLoopException;
import iterator.RelSpec;
import iterator.SortException;
import zindex.ZCurve;
/*
 * @author Jayanth Kumar M J
 * this class provides api's for path expression.
 * it can handle path expressions of type
 * PE1 : NN/(NN/)* /NN
 * where NN <- (Node_label | Node_descriptor),
 * PE2 : NID /EN (/EN)*
 * where EN <- (Edge_label | max edge weight)
 * and 
 * PE3 : NID //Bound
 * where Bound <- (number_of_Edges | Max_total_weight)
 * 
 * It also handles a combination of PE1 and PE2 as mentioned below
 * PE12Hybrid : NN/[(NN/*)(EN/*)]/[(NN)(EN)]
 * 
 */
public class PathExpression{
	private static final String OUTPUT_FILE_NAME = "results";
	private int amountOfMemoryForEachJoin = 10;
	NodeHeapFile nodes;
	EdgeHeapFile edges;
	BTreeFile nodeLabels;
	ZCurve nodeDesc;
	BTreeFile edgeLabels;
	BTreeFile edgeWeights;
	graphDB db;
	private Iterator[] nljArray ;
	Iterator am = null;
	FileScan fam=null;
	NodeHeapFile results;
	NodeHeapFile intermediateFile;
	private static ArrayList<Integer> readCounter = new ArrayList<>();
	private static ArrayList<Integer> writeCounter = new ArrayList<>();

	public PathExpression(NodeHeapFile nodes, EdgeHeapFile edges, BTreeFile nodeLabels, ZCurve nodeDesc,
			BTreeFile edgeLabels, BTreeFile edgeWeights, graphDB db) {
		this.nodes = nodes;
		this.edges = edges;
		this.nodeLabels = nodeLabels;
		this.nodeDesc = nodeDesc;
		this.edgeLabels = edgeLabels;
		this.edgeWeights = edgeWeights;
		this.db = db;
		try {
			results =new NodeHeapFile(OUTPUT_FILE_NAME);
		} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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

	/*
	 *@param ExpType[] The array expression types
	 *@param values[] The array values for each type
	 *@return Iterator iterator for the output
	 *This method uses the Nested index loop joins 
	 *condition on the destination label to reduce number of tuples
	 *returns an iterator on the path query's output
	 * PE1 : NN/(NN/)* /NN
	 * where NN <- (Node_label | Node_descriptor),
	 * PE2 : NID /EN (/EN)*
	 * where EN <- (Edge_label | max edge weight)
	 * It also handles a combination of PE1 and PE2 as mentioned below
	 * PE12Hybrid : NN/[(NN/*)(EN/*)]/[(NN)(EN)]
	 */
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
				System.out.println("(Find tuples on NodeLabel Index File which satisfies Given condition) ");
				am = getIndexIteratorOnNodeLabel(nodeAttrTypes, nodeProjList, nodeAttrStrSizes, startNodeCondition);
			} catch (IndexException | InvalidTypeException | InvalidTupleSizeException | UnknownIndexTypeException
					| IOException e) {
				e.printStackTrace();
			}
		}else if(inputAttrTypes[0].expType == ExpType.expDesc){
			CondExpr[] startNodeCondition = getConditionExprForDescriptor(values[0],2);
			try {
				System.out.println("(Find tuples on node Descriptor Index File which satisfies Given condition) ");
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
					if(nodelabels.isEmpty()){
						nodelabels.add("INVALID_LBL");
					}
					zIndexIterator.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
				CondExpr[] rightFilter = getConditionExprOnNodeLabels(nodelabels, 8);
				System.out.println(" Join On ( Source label Index file with source condition and"
						+ "destination Condition (dest_label = next_Descriptor's_label_in_PE))");
				if(i==1){
					try {
						nljArray[j] = getNestedIndexJoinOnSrouceLabel(nodeAttrTypes,2, nodeAttrStrSizes, rightFilter, Etypes,8, Esizes,
								indexProjList, am, outFilter, proj1,1,null);
					} catch (NestedLoopException | IOException e) {
						e.printStackTrace();
					}
				}else{
					try {
						nljArray[j] = getNestedIndexJoinOnSrouceLabel(Jtypes, 5,Jsizes, rightFilter, Etypes, 8,Esizes,
								indexProjList, nljArray[j-1], outFilter2, proj1,5,null);
					} catch (NestedLoopException | IOException e) {
						e.printStackTrace();
					}
				}
				break;
			case ExpType.expNodeLabel://node labels
				ArrayList<String> destLabels = new ArrayList<>();
				destLabels.add(values[i]);
				rightFilter = getConditionExprOnNodeLabels(destLabels, 8);
				System.out.println(" Join On ( Source label Index file with source condition and"
						+ "destination Condition (dest_label = next_label_in_PE))");
				if(i==1){
					try {
						nljArray[j] = getNestedIndexJoinOnSrouceLabel(nodeAttrTypes,2, nodeAttrStrSizes, rightFilter, Etypes,8, Esizes,
								indexProjList, am, outFilter, proj1,1,null);
					} catch (NestedLoopException | IOException e) {
						e.printStackTrace();
					}
				}else{
					try {
						nljArray[j] = getNestedIndexJoinOnSrouceLabel(Jtypes, 5,Jsizes, rightFilter, Etypes, 8,Esizes,
								indexProjList, nljArray[j-1], outFilter2, proj1,5,null);
					} catch (NestedLoopException | IOException e) {
						e.printStackTrace();
					}
				}
				break;
			case ExpType.expEdgeLabel:// for edge label
				ArrayList<String> edgeLabels = new ArrayList<>();
				edgeLabels.add(values[i]);
				rightFilter = getConditionExprOnNodeLabels(edgeLabels, 1);
				System.out.println(" Join On ( Source label Index file with source condition and"
						+ "edge label Condition (edge_label = next_edge_label_in_PE))");
				if(i==1){
					try {
						nljArray[j] = getNestedIndexJoinOnSrouceLabel(nodeAttrTypes,2, nodeAttrStrSizes, rightFilter, Etypes,8, Esizes,
								indexProjList, am, outFilter, proj1,1,null);
					} catch (NestedLoopException | IOException e) {
						e.printStackTrace();
					}
				}else{
					try {
						nljArray[j] = getNestedIndexJoinOnSrouceLabel(Jtypes, 5,Jsizes, rightFilter, Etypes, 8,Esizes,
								indexProjList, nljArray[j-1], outFilter2, proj1,5,null);
					} catch (NestedLoopException | IOException e) {
						e.printStackTrace();
					}
				}
				break;
			case ExpType.expWeight:// for edge weight
				System.out.println(" Join On ( Source label Index file with source condition and"
						+ "edge weight Condition (edge_weight <= next_edge_weight_in_PE))");
				rightFilter = getConditionExprOnEdgeWeight(Integer.parseInt(values[i]), 6);
				if(i==1){
					try {
						nljArray[j] = getNestedIndexJoinOnSrouceLabel(nodeAttrTypes,2, nodeAttrStrSizes, rightFilter, Etypes,8, Esizes,
								indexProjList, am, outFilter, proj1,1,null);
					} catch (NestedLoopException | IOException e) {
						e.printStackTrace();
					}
				}else{
					try {
						nljArray[j] = getNestedIndexJoinOnSrouceLabel(Jtypes, 5,Jsizes, rightFilter, Etypes, 8,Esizes,
								indexProjList, nljArray[j-1], outFilter2, proj1,5,null);
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
		NestedIndexLoopJoin.setNUMBER_OF_JOINS(nljArray.length);
		NestedIndexLoopJoin.savecurrentReadWriteCounter();
		return nljArray[nljArray.length-1];
	}
	
	/*
	 *@param ExpType starts nodes expression type
	 *@param values value if the start node's expression type
	 *@return Iterator iterator for the output
	 *This method uses the Nested index loop joins on source labels
	 *returns an iterator on the path query's output
	 *For max total weight bound it uses sum relation spec to 
	 *add to fields and project the result on the output tuple   
	 *For total number of edges bound, it does index nestedloop join
	 *on the source lable and stores the intermediate results
	 *as the Nodes which are reachable within the 
	 *total number of edges gives should be available in the output. 
	 *  PE3 : NID //Bound
	 * where Bound <- (number_of_Edges | Max_total_weight)
	 * 
	 */
	
	public Iterator evaluateBoundPathExpression(ExpType inputAttrType, String values,ExpType boundType, int bound){
		PathExpression.readCounter = new ArrayList<>();
		PathExpression.writeCounter = new ArrayList<>();
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
	    labels.add(values);
	    
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
		AttrType[] JtypesWithSum = { new AttrType(AttrType.attrString), 
				new AttrType(AttrType.attrString),
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrString), 
				new AttrType(AttrType.attrString),
				new AttrType(AttrType.attrInteger),};

		short[] Jsizes = new short[4];
		Jsizes[0] = Tuple.LABEL_MAX_LENGTH;
		Jsizes[1] = Tuple.LABEL_MAX_LENGTH;
		Jsizes[2] = 4;
		Jsizes[3] = 4;
		
		FldSpec[] jProjList = new FldSpec[5];
		jProjList[0] = new FldSpec(relation, 1);
		jProjList[1] = new FldSpec(relation, 2);
		jProjList[2] = new FldSpec(relation, 3);
		jProjList[3] = new FldSpec(relation, 4);
		jProjList[4] = new FldSpec(relation, 5);
		
		FldSpec[] jProjListWithWeight = new FldSpec[6];
		jProjListWithWeight[0] = new FldSpec(relation, 1);
		jProjListWithWeight[1] = new FldSpec(relation, 2);
		jProjListWithWeight[2] = new FldSpec(relation, 3);
		jProjListWithWeight[3] = new FldSpec(relation, 4);
		jProjListWithWeight[4] = new FldSpec(relation, 5);
		jProjListWithWeight[5] = new FldSpec(relation, 6);
		
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
				new FldSpec(new RelSpec(RelSpec.innerRel), 8)};
				//new FldSpec(new RelSpec(RelSpec.sum), -1 ,8)};
				
		FldSpec[] proj2 = { new FldSpec(new RelSpec(RelSpec.outer), 1), 
				new FldSpec(new RelSpec(RelSpec.innerRel), 1),
				new FldSpec(new RelSpec(RelSpec.innerRel), 6), 
				new FldSpec(new RelSpec(RelSpec.innerRel), 7),
				new FldSpec(new RelSpec(RelSpec.innerRel), 8),
				new FldSpec(new RelSpec(RelSpec.sum), -1 ,6)};
		
		FldSpec[] proj3 = { new FldSpec(new RelSpec(RelSpec.outer), 1), 
				new FldSpec(new RelSpec(RelSpec.innerRel), 1),
				new FldSpec(new RelSpec(RelSpec.innerRel), 6), 
				new FldSpec(new RelSpec(RelSpec.innerRel), 7),
				new FldSpec(new RelSpec(RelSpec.innerRel), 8),
				new FldSpec(new RelSpec(RelSpec.sum), 3 ,6)};
		int readCounter = PCounter.getRCount();
		int writeCounter = PCounter.getWCount();
	    if(inputAttrType.expType==ExpType.expNodeLabel){
			CondExpr[] startNodeCondition = getConditionExprOnNodeLabels(labels,1);
			
			try {
				System.out.println("(Find tuples on NodeLabel Index File which satisfies Given condition) ");
				am = getIndexIteratorOnNodeLabel(nodeAttrTypes, nodeProjList, nodeAttrStrSizes, startNodeCondition);
			} catch (IndexException | InvalidTypeException | InvalidTupleSizeException | UnknownIndexTypeException
					| IOException e) {
				e.printStackTrace();
			}
		}else if(inputAttrType.expType == ExpType.expDesc){
			CondExpr[] startNodeCondition = getConditionExprForDescriptor(values,2);
			try {
				System.out.println("(Find tuples on node Descriptor Index File which satisfies Given condition) ");
				am = getIndexIteratorOnDescriptor(nodeAttrTypes, nodeProjList, nodeAttrStrSizes, startNodeCondition);
			} catch (IndexException | InvalidTypeException | InvalidTupleSizeException | UnknownIndexTypeException
					| IOException e) {
				e.printStackTrace();
			}
		}
	    NestedIndexLoopJoin nlj=null;
		boolean done =false;
		switch (boundType.expType) {
			case ExpType.expNoOfEdges:
				for(int i=0;i<bound;i++){
					CondExpr[] rightFilter = null;
					System.out.println(" Join On (Source label Index file with source condition)");
					if(i==0){
						try {
							nlj = getNestedIndexJoinOnSrouceLabel(nodeAttrTypes,2, nodeAttrStrSizes, rightFilter, Etypes,8, Esizes,
									indexProjList, am, outFilter, proj1,1,null);
						} catch (NestedLoopException | IOException e) {
							e.printStackTrace();
						}
					}else{
						String prevFileName = "temp"+(i-1);
						CondExpr[] jFilter = null;
						try {
							fam = new FileScan(prevFileName,Jtypes,
									Jsizes, (short) Jtypes.length, (short)Jtypes.length, jProjList, jFilter);
						}catch (Exception e) {
							System.err.println("*** Error creating scan for Index scan");
							System.err.println("" + e);
							Runtime.getRuntime().exit(1);
						}
						try {
							nlj = getNestedIndexJoinOnSrouceLabel(Jtypes, 5,Jsizes, rightFilter, Etypes, 8,Esizes,
									indexProjList, fam, outFilter2, proj1,5,null);
						} catch (NestedLoopException | IOException e) {
							e.printStackTrace();
						}
					}
					try {
						intermediateFile = new NodeHeapFile("temp"+i);
					} catch (Exception e) {
						e.printStackTrace();
					}
					Tuple t = null;
					try {
						while ((t=nlj.get_next())!=null) {
							Tuple tp = new Tuple();
							tp.setHdr((short)5, Jtypes, Jsizes);
							tp.tupleCopy(t);
							intermediateFile.insertNode(tp.getTupleByteArray());
							results.insertNode(tp.getTupleByteArray());
						}
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					try {
						if(fam!=null){
							fam.close();
						}
						//delete prev intermediate file
						nlj.close();
						if(i>0){
							intermediateFile = new NodeHeapFile("temp"+(i-1));
							intermediateFile.deleteFile();
						}
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					PathExpression.readCounter.add(PCounter.getRCount()-readCounter);
					PathExpression.writeCounter.add(PCounter.getWCount()-writeCounter);
					readCounter = PCounter.getRCount();
					writeCounter = PCounter.getWCount();
				}
			try {
				intermediateFile = new NodeHeapFile("temp"+(bound-1));
				intermediateFile.deleteFile();
				if(fam!=null){
					fam.close();
				}
				nlj.close();
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
				break;
			case ExpType.expTotalWeights://node labels
				int i=0;
				done = false;
				CondExpr[] sumFilter = new CondExpr[2];
				totalEdgeWeightCondExpr(sumFilter,bound);
				while(!done){
					CondExpr[] rightFilter = getConditionExprOnEdgeWeight(bound, 6);
					System.out.println(" Join On ( Source label Index file with source condition and totalWeight <= given_total_weight)");
					if(i==0){
						try {
							nlj = getNestedIndexJoinOnSrouceLabel(nodeAttrTypes,2, nodeAttrStrSizes, rightFilter, Etypes,8, Esizes,
									indexProjList, am, outFilter, proj2,1,sumFilter);
						} catch (NestedLoopException | IOException e) {
							e.printStackTrace();
						}
					}else{
						String prevFileName = "temp"+(i-1);
						CondExpr[] jFilter = getConditionExprOnEdgeWeight(bound, 6);
						try {
							fam = new FileScan(prevFileName,JtypesWithSum,
									Jsizes, (short) JtypesWithSum.length, (short)JtypesWithSum.length, jProjListWithWeight, jFilter);
						}catch (Exception e) {
							System.err.println("*** Error creating scan for Index scan");
							System.err.println("" + e);
							Runtime.getRuntime().exit(1);
						}
						try {
							nlj = getNestedIndexJoinOnSrouceLabel(JtypesWithSum, 6,Jsizes, rightFilter, Etypes, 8,Esizes,
									indexProjList, fam, outFilter2, proj3,5,sumFilter);
						} catch (NestedLoopException | IOException e) {
							e.printStackTrace();
						}
					}
					try {
						intermediateFile = new NodeHeapFile("temp"+i);
					} catch (Exception e) {
						e.printStackTrace();
					}
					Tuple t = null;
					try {
						boolean hasAtleastOneTuple = false;
						while ((t=nlj.get_next())!=null) {
							hasAtleastOneTuple = true;
							Tuple tp = new Tuple();
							tp.setHdr((short)6, JtypesWithSum, Jsizes);
							tp.tupleCopy(t);
							intermediateFile.insertNode(tp.getTupleByteArray());
							results.insertNode(tp.getTupleByteArray());
						}
						if(!hasAtleastOneTuple){
							done=true;
						}
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					try {
						if(fam!=null){
							fam.close();
						}
						//delete prev intermediate file
						nlj.close();
						intermediateFile = new NodeHeapFile("temp"+(i-1));
						intermediateFile.deleteFile();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					PathExpression.readCounter.add(PCounter.getRCount()-readCounter);
					PathExpression.writeCounter.add(PCounter.getWCount()-writeCounter);
					readCounter = PCounter.getRCount();
					writeCounter = PCounter.getWCount();
					i++;
				}
				break;
			default:
				System.out.println("Attribute type "+inputAttrType.expType+ " not supported");
				break;
		}
		try {
			if(boundType.expType == ExpType.expTotalWeights){
				fam = new FileScan(OUTPUT_FILE_NAME,JtypesWithSum,
						Jsizes, (short) JtypesWithSum.length, (short)JtypesWithSum.length, jProjListWithWeight, null);
				/*Tuple t =null;
				while ((t = fam.get_next()) != null) {
					t.print(JtypesWithSum);
				}*/
			}else{
				fam = new FileScan(OUTPUT_FILE_NAME,Jtypes,
						Jsizes, (short) Jtypes.length, (short)Jtypes.length, jProjList, null);
				/*Tuple t =null;
				while ((t = fam.get_next()) != null) {
					t.print(Jtypes);
				}*/
			}
			
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		
		/*try {
			//am.close();
			fam.close();
		} catch (Exception e) {
			e.printStackTrace();
		}*/
		
		return fam;
	}

	private void totalEdgeWeightCondExpr(CondExpr[] expr, int bound) {

		expr[0] = new CondExpr();
		expr[0].next = null;
		expr[0].op = new AttrOperator(AttrOperator.aopLE);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].type2 = new AttrType(AttrType.attrInteger);
		expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 6);
		expr[0].operand2.integer = bound;
		expr[1] = null;
		
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
			CondExpr[] outFilter, FldSpec[] proj1, int srcLblFldNo, CondExpr[] sumFilter) throws IOException, NestedLoopException {
		NestedIndexLoopJoin nlj;
		nlj = new NestedIndexLoopJoin(nodeAttrTypes, numberOfAttrInRel1, nodeAttrStrSizes, Etypes, Etypes.length, Esizes, amountOfMemoryForEachJoin, am, "GraphDBEDGESRCLABEL", new IndexType(IndexType.B_Index),
				edges.getFileName(), indexProjList, 7, outFilter, indexExpr, proj1, proj1.length, srcLblFldNo, sumFilter);
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

		
	/*
	 * closes all the iterators which were open for the execution of the path query
	 */
	public void close() throws IOException, JoinsException, SortException, IndexException {
		try {
			am.close();
			if(nljArray!=null){
				for(int i = 0;i<nljArray.length;i++){
					nljArray[i].close();
				}
				System.out.println("PathExpression.close() reads : "+NestedIndexLoopJoin.getReadCounter());
				System.out.println("PathExpression.close() write : "+NestedIndexLoopJoin.getWriteCounter());
			}
			if(fam!=null){
				fam.close();
				System.out.println("PathExpression.close() reads : "+PathExpression.readCounter);
				System.out.println("PathExpression.close() write : "+PathExpression.writeCounter);
			}
			results.deleteFile();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
