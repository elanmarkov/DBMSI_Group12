/*
 * Program to Perform Triangle Queries.
 * Queries are of the form TQA,TQB or TQC.
 * TQA is for getting triangles with no order and duplicates allowed.
 * TQB is for getting triangles with ascending order on the First Node Label in the Triplet.
 * TQC is for getting triangles such that no duplicates are present.
 * 
 * The Query has 3 Edge Label or Edge Weight combinations which represent the 3 edges of the triangles which will be output.
 * For Edge Label , the prefix "EL" is used, for Maximum Edge Weight, prefix "EW" is used. 
 * 
 * An example is TQA graphdb 1000 EL1;EL2;EW30
 * 
 * The above query find triangles such that one edge has label 1, other has label 2 and the thrid edge has Maximum edge weight of 30.
 * 
 * 
 * The Operation is executed using 2 IndexNestedLoop Joins. First Join is between an indexscan iterator which has only those elements
 * which satisfies the first edge condition and the EdgesSourceLabel Index File. The indexscan done earlier returns just the source 
 * and destination node labels. The join is performed on the sourcelabel in the EdgesSouceLabel Index and the second attribute from 
 * the indexscan. The projection from this join has 4 columns(SourceNode1,DestNode1,SourceNode2,DestNode2), such that DestNode1 = 
 * SourceNode2.
 * 
 * One more NestedIndexLoop Join is performed, on the result of last join and the EdgesSourceLabel again. This time, the join is given
 * the label or weight condition as the rightfilter and the outer filter are the conditions that the source label of the edge is the 
 * same as the value of column 4 of the left relation(i.e is the iterator obtained from first join) as well as the condition that the
 * destination label is same as the column 1 of the left relation.
 *  
 * */
package diskmgr;


import java.io.IOException;
import java.util.Arrays;

import bufmgr.PageNotReadException;
import global.*;
import heap.*;
import index.IndexException;
import index.IndexScan;
import index.UnknownIndexTypeException;
import iterator.*;

public class TriangleQuery {
	
	private static boolean OK = true;
	private static boolean FAIL = false;
	public NestedIndexLoopJoin NILJ;
	public NestedIndexLoopJoin nlj;
	public IndexScan leftscan;
	public TriangleQuery(String query){
		performTriangleQuery(query);
	};
	public void performTriangleQuery(String query){
		int WorL[] = new int[2];   //Used to check if the Scan is needed on Edge Labels or Edge Weights, 1 if Label, 6 if weight.
		CondExpr[] main_expr = new CondExpr[4];
		main_expr[0]=new CondExpr();
		main_expr[1]=new CondExpr();
		main_expr[2]=new CondExpr();
		main_expr[3]=new CondExpr();
		CondExpr[] leftexpr= new CondExpr[2];   // Will be used to gather the required Tuples at the outer.
		leftexpr[0] = new CondExpr();
		leftexpr[1] = new CondExpr();
		CondExpr[] rightexpr = new CondExpr[2];	//Will be used to gather just the required tuples from the inner.
		rightexpr[0] = new CondExpr();
		rightexpr[1] = new CondExpr();
		CondExpr[] outerfilter = new CondExpr[2]; //Will be used to filter out just those tuples which have Dest_node=Source_node
		outerfilter[0] = new CondExpr();
		outerfilter[1] = new CondExpr();
		String[] queries = new String[3];
		queries[0] = new String();
		queries[1] = new String();
		queries[2] = new String();
		queries = query.split(";");    // Implement the parsing of the input to contain the queries that need to be run.
		String[] labels = new String[3];
		labels[0] = new String();
		labels[1] = new String();
		labels[2] = new String();
		labels[0] = queries[0].substring(0,2); 
		labels[1] = queries[1].substring(0,2); 
		labels[2] = queries[2].substring(0,2);
		if(labels[0].equals("EL") && labels[1].equals("EL")){
			String[] edge_label = new String[2];
			edge_label[0] = new String();
			edge_label[1] = new String();
			edge_label[0] = queries[0].substring(2);
			edge_label[1] = queries[1].substring(2);
			main_expr = setCondExprLL(edge_label);
			WorL[0] = 1;
			WorL[1] = 1;
				
		}
		else if(labels[0].equals("EL") && labels[1].equals("EW")){
			String edge_label = new String();
			edge_label = queries[0].substring(2);
			int weight = Integer.parseInt(queries[1].substring(2));
			main_expr = setCondExprLW(edge_label, weight);
			WorL[0] = 1;
			WorL[1] = 6;
		}
		else if(labels[0].equals("EW") && labels[1].equals("EW")){
			int[] weights = new int[2];
			String sublabel1 = queries[0].substring(2);
			String sublabel2 = queries[1].substring(2);
			weights[0] = Integer.parseInt(sublabel1);
			weights[1] = Integer.parseInt(sublabel2);
			main_expr = setCondExprWW(weights);
			WorL[0] = 6;
			WorL[1] = 6;
		}
		else if(labels[0].equals("EW") && labels[1].equals("EL")){
			String edge_label = new String();
			edge_label = queries[1].substring(2);
			int weight = Integer.parseInt(queries[0].substring(2));
			
			main_expr = setCondExprWL(edge_label,weight);
			WorL[0] = 6;
			WorL[1] = 1;
		}
		else{
			System.out.println("Queries not provided properly.");
		}
		leftexpr[0] = main_expr[1];
		leftexpr[1] = null;
		rightexpr[0] = main_expr[2];
		rightexpr[1] = null;
		outerfilter[0] = main_expr[0];
		outerfilter[1] = null;
			try {
				performfirstjoin(leftexpr,rightexpr,outerfilter,WorL);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		CondExpr[] secondexpr= new CondExpr[4];
		secondexpr[0] = new CondExpr();
		secondexpr[1] = new CondExpr();
		secondexpr[2] = new CondExpr();
		secondexpr[3] = new CondExpr();
		if(labels[2].equals("EW")) {
			int weight = Integer.parseInt(queries[2].substring(2));
			secondexpr = setCondExprW(weight);
		}
		else if(labels[2].equals("EL")) {
			secondexpr = setCondExprL(queries[2].substring(2));
		}
		nlj = performSecondJoin(secondexpr);
		
	
	}
	public NestedIndexLoopJoin performSecondJoin (CondExpr[] main_expr) {
		AttrType [] E1types = new AttrType[8];
		E1types[0] = new AttrType(AttrType.attrString);
		E1types[1] = new AttrType(AttrType.attrInteger);
		E1types[2] = new AttrType(AttrType.attrInteger);
		E1types[3] = new AttrType(AttrType.attrInteger);
		E1types[4] = new AttrType(AttrType.attrInteger);
		E1types[5] = new AttrType(AttrType.attrInteger);
		E1types[6] = new AttrType(AttrType.attrString);
		E1types[7] = new AttrType(AttrType.attrString);
		
		short [] E1sizes = new short[3];
		E1sizes[0] = Tuple.LABEL_MAX_LENGTH;
		E1sizes[1] = 4;
		E1sizes[2] = 4;
		CondExpr[] outfilter = new CondExpr[3];
		outfilter[0] = main_expr[0];
		outfilter[1] = main_expr[1];
		outfilter[2] = null;
		CondExpr[] rightexpr = new CondExpr[2];
		rightexpr[0] = main_expr[2];
		rightexpr[1] = null;
		
		AttrType [] E2types = new AttrType[4];
		E2types[0] = new AttrType(AttrType.attrString);
		E2types[1] = new AttrType(AttrType.attrString);
		E2types[2] = new AttrType(AttrType.attrString);
		E2types[3] = new AttrType(AttrType.attrString);

		short [] E2sizes = new short[4];
		E2sizes[0] = 4;
		E2sizes[1] = 4;
		E2sizes[2] = 4;
		E2sizes[3] = 4;
		
		FldSpec [] proj_list = new FldSpec[3];
		proj_list[0] = new FldSpec(new RelSpec(RelSpec.innerRel), 7);
		proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), 8);
		proj_list[2] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		RelSpec rel = new RelSpec(RelSpec.outer);
		FldSpec[] indexprojlist = new FldSpec[8];
		indexprojlist[0] = new FldSpec(rel,1);
		indexprojlist[1] = new FldSpec(rel,2);
		indexprojlist[2] = new FldSpec(rel,3);
		indexprojlist[3] = new FldSpec(rel,4);
		indexprojlist[4] = new FldSpec(rel,5);
		indexprojlist[5] = new FldSpec(rel,6);
		indexprojlist[6] = new FldSpec(rel,7);
		indexprojlist[7] = new FldSpec(rel,8);
		
		NestedIndexLoopJoin inl = null;
		try {
			inl = new NestedIndexLoopJoin(E2types,4, E2sizes, E1types, 8,
					E1sizes, 100, this.NILJ, "GraphDBEDGESRCLABEL", new IndexType(IndexType.B_Index), "GraphDBEDGEHEAP",indexprojlist,
					7, outfilter, rightexpr, proj_list, 3, 4,null);
		} catch(Exception e) {
			e.printStackTrace();
		}
		return inl;
	}
	
	public void performfirstjoin(CondExpr[] leftexpr, CondExpr[] rightexpr, CondExpr[] outfilter,int[] WorL) throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		boolean status = OK;

		FldSpec [] proj_list = new FldSpec[4];
		proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		proj_list[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		proj_list[2] = new FldSpec(new RelSpec(RelSpec.innerRel), 7);
		proj_list[3] = new FldSpec(new RelSpec(RelSpec.innerRel), 8);

		TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
		AttrType[] attrType = new AttrType[8];				//Initiating the Index Scan......
		attrType[0] = new AttrType(AttrType.attrString);
		attrType[1] = new AttrType(AttrType.attrInteger);
		attrType[2] = new AttrType(AttrType.attrInteger);
		attrType[3] = new AttrType(AttrType.attrInteger);
		attrType[4] = new AttrType(AttrType.attrInteger);
		attrType[5] = new AttrType(AttrType.attrInteger);
		attrType[6] = new AttrType(AttrType.attrString);
		attrType[7] = new AttrType(AttrType.attrString);
		FldSpec[] projlist = new FldSpec[2];
		RelSpec rel = new RelSpec(RelSpec.outer);
		projlist[1] = new FldSpec(rel, 8);
		projlist[0] = new FldSpec(rel, 7);
		short[] attrSize = new short[3];
		attrSize[0] = Tuple.LABEL_MAX_LENGTH;
		attrSize[1] = 4;
		attrSize[2] = 4;
		AttrType[] leftAttr = new AttrType[2];
		leftAttr[0] = new AttrType(AttrType.attrString);
		leftAttr[1] = new AttrType(AttrType.attrString);
	
		short[] leftattrSize = new short[3];
		leftattrSize[0] = 4;
		leftattrSize[1] = 4;
		FldSpec[] indexprojlist = new FldSpec[8];
		indexprojlist[0] = new FldSpec(rel,1);
		indexprojlist[1] = new FldSpec(rel,2);
		indexprojlist[2] = new FldSpec(rel,3);
		indexprojlist[3] = new FldSpec(rel,4);
		indexprojlist[4] = new FldSpec(rel,5);
		indexprojlist[5] = new FldSpec(rel,6);
		indexprojlist[6] = new FldSpec(rel,7);
		indexprojlist[7] = new FldSpec(rel,8);
		AttrType[] output = new AttrType[4];
		output[0] = new AttrType(AttrType.attrString);
		output[1] = new AttrType(AttrType.attrString);
		output[2] = new AttrType(AttrType.attrString);
		output[3] = new AttrType(AttrType.attrString);

		String[] indexName = {"GraphDBEDGELABEL","GraphDBEDGEWEIGHT"};
		if(WorL[0] == 6)
			indexName[0] = "GraphDBEDGEWEIGHT";
		if(WorL[1] == 1)
			indexName[1] = "GraphDBEDGELABEL";
		
		leftscan = null;
		
		try {

			leftscan = new IndexScan(new IndexType(IndexType.B_Index), "GraphDBEDGEHEAP", indexName[0], attrType, attrSize, 8, 2, projlist, leftexpr, WorL[0], false);
				} catch (IndexException | InvalidTypeException | InvalidTupleSizeException | UnknownIndexTypeException
				| IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.NILJ = new NestedIndexLoopJoin(leftAttr,2, leftattrSize, attrType, 8,
				attrSize, 100, leftscan, "GraphDBEDGESRCLABEL", new IndexType(IndexType.B_Index), "GraphDBEDGEHEAP",indexprojlist,
				7, outfilter, rightexpr, proj_list, 4, 2,null);
	

		
	}
	public Sort performSorting(NestedIndexLoopJoin input_join) {
		AttrType[] attrType = new AttrType[3];
	    attrType[0] = new AttrType(AttrType.attrString);
	    attrType[1] = new AttrType(AttrType.attrString);
	    attrType[2] = new AttrType(AttrType.attrString);
	    
	    short[] attrSize = new short[3];
	    attrSize[0] = 4;
	    attrSize[1] = 4;
	    attrSize[2] = 4;
	   
	    TupleOrder order = new TupleOrder(TupleOrder.Ascending);
	    Sort sort = null;
	   
	    try {
			sort = new Sort(attrType, (short) 3, attrSize, input_join, 1,order, 3, 100);
		} catch (SortException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	    return sort;
	}
	
	public void performDuplicateRemoval(NestedIndexLoopJoin input_join) throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, IOException, Exception {
		Tuple t;
		t=null;
		Heapfile tempheap = new Heapfile("Heapfortriangle"); 
		AttrType[] type = new AttrType[4];
		type[0] = new AttrType(AttrType.attrString);
		type[2] = new AttrType(AttrType.attrString);
		type[1] = new AttrType(AttrType.attrString);
		type[3] = new AttrType(AttrType.attrString);
		FldSpec[] projlist = new FldSpec[4];
		RelSpec rel = new RelSpec(RelSpec.outer);
		projlist[0] = new FldSpec(rel, 1);
		projlist[1] = new FldSpec(rel, 2);
		projlist[2] = new FldSpec(rel, 3);
		projlist[3] = new FldSpec(rel, 4);
		short[] strSizes = new short[4];
		strSizes[0]=30;
		strSizes[1]=4;
		strSizes[2]=4;
		strSizes[3]=4;
		while((t=input_join.get_next())!=null) {
		String[] values = new String[3];
		values = t.convertArray();
		while(values[0].compareTo(values[1])>0 || values[0].compareTo(values[2])>0) {
			String temp;
			temp = values[1];
			values[1] = values[0];
			values[0] = values[2];
			values[2] = temp;
			}
		Tuple t1= new Tuple();
		t1.setHdr((short)4, type, strSizes);
		String concat = values[0]+values[1]+values[2];
		t1.setStrFld(1, concat);
		t1.setStrFld(2, values[0]);
		t1.setStrFld(3, values[1]);
		t1.setStrFld(4, values[2]);
		
		tempheap.insertRecord(t1.getTupleByteArray());
			
		}
		
		
		FileScan fscan = new FileScan("Heapfortriangle", type, strSizes, (short) 4, 4, projlist, null);
		DuplElim dup = new DuplElim(type,(short)4,strSizes,fscan, 100, false);
		Tuple t1 = new Tuple();
		int[] fldno = {2,3,4};
		while((t1=dup.get_next())!=null) {
			t1.print(type,fldno);
		}
		dup.close();
		tempheap.deleteFile();
				
	}


	private static CondExpr[] setCondExprLL(String[] labels) {
		
		CondExpr[] expr= new CondExpr[4];
		expr[0] = new CondExpr();
		expr[1] = new CondExpr();
		expr[2] = new CondExpr();
		expr[3] = new CondExpr();
		expr[0].next  = null;
		expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);
		expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
		expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),7);

		expr[1].next   = null;
		expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
		expr[1].type1 = new AttrType(AttrType.attrSymbol);
		expr[1].type2 = new AttrType(AttrType.attrString);
		expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
		expr[1].operand2.string = labels[0];

		expr[2].next   = null;
		expr[2].op    = new AttrOperator(AttrOperator.aopEQ);
		expr[2].type1 = new AttrType(AttrType.attrSymbol);
		expr[2].type2 = new AttrType(AttrType.attrString);
		expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
		expr[2].operand2.string = labels[1];
		expr[3] = null;
		return expr;
	}

	private static CondExpr[] setCondExprWW(int[] weights) {
		CondExpr[] expr= new CondExpr[4];
		expr[0] = new CondExpr();
		expr[1] = new CondExpr();
		expr[2] = new CondExpr();
		expr[3] = new CondExpr();
		expr[0].next  = null;
		expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);
		expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
		expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),7);

		expr[1].next   = null;
		expr[1].op    = new AttrOperator(AttrOperator.aopLE);
		expr[1].type1 = new AttrType(AttrType.attrSymbol);
		expr[1].type2 = new AttrType(AttrType.attrInteger);
		expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),6);
		expr[1].operand2.integer = weights[0];
		expr[2].next   = null;
		expr[2].op    = new AttrOperator(AttrOperator.aopLE);
		expr[2].type1 = new AttrType(AttrType.attrSymbol);
		expr[2].type2 = new AttrType(AttrType.attrInteger);
		expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),6);
		expr[2].operand2.integer = weights[1];
		expr[3] = null;
		return expr;
	}

	private static CondExpr[] setCondExprLW(String label,int weight) {
		CondExpr[] expr= new CondExpr[4];
		expr[0] = new CondExpr();
		expr[1] = new CondExpr();
		expr[2] = new CondExpr();
		expr[3] = new CondExpr();
		expr[0].next  = null;
		expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);
		expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
		expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),7);
		expr[1].next   = null;
		expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
		expr[1].type1 = new AttrType(AttrType.attrSymbol);
		expr[1].type2 = new AttrType(AttrType.attrString);
		expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
		expr[1].operand2.string = label;
		expr[2].next   = null;
		expr[2].op    = new AttrOperator(AttrOperator.aopLE);
		expr[2].type1 = new AttrType(AttrType.attrSymbol);
		expr[2].type2 = new AttrType(AttrType.attrInteger);
		expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),6);
		expr[2].operand2.integer = weight;
		expr[3] = null;
		return expr;
	}

	private static CondExpr[] setCondExprWL(String label,int weight) {
		CondExpr[] expr= new CondExpr[4];
		expr[0] = new CondExpr();
		expr[1] = new CondExpr();
		expr[2] = new CondExpr();
		expr[3] = new CondExpr();
		expr[0].next  = null;
		expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);
		expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
		expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),7);

		expr[2].next   = null;
		expr[2].op    = new AttrOperator(AttrOperator.aopEQ);
		expr[2].type1 = new AttrType(AttrType.attrSymbol);
		expr[2].type2 = new AttrType(AttrType.attrString);
		expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
		expr[2].operand2.string = label;

		expr[1].next   = null;
		expr[1].op    = new AttrOperator(AttrOperator.aopLE);
		expr[1].type1 = new AttrType(AttrType.attrSymbol);
		expr[1].type2 = new AttrType(AttrType.attrInteger);
		expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),6);
		expr[1].operand2.integer = weight;

		expr[3] = null;
		return expr;
	}
	private static CondExpr[] setCondExprW(int weight) {
  		CondExpr[] expr = new CondExpr[4];
  		expr[0] = new CondExpr();
  		expr[1] = new CondExpr();
  		expr[2] = new CondExpr();
  		expr[3] = new CondExpr();
  
  		expr[0].next  = null;
  		expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
  		expr[0].type1 = new AttrType(AttrType.attrSymbol);
  		expr[0].type2 = new AttrType(AttrType.attrSymbol);
  		expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),7);
  		expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.outer),4);
  
  		expr[1].next   = null;
  		expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
  		expr[1].type1 = new AttrType(AttrType.attrSymbol);
  		expr[1].type2 = new AttrType(AttrType.attrSymbol);
  		expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),8);
  		expr[1].operand2.symbol =  new FldSpec (new RelSpec(RelSpec.outer),1);
  
  		expr[2].next   = null;
  		expr[2].op    = new AttrOperator(AttrOperator.aopLE);
  		expr[2].type1 = new AttrType(AttrType.attrSymbol);
  		expr[2].type2 = new AttrType(AttrType.attrInteger);
  		expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),6);
  		expr[2].operand2.integer = weight;
  
  		expr[3] = null;
  		return expr;
  	}
	private static CondExpr[] setCondExprL(String label) {
  		CondExpr[] expr = new CondExpr[4];
  		expr[0] = new CondExpr();
  		expr[1] = new CondExpr();
  		expr[2] = new CondExpr();
  		expr[3] = new CondExpr();
  
  		expr[0].next  = null;
  		expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
  		expr[0].type1 = new AttrType(AttrType.attrSymbol);
  		expr[0].type2 = new AttrType(AttrType.attrSymbol);
  		expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),7);
  		expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.outer),4);
  
  		expr[1].next   = null;
  		expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
  		expr[1].type1 = new AttrType(AttrType.attrSymbol);
  		expr[1].type2 = new AttrType(AttrType.attrSymbol);
  		expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),8);
  		expr[1].operand2.symbol =  new FldSpec (new RelSpec(RelSpec.outer),1);
  
  		expr[2].next   = null;
  		expr[2].op    = new AttrOperator(AttrOperator.aopEQ);
  		expr[2].type1 = new AttrType(AttrType.attrSymbol);
  		expr[2].type2 = new AttrType(AttrType.attrString);
  		expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
  		expr[2].operand2.string = label;
  
  		expr[3] = null;
  		return expr;
  	}
	




}
