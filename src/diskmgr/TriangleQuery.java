package diskmgr;


import java.io.IOException;
import java.util.Arrays;

import bufmgr.PageNotReadException;
import global.*;
import heap.*;
import index.IndexException;
import iterator.*;

public class TriangleQuery {
	

	public SortMergeEdge sme;
	public NestedLoopsJoins nlj;
	public TriangleQuery(String query){
		performTriangleQuery(query);
	};
	public void performTriangleQuery(String query){
		
		CondExpr[] expr= new CondExpr[4];
		expr[0] = new CondExpr();
		expr[1] = new CondExpr();
		expr[2] = new CondExpr();
		expr[3] = new CondExpr();
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
			expr = setCondExprLL(edge_label);
		}
		else if(labels[0].equals("EL") && labels[1].equals("EW")){
			String edge_label = new String();
			edge_label = queries[0].substring(2);
			int weight = Integer.parseInt(queries[1].substring(2));
			expr = setCondExprLW(edge_label, weight);
		}
		else if(labels[0].equals("EW") && labels[1].equals("EW")){
			int[] weights = new int[2];
			String sublabel1 = queries[0].substring(2);
			String sublabel2 = queries[1].substring(2);
			weights[0] = Integer.parseInt(sublabel1);
			weights[1] = Integer.parseInt(sublabel2);
			expr = setCondExprWW(weights);	
		}
		else if(labels[0].equals("EW") && labels[1].equals("EL")){
			String edge_label = new String();
			edge_label = queries[1].substring(2);
			int weight = Integer.parseInt(queries[0].substring(2));
			
			expr = setCondExprWL(edge_label,weight);
		}
		else{
			System.out.println("Queries not provided properly.");
		}
		sme = new SortMergeEdge(expr);
		
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
		nlj = sme.performSecondJoin(secondexpr);
		
	
	}
	public Sort performSorting(NestedLoopsJoins input_join) {
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
	
	public void performDuplicateRemoval(NestedLoopsJoins input_join) throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, IOException, Exception {
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
		expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),8);
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
		expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
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
		expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),8);
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
		expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),6);
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
		expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),8);
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
		expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),6);
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
		expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),8);
		expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),7);

		expr[1].next   = null;
		expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
		expr[1].type1 = new AttrType(AttrType.attrSymbol);
		expr[1].type2 = new AttrType(AttrType.attrString);
		expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
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
  		expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),6);
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
  		expr[2].op    = new AttrOperator(AttrOperator.aopLE);
  		expr[2].type1 = new AttrType(AttrType.attrSymbol);
  		expr[2].type2 = new AttrType(AttrType.attrString);
  		expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),6);
  		expr[2].operand2.string = label;
  
  		expr[3] = null;
  		return expr;
  	}
	




}
