package diskmgr;


import global.*;
import heap.*;
import iterator.*;

public class TriangleQuery {
	

	public SortMergeEdge sme;
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
			System.out.println("Strings:"+sublabel1+sublabel2);
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
		
		
		//performSecondMergeJoint(expr2,sme);


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
  		expr[2].op    = new AttrOperator(AttrOperator.aopLE);
  		expr[2].type1 = new AttrType(AttrType.attrSymbol);
  		expr[2].type2 = new AttrType(AttrType.attrString);
  		expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),6);
  		expr[2].operand2.string = label;
  
  		expr[3] = null;
  		return expr;
  	}
	




}
