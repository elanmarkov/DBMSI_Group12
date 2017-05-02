/*
 * This Class is just for testing the functionality of the sort merge join
 */
package tests;

import global.AttrOperator;
import global.AttrType;
import heap.Tuple;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.NestedLoopsJoins;
import iterator.RelSpec;
import iterator.SortMergeEdge;

public class SMJoinEdge {

	private static CondExpr[] setCondExprWW(int weights1, int weights2) {
		CondExpr[] expr = new CondExpr[4];
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
		expr[1].operand2.string = "1";

		expr[2].next   = null;
		expr[2].op    = new AttrOperator(AttrOperator.aopEQ);
		expr[2].type1 = new AttrType(AttrType.attrSymbol);
		expr[2].type2 = new AttrType(AttrType.attrString);
		expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
		expr[2].operand2.string = "2";

		expr[3] = null;
		return expr;
	}

	private static CondExpr[] setCondExprW(int weights1) {
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
		expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
		expr[2].operand2.string = "3";

		expr[3] = null;
		return expr;
	}

	public static void performSortMergeJoin(String label) {
		SortMergeEdge sme = null;
		int i = 1;
		if(i == 1) {
			sme = new SortMergeEdge(setCondExprWW(2000,2000));
		} else {
			if(label != null) {
				sme = new SortMergeEdge(label);
			} else  {
				sme = new SortMergeEdge();
			}
		}

		NestedLoopsJoins inl = sme.performSecondJoin(setCondExprW(2000));
		AttrType [] jtype1 = new AttrType[3];
		jtype1[0] = new AttrType (AttrType.attrString);
		jtype1[1] = new AttrType (AttrType.attrString);
		jtype1[2] = new AttrType (AttrType.attrString);
		Tuple t1 = new Tuple();
		t1 = null;
		try {
			System.out.println("SMJoinEdge.performSortMergeJoin()");
			while ((t1 = inl.get_next()) != null) {
				try {
					t1.print(jtype1);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			try{
				inl.close();
			} catch(Exception e) {
				e.printStackTrace();
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
