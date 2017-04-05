package iterator;

import global.AttrOperator;
import global.AttrType;
import global.Descriptor;
import global.TupleOrder;
import heap.Tuple;

public class SortMergeEdge {
	private static boolean OK = true;
	private static boolean FAIL = false;
	private  CondExpr  OutputFilter[];
	private SortMerge sm;

	public SortMergeEdge(String label) {

		OutputFilter = new CondExpr[3];
		OutputFilter[0] = new CondExpr();
		OutputFilter[1] = new CondExpr();
		OutputFilter[2] = new CondExpr();
		
		setCondExpr(OutputFilter,label);
		performSortMergeJoin();
	}

	public SortMergeEdge() {

		OutputFilter = new CondExpr[3];
		OutputFilter[0] = new CondExpr();
		OutputFilter[1] = new CondExpr();
		OutputFilter[2] = new CondExpr();
		
		setCondExpr(OutputFilter);
		performSortMergeJoin();
	}

	private static void setCondExpr(CondExpr[] expr) {

		expr[0].next  = null;
		expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);
		expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),7);
		expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),8);

		expr[1]   = null;

		expr[2] = null;
	}

	private static void setCondExpr(CondExpr[] expr, String label) {

		expr[0].next  = null;
		expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].type2 = new AttrType(AttrType.attrString);
		expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),7);
		expr[0].operand2.string = label;

		expr[1].next  = null;
		expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
		expr[1].type1 = new AttrType(AttrType.attrSymbol);
		expr[1].type2 = new AttrType(AttrType.attrString);
		expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),8);
		expr[1].operand2.string = label;


		expr[2] = null;
	}
	
	public void performSortMergeJoin() {
		boolean status = OK;
		
		AttrType [] E1types = new AttrType[8];
		E1types[0] = new AttrType(AttrType.attrString);
		E1types[1] = new AttrType(AttrType.attrInteger);
		E1types[2] = new AttrType(AttrType.attrInteger);
		E1types[3] = new AttrType(AttrType.attrInteger);
		E1types[4] = new AttrType(AttrType.attrInteger);
		E1types[5] = new AttrType(AttrType.attrInteger);
		E1types[6] = new AttrType(AttrType.attrString);
		E1types[7] = new AttrType(AttrType.attrString);

		//SOS
		short [] E1sizes = new short[3];
		E1sizes[0] = Tuple.LABEL_MAX_LENGTH;
		E1sizes[1] = 4;
		E1sizes[2] = 4;

		FldSpec [] E1projection = new FldSpec[8];
		E1projection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		E1projection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		E1projection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
		E1projection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);
		E1projection[4] = new FldSpec(new RelSpec(RelSpec.outer), 5);
		E1projection[5] = new FldSpec(new RelSpec(RelSpec.outer), 6);
		E1projection[6] = new FldSpec(new RelSpec(RelSpec.outer), 7);
		E1projection[7] = new FldSpec(new RelSpec(RelSpec.outer), 8);

		FileScan am = null;
		try {
			am  = new FileScan("GraphDBEDGEHEAP", E1types, E1sizes, 
					(short)8, (short)8,
					E1projection, null);
		}
		catch (Exception e) {
			status = FAIL;
			System.err.println (""+e);
		}

		if (status != OK) {
			//bail out
			System.err.println ("*** Error setting up scan for Edges");
			Runtime.getRuntime().exit(1);
		}

		AttrType [] E2types = new AttrType[8];
		E2types[0] = new AttrType(AttrType.attrString);
		E2types[1] = new AttrType(AttrType.attrInteger);
		E2types[2] = new AttrType(AttrType.attrInteger);
		E2types[3] = new AttrType(AttrType.attrInteger);
		E2types[4] = new AttrType(AttrType.attrInteger);
		E2types[5] = new AttrType(AttrType.attrInteger);
		E2types[6] = new AttrType(AttrType.attrString);
		E2types[7] = new AttrType(AttrType.attrString);

		short [] E2sizes = new short[3];
		E2sizes[0] = Tuple.LABEL_MAX_LENGTH;
		E2sizes[1] = 4;
		E2sizes[2] = 4;
		FldSpec [] E2projection = new FldSpec[8];
		E2projection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		E2projection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		E2projection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
		E2projection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);
		E2projection[4] = new FldSpec(new RelSpec(RelSpec.outer), 5);
		E2projection[5] = new FldSpec(new RelSpec(RelSpec.outer), 6);
		E2projection[6] = new FldSpec(new RelSpec(RelSpec.outer), 7);
		E2projection[7] = new FldSpec(new RelSpec(RelSpec.outer), 8);

		FileScan am2 = null;
		try {
			am2 = new FileScan("GraphDBEDGEHEAP", E2types, E2sizes, 
					(short)8, (short) 8,
					E2projection, null);
		}
		catch (Exception e) {
			status = FAIL;
			System.err.println (""+e);
		}

		if (status != OK) {
			//bail out
			System.err.println ("*** Error setting up scan for reserves");
			Runtime.getRuntime().exit(1);
		}


		FldSpec [] proj_list = new FldSpec[16];
		proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
		proj_list[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
		proj_list[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);
		proj_list[4] = new FldSpec(new RelSpec(RelSpec.outer), 5);
		proj_list[5] = new FldSpec(new RelSpec(RelSpec.outer), 6);
		proj_list[6] = new FldSpec(new RelSpec(RelSpec.outer), 7);
		proj_list[7] = new FldSpec(new RelSpec(RelSpec.outer), 8);
		proj_list[8] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		proj_list[9] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
		proj_list[10] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);
		proj_list[11] = new FldSpec(new RelSpec(RelSpec.innerRel), 4);
		proj_list[12] = new FldSpec(new RelSpec(RelSpec.innerRel), 5);
		proj_list[13] = new FldSpec(new RelSpec(RelSpec.innerRel), 6);
		proj_list[14] = new FldSpec(new RelSpec(RelSpec.innerRel), 7);
		proj_list[15] = new FldSpec(new RelSpec(RelSpec.innerRel), 8); 

		TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
		sm = null;
		Descriptor y = new Descriptor();
		y.set(-1,-1,-1,-1,-1);
		try {
			sm = new SortMerge(E1types, 8, E1sizes,
					E2types, 8, E2sizes,
					7, 4, 
					8, 4, 
					50,
					am, am2, 
					false, false, ascending,10, y,
					OutputFilter, proj_list, 16);
		}
		catch (Exception e) {
			System.err.println("*** join error in SortMerge constructor ***"); 
			status = FAIL;
			System.err.println (""+e);
			e.printStackTrace();
		}

		if (status != OK) {
			//bail out
			System.err.println ("*** Error constructing SortMerge");
			Runtime.getRuntime().exit(1);
		}
	}
	
	public Tuple get_next() {
		boolean status = OK;
		Tuple t = new Tuple();
		t = null;

		try {
				t = sm.get_next();
		} catch (Exception e) {
			System.err.println (""+e);
			e.printStackTrace();
			status = FAIL;
		}
		if (status != OK) {
			//bail out
			System.err.println ("*** Error in get next tuple ");
			Runtime.getRuntime().exit(1);
		}
		return t;
	}
	 
	public void close() {
		boolean status = OK;
		try {
			sm.close();
		}
		catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		System.out.println ("\n"); 
		if (status != OK) {
			//bail out
			System.err.println ("*** Error in closing ");
			Runtime.getRuntime().exit(1);
		}
	}
}
