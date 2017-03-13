package tests;

import diskmgr.EdgeQueryHandler;
import diskmgr.graphDB;
import global.GlobalConst;
import global.SystemDefs;
import heap.InvalidTupleSizeException;

class EQDriver extends TestDriver implements GlobalConst
{

	public EQDriver () {
		super("nodequerytest");      
	}
	
	public boolean runTests(String argv[]) {
		dbpath = argv[0];
		graphDB database = SystemDefs.JavabaseDB;
		EdgeQueryHandler queries = null;
		try {
			queries = database.getEdgeQueryHandler();
			
		} catch (Exception e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} 
		
		boolean _pass = false;
		switch(Integer.parseInt(argv[2])) {
		case 0:
			if(Integer.parseInt(argv[3]) == 1) {
				_pass = queries.edgeIndexTest0(argv);
			} else {
				_pass = queries.edgeHeapTest0(argv);
			}
			break;
		case 1:
			if(Integer.parseInt(argv[3]) == 1) {
				_pass = queries.edgeIndexTest1(argv);
			} else {
				_pass = queries.edgeHeapTest1(argv);
			}
			break;
		case 2:
			if(Integer.parseInt(argv[3]) == 1) {
				_pass = queries.edgeIndexTest2(argv);
			} else {
				_pass = queries.edgeHeapTest2(argv);
			}
			break;
		case 3:
			if(Integer.parseInt(argv[3]) == 1) {
				_pass = queries.edgeIndexTest3(argv);
			} else {
				_pass = queries.edgeHeapTest3(argv);
			}
			break;
		case 4:
			if(Integer.parseInt(argv[3]) == 1) {
				_pass = queries.edgeIndexTest4(argv);
			} else {
				_pass = queries.edgeHeapTest4(argv);
			}
			break;
		case 5:
			if(Integer.parseInt(argv[3]) == 1) {
				_pass = queries.edgeIndexTest5(argv);
			} else {
				_pass = queries.edgeHeapTest5(argv);
			}
		case 6:
			if(Integer.parseInt(argv[3]) == 1) {
				try {
					_pass = queries.edgeIndexTest6(argv);
				} catch (InvalidTupleSizeException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				_pass = queries.edgeHeapTest6(argv);
			}
			break;
		default:
			System.out.println("Not supported");
		}
		return _pass;
	}

}

public class EdgeQuery {

	public static void main(String argv[]) {
		EQDriver hd = new EQDriver();
		boolean dbstatus;

		dbstatus = hd.runTests(argv);

		if (dbstatus != true) {
			System.err.println ("Error encountered during nodequery tests:\n");
			Runtime.getRuntime().exit(1);
		}

		Runtime.getRuntime().exit(0);
	}

}

