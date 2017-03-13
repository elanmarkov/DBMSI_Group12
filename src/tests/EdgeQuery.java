package tests;

import diskmgr.EdgeQueryHandler;
import global.SystemDefs;
import heap.InvalidTupleSizeException;

public class EdgeQuery {
	public boolean runTests(String argv[]) {
		EdgeQueryHandler queries = null;
		try {
			queries = SystemDefs.JavabaseDB.getEdgeQueryHandler();
		} catch (Exception e2) {
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
			break;
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

