package tests;


import diskmgr.NodeQueryHandler;
import global.SystemDefs;

public class nodequery {
	public static void main(String argv[]) {
		Runtime.getRuntime().exit(0);
	}
	
	public boolean runTests(String argv[]) {

		NodeQueryHandler queries = null;
		try {
			queries = SystemDefs.JavabaseDB.getNodeQueryHandler();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


		boolean _pass = false;
		switch(Integer.parseInt(argv[2])) {
		case 0:
			if(Integer.parseInt(argv[3]) == 1) {
				_pass = queries.nodeIndexTest0(argv);
			} else {
				_pass = queries.nodeHeapTest0(argv);
			}
			break;
		case 1:
			if(Integer.parseInt(argv[3]) == 1) {
				_pass = queries.nodeIndexTest1(argv);
			} else {
				_pass = queries.nodeHeapTest1(argv);
			}
			break;
		case 2:
			if(Integer.parseInt(argv[3]) == 1) {
				_pass = queries.nodeIndexTest2(argv);
			} else {
				_pass = queries.nodeHeapTest2(argv);
			}
			break;
		case 3:
			if(Integer.parseInt(argv[3]) == 1) {
				_pass = queries.nodeIndexTest3(argv);
			} else {
				_pass = queries.nodeHeapTest3(argv);
			}
			break;
		case 4:
			if(Integer.parseInt(argv[3]) == 1) {
				_pass = queries.nodeIndexTest4(argv);
			} else {
				_pass = queries.nodeHeapTest4(argv);
			}
			break;
		case 5:
			if(Integer.parseInt(argv[3]) == 1) {
				_pass = queries.nodeIndexTest5(argv);
			} else {
				_pass = queries.nodeHeapTest5(argv);
			}
			break;
		default:
			System.out.println("Not supported");
		}
		return _pass;
	}

}
