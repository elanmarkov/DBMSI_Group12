package tests;


import java.io.IOException;
import java.util.Arrays;

import diskmgr.PathQueryHandler;
import global.AttrType;
import global.ExpType;
import global.SystemDefs;
import heap.InvalidTupleSizeException;

public class PathQuery {
	
	public boolean runTests(String argv[]) throws InvalidTupleSizeException, IOException {

		PathQueryHandler queries = SystemDefs.JavabaseDB.getPathQueryHandler();
		try {
			queries = SystemDefs.JavabaseDB.getPathQueryHandler();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        boolean _pass = false;
		switch(argv[0]) {
		case "PQ1":
			if(argv[3].equals("a")) {
				System.out.println("PathQuery.runTests() 1");
				_pass = queries.pathQuery1a(argv[4]);
			} 
			else if(argv[3].equals("b")) {
				_pass = queries.pathQuery1b(argv[4]);
			}
			else {
				_pass = queries.pathQuery1c(argv[4]);
			}
			break;
		case "PQ2":
			if(argv[3].equals("a")) {
				_pass = queries.pathQuery2a(argv[4]);
			} else  if (argv[3].equals("b")){
				System.out.println("PathQuery.runTests() 3");
				_pass = queries.pathQuery2b(argv[4]);
			}
			else if (argv[0].equals("c"))
			{
				System.out.println("PathQuery.runTests() 4");
				_pass = queries.pathQuery2c(argv[4]);
			}
			break;
		case "PQ3":
			if(Integer.parseInt(argv[0]) == 1) {// change it
				_pass = queries.pathQuery3a(argv[4]);
			} else if (argv[0].equals("b")){
				_pass = queries.pathQuery3b(argv[4]);
			}
			else{
				_pass = queries.pathQuery3c(argv[4]);
			}
			break;
	
		default:
			System.out.println("Not supported");
		}
		return _pass;
	}

}
