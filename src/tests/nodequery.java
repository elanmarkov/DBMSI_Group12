package tests;

import java.io.IOException;

import btree.AddFileEntryException;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.IndexFileScan;
import btree.IteratorException;
import btree.KeyDataEntry;
import btree.KeyNotMatchException;
import btree.PinPageException;
import btree.ScanIteratorException;
import btree.UnpinPageException;
import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import diskmgr.graphDB;
import global.AttrType;
import global.Descriptor;
import global.EID;
import global.GlobalConst;
import global.IndexType;
import global.NID;
import global.RID;
import global.SystemDefs;
import global.TupleOrder;
import heap.Edge;
import heap.EdgeHeapFile;
import heap.Escan;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.Node;
import heap.NodeHeapFile;
import heap.Nscan;
import heap.Tuple;
import index.IndexException;
import index.IndexScan;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.Sort;
import iterator.UnknownKeyTypeException;
import zindex.ZCurve;

class NQDriver extends TestDriver implements GlobalConst
{

	private final static boolean OK = true;
	private final static boolean FAIL = false;

	public NQDriver () {
		super("nodequerytest");      
	}
	public boolean runTests(String argv[]) {
		// Kill anything that might be hanging around
		//System.out.println(argv[0]);
		dbpath = argv[0];
		SystemDefs sysdef = new SystemDefs(dbpath,1000,Integer.parseInt(argv[1]),"Clock");
		graphDB database = SystemDefs.JavabaseDB;
		try {
			database.init();
			NodeQueryHandler queries = database.getNodeQueryHandler();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String newdbpath;
		String newlogpath;
		String remove_logcmd;
		String remove_dbcmd;
		String remove_cmd = "/bin/rm -rf ";

		newdbpath = dbpath;
		newlogpath = logpath;

		remove_logcmd = remove_cmd + logpath;
		remove_dbcmd = remove_cmd + dbpath;

		// Commands here is very machine dependent.  We assume
		// user are on UNIX system here
		/*try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		}
		catch (IOException e) {
			System.err.println ("IO error: "+e);
		}

		remove_logcmd = remove_cmd + newlogpath;
		remove_dbcmd = remove_cmd + newdbpath;

		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		}
		catch (IOException e) {
			System.err.println ("IO error: "+e);
		}*/

		/////////////////////////////////
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
		//////////////////////////////////
		//Clean up again
		/*try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		}
		catch (IOException e) {
			System.err.println ("IO error: "+e);
		}*/
		return _pass;
	}

}
public class nodequery {
	public static void main(String argv[]) {
		NQDriver hd = new NQDriver();
		boolean dbstatus;

		dbstatus = hd.runTests(argv);

		if (dbstatus != true) {
			System.err.println ("Error encountered during nodequery tests:\n");
			Runtime.getRuntime().exit(1);
		}

		Runtime.getRuntime().exit(0);
	}

}
