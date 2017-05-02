package tests;
import java.io.*;
import java.util.*;
import java.lang.*;
import heap.*;
import index.IndexException;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.NestedIndexLoopJoin;
import iterator.PredEvalException;
import iterator.Sort;
import iterator.SortException;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;
import bufmgr.*;
import diskmgr.*;
import global.*;
import btree.*;

/*This class is handler for the Triangle Queries
 * 
 */
class TriangleQueryTest implements GlobalConst{
	/** Passes parameters to handler for current DB and outputs result of test. 
	 * @throws Exception 
	 * @throws IOException 
	 * @throws UnknownKeyTypeException 
	 * @throws UnknowAttrType 
	 * @throws LowMemException 
	 * @throws SortException 
	 * @throws PredEvalException 
	 * @throws TupleUtilsException 
	 * @throws PageNotReadException 
	 * @throws InvalidTypeException 
	 * @throws InvalidTupleSizeException 
	 * @throws IndexException 
	 * @throws JoinsException */
	public TriangleQueryTest(String[] query) throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, IOException, Exception
	{	boolean empty = false;
		boolean notempty = true;
		boolean status = empty;
		TriangleQuery TQ = new TriangleQuery(query[3]);
		AttrType [] jtype = new AttrType[3];
		jtype[0] = new AttrType (AttrType.attrString);
		jtype[1] = new AttrType (AttrType.attrString);
		jtype[2] = new AttrType (AttrType.attrString);
		Tuple t = new Tuple();
		t = null;
		System.out.println("PROJECTION of Source Node and Destination Node Label of edge(SELECT with first expression as edge condition).");
		System.out.println("Join On(Edge Source Node Label Index with second expression as edge condition)");
		System.out.println("Projection of [SourceNode1,DestNode1,SourceNode2,DestNode2]");
		System.out.println("Join on (Edge Source Node Label Index with third expression as edge condition and DestNode1 = DestNodeLabel of inner relation)");
		System.out.println("Projection of [SourceNode1,DestNode1,DestNode2]");
		if(query[0].equals("TQA")) {
			
			System.out.println("Triangles with duplications, with no sorting are:");
			while ((t = TQ.nlj1.get_next()) != null) {
				try {
					status = notempty;
					t.print(jtype);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
		
		else if(query[0].equals("TQB")) {
			System.out.println("Triangles with duplications, with ascending sorting done on the first Node Label are:");
			Sort sort = TQ.performSorting(TQ.nlj1);
			System.out.println("Sort on Column 1");
			while ((t = sort.get_next()) != null) {
				try {
					status = notempty;
					t.print(jtype);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
			sort.close();
			
		}
		else if(query[0].equals("TQC")) {
			status = notempty;
			
			TQ.performDuplicateRemoval(TQ.nlj1);
		}
		

		TQ.nlj1.close();
		//TQ.leftscan.close();
		System.out.println("Number of reads : "+NestedIndexLoopJoin.getReadCounter());
		System.out.println("Number of writes : "+NestedIndexLoopJoin.getWriteCounter());

		if(!status) {
			System.out.println("No Triangles Found.");
		}
		

	} 
}

