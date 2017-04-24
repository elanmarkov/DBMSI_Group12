/*Author Harshdeep
  This program runs with 2 parameters, FileName and GraphDBName. Task 13 of Phase 2. Takes
  input of batch of edges and deletes them from the GraphDBName database.*/ 
package tests;
import java.io.*;
import java.util.*;
import java.lang.*;
import heap.*;
import index.IndexException;
import iterator.JoinsException;
import iterator.LowMemException;
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

/** Class for the batch edge delete test.
Passes parameters to handler and runs program. */
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
	{
		TriangleQuery TQ = new TriangleQuery(query[3]);
		AttrType [] jtype = new AttrType[3];
		jtype[0] = new AttrType (AttrType.attrString);
		jtype[1] = new AttrType (AttrType.attrString);
		jtype[2] = new AttrType (AttrType.attrString);
		
		Tuple t = new Tuple();
		t = null;
		if(query[0].equals("TQA")) {
			System.out.println("Triangles with duplications, with no sorting are:");
			while ((t = TQ.nlj.get_next()) != null) {
				try {
					t.print(jtype);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		else if(query[0].equals("TQB")) {
			System.out.println("Triangles with duplications, with sorting are:");
			Sort sort = TQ.performSorting(TQ.nlj);
			while ((t = sort.get_next()) != null) {
				try {
					t.print(jtype);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			sort.close();
			
		}
		else if(query[0].equals("TQC")) {
			System.out.println("Triangles without duplications, with sorting are:");
			TQ.performDuplicateRemoval(TQ.nlj);
		}
		
		TQ.nlj.close();
		TQ.sme.close();
		
		
		

	} 
}

