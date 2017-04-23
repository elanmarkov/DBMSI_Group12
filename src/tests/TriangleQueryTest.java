/*Author Harshdeep
  This program runs with 2 parameters, FileName and GraphDBName. Task 13 of Phase 2. Takes
  input of batch of edges and deletes them from the GraphDBName database.*/ 
package tests;
import java.io.*;
import java.util.*;
import java.lang.*;
import heap.*;
import bufmgr.*;
import diskmgr.*;
import global.*;
import btree.*;

/** Class for the batch edge delete test.
Passes parameters to handler and runs program. */
class TriangleQueryTest implements GlobalConst{
	/** Passes parameters to handler for current DB and outputs result of test. */
	public TriangleQueryTest(String query) throws FileNotFoundException
	{
		TriangleQuery TQ = new TriangleQuery(query);
		AttrType [] jtype = new AttrType[4];
		jtype[0] = new AttrType (AttrType.attrString);
		jtype[1] = new AttrType (AttrType.attrString);
		jtype[2] = new AttrType (AttrType.attrString);
		jtype[3] = new AttrType (AttrType.attrString);
		Tuple t = new Tuple();
		t = null;
		while ((t = TQ.sme.get_next()) != null) {
			try {
				t.print(jtype);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		TQ.sme.close();

	} 
}

