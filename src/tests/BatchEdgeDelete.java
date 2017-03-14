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
class BatchEdgeDelete implements GlobalConst{
	/** Passes parameters to handler for current DB and outputs result of test. */
	public void batchedgedeletefunction (String[] args) throws FileNotFoundException
	{
		BatchEdgeDeleteHandler BH = SystemDefs.JavabaseDB.getBatchEdgeDeleteHandler();
		
			try{
				BH.runbatchedgedelete(args[2],args[1]);
			}
			catch(Exception e){
				System.out.println (""+e);	
			}

	} 


}
