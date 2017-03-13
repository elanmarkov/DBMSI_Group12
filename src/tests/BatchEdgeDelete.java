/*Author Harshdeep
  This program runs with 2 parameters, FileName and GraphDBName. Task 13 of Phase 2. Takes
  input of batch of edges and deletes them from the GraphDBName database.*/ 

import java.io.*;
import java.util.*;
import java.lang.*;
import heap.*;
import bufmgr.*;
import diskmgr.*;
import global.*;
import btree.*;


class BatchEdgeDelete implements GlobalConst{
	
	
	
	

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
