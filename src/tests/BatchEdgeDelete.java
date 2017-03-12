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
	
	
	
	

	public static void main (String[] args) throws FileNotFoundException
	{
		BatchEdgeDeleteHandler BH = new BatchEdgeDeleteHandler();
		if(args.length==2){
			try{
				BH.runbatchedgedelete(args[0],args[1]);
			}
			catch(Exception e){
				System.out.println (""+e);	
			}
		}
		else{
			System.out.println("Improper Arguments");
		}

	} 


}
