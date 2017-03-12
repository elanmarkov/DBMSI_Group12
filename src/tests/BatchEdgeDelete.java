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
	
   private final static boolean OK   = true;
   private final static boolean FAIL = false;
  
   public void runbatchedgedelete(String dbname, String filename){

	SystemDefs   sysdefs  = new SystemDefs(dbname,100,100,"Clock");
	EdgeHeapFile edgeheap = sysdefs.JavabaseDB.getEdges();
	NodeHeapFile nodeheap = database.JavabaseDB.getNodes();

	try{
	 File file = new File(filename);
	}
	catch(){
	 System.out.println("Could not open the InputFile.");
	 FAIL = true;
	}

        Scanner inputFile = new Scanner(file);
      	// Read lines from the file until no more are left.

	if(!FAIL){

      	 while (inputFile.hasNext()){		//Loop for reading the inputFile.

 	        // Read the next name.
		String   nextinput   = inputFile.nextLine();
 	        String[] edgeinput   = nextinput.split("\\s+");
		Escan    escan       = edgeheap.openScan();
	 	Edge     edge        = new Edge();
	 	EID      eid         = new EID();
		int      pages_read  = pageRW.rcounter;
		int 	 Pages_write = pageRW.wcounter;

       	 	edge = escan.getNext(eid);

		if(edge==null) {

		 System.out.println("No edges in Database.");
		 OK = false;
		 };
	 	while(edge!=null && OK)	
	 	{
			 NID sourcenid = edge.getSource();
			 NID desnid = edge.getDestination();
			 String edgelabel = edge.getLabel();

	       		 if(Objects.equals(edgeinput[2],edgelabel)){	// Edge Found with given edge Label.
				Node sourcenode = nodeheapfile.getNode(sourcenid); 
				Node desnode    = nodeheapfile.getNode(desnid);

				if(Objects.equals(desnode.getLabel(),edgeinput[1]) && Objects.equals(sourcenode.getLabel(),edgeinput[0])){   // Checking if the edge is the one we are looking for.
				 sysdefs.JavabaseDB.deleteEdge(eid);
				 OK = false;
				 }

			 } 

			 edge = getNext(eid);
	  	}

		escan.closescan();
       	  	
          }

          inputFile.close();
	  pages_read  = pageRW.rcounter - pages_read;
	  pages_write = pageRW.wcounter - pages_write;
	  System.out.println("Number of Pages Read: "+String(pages_read)+" Number of Page writes performed: "+String(pages_write));
	  int edgecnt = sysdefs.JavabaseDB.getEdgeCnt();
	  int nodecnt = sysdefs.JavabaseDB.getNodeCnt();
	  System.out.println("Total Edge Count: "+ String(edgecnt) + "Total node Count: "+ String(nodecnt));

         }
	     
 	
   }

   public static void main (String[] args) throws FileNotFoundException
   {
    
	if(args.length==2){
	 try{
	  runbatchdegedelete(args[0],args[1]);
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
