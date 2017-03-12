//package tests;

import java.io.*;
import java.util.*;
import java.lang.*;
import heap.*;
import bufmgr.*;
import diskmgr.*;
import global.*;
import btree.*;


class BatchNodeDelete implements GlobalConst
{	
  private final static boolean OK   = true;
  private final static boolean FAIL = false;
  
  public void runbatchnodedelete(String dbname, String filename) throws FileNotFoundException{

	SystemDefs   sysdefs     = new SystemDefs(dbname,100, 100, "Clock");
	int          pages_read  = pageRW.rcounter;
	int 	     Pages_write = pageRW.wcounter;
	NodeHeapFile nodeheap    = sysdefs.JavabaseDB.getNodes();

	try{
	 File file = new File(filename);
	}
	catch(){
	 System.out.println("Could not open the InputFile.");
	 FAIL = true;
	}

        Scanner      inputFile    = new Scanner(file);
	EdgeHeapFile edgeheapfile = sysdefs.JavabaseDB.getEdges();
        

      	// Read lines from the file until no more are left.
	if(!FAIL){

      	 while (inputFile.hasNext())	//while 01 for going through the BatchNodeFile
         {
 	   // Read the next name.
 	   String inputnodelabel = inputFile.nextLine();
	   Nscan           nscan = nodeheap.openNodeScan();
	   Node             node = new Node();
	   NID               nid = new NID();
                            node = nscan.getNext(nid);
	    while(node!=null){	//while 02 for going through the nodeheapfile looking for the particular node

	      String label = node.getLabel();

	      if(Objects.equals(label,inputlabel)){	// nid with the given nodelabel found
          	 	 
	       Escan escan = edgeheap.openEdgeScan();
	       EID eid = new EID();
	       Edge edge = new Edge();
               edge = escan.getNext(eid);
	       while(edge!=null){		// trying to find the edges with destination and source node
	    	if(Objects.equals(edge.getSource(),nid) || Objects.equals(edge.getDestination(),nid)){
	     					
                 sysdefs.JavabaseDB.deleteEdge(eid);	
	         }

	    	 edge = escan.getNext(eid);
	        }
                escan.closescan();
	       } 

	      sysdefs.JavabaseDB.deleteNode(nid);
              node = getNext(nid);
	   }

	  nscan.closescan();
       	 }

         inputFile.close();
	 pages_read  = pageRW.rcounter - pages_read;
	 pages_write = pageRW.wcounter - pages_write;
	 System.out.println("Number of Pages Read: "+String(pages_read)+" Number of Page writes performed: "+String(pages_write));
        }

   }

   public static void main (String[] args) throws FileNotFoundException
   {
    
	if(args.length==2){
	 try{
	  runbatchnodedelete(args[0],args[1]);
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
