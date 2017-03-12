package diskmgr;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Objects;
import java.util.Scanner;

import global.EID;
import global.NID;
import global.SystemDefs;
import heap.Edge;
import heap.EdgeHeapFile;
import heap.Escan;
import heap.Node;
import heap.NodeHeapFile;
import heap.Nscan;

public class BatchNodeDeleteHandler{
	private final static boolean OK   = true;
	  private final static boolean FAIL = false;
	  
	  public void runbatchnodedelete(String dbname, String filename) throws FileNotFoundException{

		SystemDefs   sysdefs     = new SystemDefs(dbname,100, 100, "Clock");
		PCounter     pageRW      = new PCounter();
		int          pages_read  = pageRW.rcounter;
		int 	     pages_write = pageRW.wcounter;
		NodeHeapFile nodeheap    = sysdefs.JavabaseDB.nodes;
	    File         file;
		try{
		 file = new File(filename);
		}
		catch(Exception e){
		 System.out.println("Could not open the InputFile.");
		 FAIL = true;
		}

	        Scanner      inputFile    = new Scanner(file);
		EdgeHeapFile     edgeheap     = sysdefs.JavabaseDB.getEdges();
	        

	      	// Read lines from the file until no more are left.
		if(!FAIL){

	      	 while (inputFile.hasNext())	//while 01 for going through the BatchNodeFile
	         {
	 	   // Read the next name.
	 	   String inputnodelabel = inputFile.nextLine();
		   Nscan           nscan = nodeheap.openScan();
		   Node             node = new Node();
		   NID               nid = new NID();
	                            node = nscan.getNext(nid);
		    while(node!=null){	//while 02 for going through the nodeheapfile looking for the particular node

		      String label = node.getLabel();

		      if(Objects.equals(label,inputnodelabel)){	// nid with the given nodelabel found
	          	 	 
		       Escan escan = edgeheap.openScan();
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
	              node = nscan.getNext(nid);
		   }

		  nscan.closescan();
	       	 }

	         inputFile.close();
		 pages_read  = pageRW.rcounter - pages_read;
		 pages_write = pageRW.wcounter - pages_write;
		 System.out.println("Number of Pages Read: "+pages_read+" Number of Page writes performed: "+pages_write);
	        }

	   }

}