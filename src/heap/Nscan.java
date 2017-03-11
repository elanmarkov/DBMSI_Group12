package heap;

import java.io.IOException;

import global.NID;

public class Nscan extends Scan{

	public Nscan(Heapfile hf) throws InvalidTupleSizeException, IOException {
		super(hf);
	}
	
	public Node getNext(NID nid) 
		    throws InvalidTupleSizeException,
			   IOException{
			Tuple tp = super.getNext(nid);
			Node node = new Node(tp.data, 0);
			return node;
		  }
	
	public boolean position(NID nid) 
		    throws InvalidTupleSizeException,
			   IOException{ 
			return position(nid);
		  }
	
}
