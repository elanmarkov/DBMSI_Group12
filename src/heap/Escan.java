package heap;

import java.io.IOException;

import global.EID;

public class Escan extends Scan{

	public Escan(Heapfile hf) throws InvalidTupleSizeException, IOException {
		super(hf);
	}
	
	public Edge getNext(EID eid) 
		    throws InvalidTupleSizeException,
			   IOException{
			Tuple tp = super.getNext(eid);
			Edge edge = new Edge(tp.data, 0);
			return edge;
		  }
	
	public boolean position(EID eid) 
		    throws InvalidTupleSizeException,
			   IOException{ 
			return position(eid);
		  }

}
