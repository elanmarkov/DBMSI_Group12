package heap;

import java.io.IOException;

import global.Convert;
import global.Descriptor;
import global.NID;
import global.RID;

public class Nscan extends Scan{

	public Nscan(Heapfile hf) throws InvalidTupleSizeException, IOException {
		super(hf);
	}
	
	public Node getNext(NID nid) 
		    throws InvalidTupleSizeException,
			   IOException{
			Tuple tp = super.getNext(new RID(nid.pageNo,nid.slotNo));
			if(tp!=null){
				Node node = new Node(tp.data, 0);
				Descriptor desc = Convert.getDescValue(Node.LABEL_MAX_LENGTH+2, tp.data);
				String nodeLbl = Convert.getStrValue(0, tp.data, Node.LABEL_MAX_LENGTH+2);
				node.setDesc(desc);
				node.setLabel(nodeLbl);
				return node;
			}else{
				return null;
			}
			
		  }
	
	public boolean position(NID nid) 
		    throws InvalidTupleSizeException,
			   IOException{ 
			return position(nid);
		  }
	
}
