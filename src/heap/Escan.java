package heap;

import java.io.IOException;

import global.Convert;
import global.EID;
import global.NID;
import global.RID;

public class Escan extends Scan{

	public Escan(Heapfile hf) throws InvalidTupleSizeException, IOException {
		super(hf);
	}
	
	public Edge getNext(EID eid) 
		    throws InvalidTupleSizeException,
			   IOException{
			RID rid = new RID(eid.pageNo,eid.slotNo);
			Tuple tp = super.getNext(rid);
			eid.pageNo.pid=rid.pageNo.pid;
			eid.slotNo=rid.slotNo;
			if(tp!=null){
				Edge edge = new Edge(tp.data, 0);
				/*try {
					String eLbl = Convert.getStrValue(0, edge.data, Edge.LABEL_MAX_LENGTH+2);
					
					NID srcId = new NID();
					srcId.pageNo.pid = Convert.getIntValue(Edge.LABEL_MAX_LENGTH+2, edge.data);
					srcId.slotNo=Convert.getIntValue(Edge.LABEL_MAX_LENGTH+2+4, edge.data);
					NID destId = new NID();
					destId.pageNo.pid = Convert.getIntValue(Edge.LABEL_MAX_LENGTH+2+4+4, edge.data);
					destId.slotNo=Convert.getIntValue(Edge.LABEL_MAX_LENGTH+2+4+4+4, edge.data);
					int weight=Convert.getIntValue(Edge.LABEL_MAX_LENGTH+2+4+4+4+4, edge.data);
					edge.setLabel(eLbl);
					edge.setSource(srcId);
					edge.setDestination(destId);
					edge.setWeight(weight);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}*/
				return edge;
			}else{
				return null;
			}
		  }
	
	public boolean position(EID eid) 
		    throws InvalidTupleSizeException,
			   IOException{ 
			return position(eid);
		  }

}
