package heap;

import java.io.IOException;

import global.NID;
import global.RID;

public class Nscan extends Scan {

	public Nscan(Heapfile hf) throws InvalidTupleSizeException, IOException {
		super(hf);
	}

	public Node getNext(NID nid) throws InvalidTupleSizeException, IOException {
		RID rid = new RID(nid.pageNo, nid.slotNo);
		Tuple tp = super.getNext(rid);
		nid.pageNo.pid = rid.pageNo.pid;
		nid.slotNo = rid.slotNo;
		if (tp != null) {
			Node node = new Node(tp.data, 0);
			// Descriptor desc = Convert.getDescValue(Node.LABEL_MAX_LENGTH+2,
			// tp.data);
			// String nodeLbl = Convert.getStrValue(0, tp.data,
			// Node.LABEL_MAX_LENGTH+2);
			// node.setDesc(desc);
			// node.setLabel(nodeLbl);
			return node;
		} else {
			return null;
		}

	}

	public boolean position(NID nid) throws InvalidTupleSizeException, IOException {
		return position(nid);
	}

}
