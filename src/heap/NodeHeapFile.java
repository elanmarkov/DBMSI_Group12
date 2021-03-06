package heap;

import java.io.IOException;

import global.Convert;
import global.Descriptor;
import global.NID;
import global.RID;

public class NodeHeapFile extends Heapfile{

	public NodeHeapFile(String name) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
		super(name);
	}
	
	public int getNodeCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException,
	HFBufMgrException, IOException{
		return super.getRecCnt();	
		
	}
	
	public NID insertNode(byte[] recPtr) throws InvalidSlotNumberException, InvalidTupleSizeException,
	SpaceNotAvailableException, HFException, HFBufMgrException, HFDiskMgrException, IOException {
		RID rid = super.insertRecord(recPtr);
		NID nid = new NID(rid.pageNo, rid.slotNo);
		return nid;
	}
	
	public boolean deleteNode(NID nid) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException,
	HFBufMgrException, HFDiskMgrException, Exception{
		return super.deleteRecord(nid);
	}
	
	public boolean updateNode(NID nid, Node node) throws InvalidSlotNumberException, InvalidUpdateException,
	InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception {
		return updateRecord(nid, node);
	}
	
	public Node getNode(NID nid) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException,
	HFDiskMgrException, HFBufMgrException, Exception {
		RID rid = new RID(nid.pageNo, nid.slotNo);
		Tuple tp= super.getRecord(rid);
		nid.pageNo.pid=rid.pageNo.pid;
		nid.slotNo=rid.slotNo;
		if(tp!=null){
			Node node = new Node(tp.data, 0);
			/*Descriptor desc = Convert.getDescValue(Node.LABEL_MAX_LENGTH+2, tp.data);
			String nodeLbl = Convert.getStrValue(0, tp.data, Node.LABEL_MAX_LENGTH+2);
			node.setDesc(desc);
			node.setLabel(nodeLbl);*/
			return node;
		}else{
			return null;
		}
	}
	
	public Nscan openScan() throws InvalidTupleSizeException, IOException {
		Nscan newscan = new Nscan(this);
		return newscan;
	}

	public String getFileName() {
		return _fileName;
	}
}
