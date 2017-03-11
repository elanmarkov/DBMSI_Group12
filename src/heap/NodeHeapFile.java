package heap;

import java.io.IOException;

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
		Tuple tp= super.getRecord(nid);
		Node node = new Node(tp.data, 0);
		return node;
	}
	
	public Nscan openScan() throws InvalidTupleSizeException, IOException {
		Nscan newscan = new Nscan(this);
		return newscan;
	}
}
