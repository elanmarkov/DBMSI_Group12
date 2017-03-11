package heap;

import java.io.IOException;

import global.EID;
import global.RID;

public class EdgeHeapFile extends Heapfile{

	public EdgeHeapFile(String name) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
		super(name);
	}
	
	public int getEdgeCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException,
	HFBufMgrException, IOException{
		return super.getRecCnt();	
		
	}
	
	public EID insertEdge(byte[] recPtr) throws InvalidSlotNumberException, InvalidTupleSizeException,
	SpaceNotAvailableException, HFException, HFBufMgrException, HFDiskMgrException, IOException {
		RID rid = super.insertRecord(recPtr);
		EID eid = new EID(rid.pageNo, rid.slotNo);
		return eid;
	}
	
	public boolean deleteEdge(EID eid) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException,
	HFBufMgrException, HFDiskMgrException, Exception{
		return super.deleteRecord(eid);
	}
	
	public boolean updateNode(EID eid, Edge edge) throws InvalidSlotNumberException, InvalidUpdateException,
	InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception {
		return updateRecord(eid, edge);
	}
	
	public Edge getNode(EID nid) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException,
	HFDiskMgrException, HFBufMgrException, Exception {
		Tuple tp= super.getRecord(nid);
		Edge edge = new Edge(tp.data, 0);
		return edge;
	}
	
	public Escan openScan() throws InvalidTupleSizeException, IOException {
		Escan newscan = new Escan(this);
		return newscan;
	}

}
