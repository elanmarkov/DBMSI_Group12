package heap;

import java.io.IOException;

import global.Convert;
import global.EID;
import global.NID;
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
	
	public boolean updateEdge(EID eid, Edge edge) throws InvalidSlotNumberException, InvalidUpdateException,
	InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception {
		return updateRecord(eid, edge);
	}
	
	public Edge getEdge(EID eid) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException,
	HFDiskMgrException, HFBufMgrException, Exception {
		Tuple tp= super.getRecord(eid);
		if(tp!=null){
			Edge edge = new Edge(tp.data, 0);
			try {
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
			}
			return edge;
		}else{
			return null;
		}
	}
	
	public Escan openScan() throws InvalidTupleSizeException, IOException {
		Escan newscan = new Escan(this);
		return newscan;
	}
	public String getFileName() {
		return _fileName;
	}

}
