package global;

public class NID extends RID {
	
	public NID() {
		super();
	}

	public NID(PageId pageno, int slotno) {
		super(pageno, slotno);
	}

	@Override
	public String toString() {
		return "NID [slotNo=" + slotNo + ", pageNo=" + pageNo + "]";
	}
	
}
