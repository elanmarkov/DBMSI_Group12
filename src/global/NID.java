package global;

public class NID extends RID {
	private String label;
	//private Descriptor attrDesc;

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
		
	}

	public NID() {
		super();
		this.label="";
		
		// TODO Auto-generated constructor stub
	}

	public NID(PageId pageno, int slotno, String label) {
		super(pageno, slotno);
		this.label=label;
		// TODO Auto-generated constructor stub
	}
	
}
