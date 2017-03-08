package global;

public class EID extends RID {
	private String label;
	private NID source;
	private NID destination;
	private int weight;
	//private Descriptor attrDesc;

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
		
	}

	public EID() {
		super();
		this.label="";
		
		
		// TODO Auto-generated constructor stub
	}

	public EID(PageId pageno, int slotno, String label) {
		super(pageno, slotno);
		this.label=label;
		// TODO Auto-generated constructor stub
	}
}
