package heap;

import java.io.IOException;

import global.AttrType;
import global.Convert;
import global.Descriptor;

public class Node extends Tuple {

	private String label;
	
	private Descriptor attrDesc;

	public Node()
	{
		super();
		
	}
    public Node(byte[] anode, int offset)
    {
    	super(anode, offset, anode.length);
    }
    
    public Node(byte[] anode, int offset, int length)
    {
    	super(anode, offset, length);
    }
    
	public Node(Node fromNode)
	{
		super(fromNode);
		this.attrDesc= fromNode.attrDesc;
		this.label=fromNode.label;
	}
	public String getLabel()
	{
		return label;
	}
	public Descriptor getDesc()
	{
		return attrDesc;
	}
	public Node setLabel(String label)
	{
	 this.label=getFixedLengthLable(label);
	 try {
		Convert.setStrValue(this.label, 0, data);
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	tuple_length = getNodeLength();
	return this;	
	}
	
	
	public Node setDesc(Descriptor Desc)
	{
		attrDesc=Desc;
		try {
			Convert.setDescValue(Desc, LABEL_MAX_LENGTH+2, data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		tuple_length = getNodeLength();
		return this;
	}
	private int getNodeLength() {
		return 10+LABEL_MAX_LENGTH+2;
	}
	public byte[] getNodeByteArray()
	{
		 return getTupleByteArray();
	}
	public void print(AttrType type[]) throws IOException
	{
		  System.out.print("[");
		 // Descriptor desc = Convert.getDescValue(LABEL_MAX_LENGTH+2, this.data);
		  System.out.print(this.attrDesc);
		  //String nodeLbl = Convert.getStrValue(0, data, LABEL_MAX_LENGTH+2);
		  System.out.print(", "+this.label);
		  System.out.println("]");
	}
	
	public void nodeCopy(Node fromNode)
	{
		 byte [] temparray = fromNode.getNodeByteArray();
	     System.arraycopy(temparray, 0, data, tuple_offset, tuple_length);  
		
	}
	public void nodeInit(byte[] anode, int offset)
	{
		 tupleInit(anode, offset, anode.length);
	
	}
	public void nodeSet(byte[] fromnode, int offset)
	{
		tupleSet(fromnode, offset, fromnode.length);
	     
	}
	
}
