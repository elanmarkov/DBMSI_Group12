package heap;

import java.io.IOException;

import global.AttrType;
import global.Descriptor;

public class Node extends Tuple {

	private String label;
	public static final AttrType[] types = {new AttrType(AttrType.attrString),new AttrType(AttrType.attrDesc)};
	public static final short[] sizes = {LABEL_MAX_LENGTH,10};
	public static final short numFld = 2;
	private Descriptor attrDesc;

	public Node()
	{
		super();
		try {
			super.setHdr(numFld, types, sizes);
		} catch (InvalidTypeException | InvalidTupleSizeException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public Node(Tuple tp){
		this.data=tp.data;
		try {
			try {
				super.setHdr(numFld, types, sizes);
			} catch (InvalidTypeException | InvalidTupleSizeException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//this.attrDesc = Convert.getDescValue(Node.LABEL_MAX_LENGTH+2, tp.data);
			//this.label = Convert.getStrValue(0, tp.data, Node.LABEL_MAX_LENGTH+2);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
    public Node(byte[] anode, int offset)
    {
    	super(anode, offset, anode.length);
    	try {
			super.setHdr(numFld, types, sizes);
		} catch (InvalidTypeException | InvalidTupleSizeException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
		try {
			this.label = getStrFld(1);
		} catch (FieldNumberOutOfBoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return label;
	}
	public Descriptor getDesc()
	{
		try {
			this.attrDesc = getDescFld(2);
		} catch (FieldNumberOutOfBoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return attrDesc;
	}
	public Node setLabel(String label)
	{
	 this.label=getFixedLengthLable(label);
	 try {
		setStrFld(1, this.label);
	} catch (FieldNumberOutOfBoundException | IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	 /*try {
		Convert.setStrValue(this.label, 0, data);
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	tuple_length = getNodeLength();*/
	return this;	
	}
	
	
	public Node setDesc(Descriptor Desc)
	{
		attrDesc=Desc;
		try {
			setDescFld(2, Desc);
		} catch (FieldNumberOutOfBoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/*try {
			Convert.setDescValue(Desc, LABEL_MAX_LENGTH+2, data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		tuple_length = getNodeLength();*/
		return this;
	}
	
	public byte[] getNodeByteArray()
	{
		 return getTupleByteArray();
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
