package heap;

import java.io.IOException;

import global.AttrType;
import global.Convert;

public class Node extends Tuple {

	private String label;
	public Node()
	{
		data = new byte[max_size];
	       tuple_offset = 0;
	       tuple_length = max_size;
		
	}
    public void Node(byte[] anode, int offset)
    {
    	 data = anode;
         tuple_offset = offset;	
    }
	public void Node(Node fromNode)
	{
		data = fromNode.getTupleByteArray();
	       tuple_length = fromNode.getLength();
	       tuple_offset = 0;
	       fldCnt = fromNode.noOfFlds(); 
	       fldOffset = fromNode.copyFldOffset();
	}
	public String getLabel()
	{
		return label;
	}
	public Descriptor getDesc()
	{
		
	}
	public Node setLabel(String Label)
	{
	 label=Label;
	return null;	
	}
	public Node setDesc(Descriptor Desc)
	{
		return null;
	}
	public byte[] getNodeByteArray()
	{
		 byte [] nodecopy = new byte [tuple_length];
	       System.arraycopy(data, tuple_offset, nodecopy, 0, tuple_length);
	       return nodecopy;
	}
	public void print(AttrType type[]) throws IOException
	{
		int i, val;
		  float fval;
		  String sval;

		  System.out.print("[");
		  for (i=0; i< fldCnt-1; i++)
		   {
		    switch(type[i].attrType) {

		   case AttrType.attrInteger:
		     val = Convert.getIntValue(fldOffset[i], data);
		     System.out.print(val);
		     break;

		   case AttrType.attrReal:
		     fval = Convert.getFloValue(fldOffset[i], data);
		     System.out.print(fval);
		     break;

		   case AttrType.attrString:
		     sval = Convert.getStrValue(fldOffset[i], data,fldOffset[i+1] - fldOffset[i]);
		     System.out.print(sval);
		     break;
		  
		   case AttrType.attrNull:
		   case AttrType.attrSymbol:
		     break;
		   }
		   System.out.print(", ");
		 } 
		 
		 switch(type[fldCnt-1].attrType) {

		   case AttrType.attrInteger:
		     val = Convert.getIntValue(fldOffset[i], data);
		     System.out.print(val);
		     break;

		   case AttrType.attrReal:
		     fval = Convert.getFloValue(fldOffset[i], data);
		     System.out.print(fval);
		     break;

		   case AttrType.attrString:
		     sval = Convert.getStrValue(fldOffset[i], data,fldOffset[i+1] - fldOffset[i]);
		     System.out.print(sval);
		     break;

		   case AttrType.attrNull:
		   case AttrType.attrSymbol:
		     break;
		   }
		   System.out.println("]");
	}
	public short size()
	{
		 return ((short) (fldOffset[fldCnt] - tuple_offset));
	}
	public void nodeCopy(Node fromNode)
	{
		 byte [] temparray = fromNode.getTupleByteArray();
	       System.arraycopy(temparray, 0, data, tuple_offset, tuple_length);  
		
	}
	public void nodeInit(byte[] anode, int offset)
	{
		 data = anode;
	      tuple_offset = offset;
	
	}
	public void nodeSet(byte[] fromnode, int offset)
	{
		System.arraycopy(fromnode, offset, data, 0, offset);
	      tuple_offset = 0;
	     
	}
	
}
