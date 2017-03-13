package heap;

import global.AttrType;
import global.Convert;
import global.EID;
import global.NID;

import java.io.IOException;



public class Edge extends Tuple{
	private String label;
	private NID source;
	private NID destination;
	private int weight;
	public Edge()
	{
		super();
		
	}
    public Edge(byte[] anode, int offset)
    {
    	super(anode, offset, anode.length);
    }
    
    public Edge(byte[] anode, int offset, int length)
    {
    	super(anode, offset, length);
    }
    
	public Edge(Edge fromEdge)
	{
		super(fromEdge);
		this.source=fromEdge.source;
		this.label=fromEdge.label;
		this.destination=fromEdge.destination;
		this.weight=fromEdge.weight;
	}
	public Edge(Tuple tp) {
		if(tp!=null){
			this.data= tp.data;
			try {
				this.label = Convert.getStrValue(0, this.data, Edge.LABEL_MAX_LENGTH+2);
				NID srcId = new NID();
				srcId.pageNo.pid = Convert.getIntValue(Edge.LABEL_MAX_LENGTH+2, this.data);
				srcId.slotNo=Convert.getIntValue(Edge.LABEL_MAX_LENGTH+2+4, this.data);
				this.source = srcId;
				NID destId = new NID();
				destId.pageNo.pid = Convert.getIntValue(Edge.LABEL_MAX_LENGTH+2+4+4, this.data);
				destId.slotNo=Convert.getIntValue(Edge.LABEL_MAX_LENGTH+2+4+4+4, this.data);
				this.destination=destId;
				this.weight=Convert.getIntValue(Edge.LABEL_MAX_LENGTH+2+4+4+4+4, this.data);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	public String getLabel()
	{
		return label;
	}
	public int getWeight()
	{
		return weight;
	}
	public NID getSource()
	{
		return source;
	
	}
	public NID getDestination()
	{
		return destination;
	}
	public Edge setLabel(String label)
	{
		this.label=getFixedLengthLable(label);
		 try {
			Convert.setStrValue(this.label, 0, data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		tuple_length = getEdgeLength();
		return this;
	}
	private int getEdgeLength() {
		// lable (length +2) + (src.pageId+src.slotNo)+(dest.pageId+dest.slotNo)+ weight
		return (LABEL_MAX_LENGTH+2) + 4+4 +4+4 +4;
	}
	public Edge setWeight(int Weight)
	{
		this.weight=Weight;
		 try {
				Convert.setIntValue(weight, LABEL_MAX_LENGTH+2+4+4+4+4, data);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			tuple_length = getEdgeLength();
		return this;
	}
	public Edge setSource(NID sourceID)
	{
		this.source=sourceID;
		 try {
			 	sourceID.pageNo.writeToByteArray(data, LABEL_MAX_LENGTH+2);
				Convert.setIntValue(sourceID.slotNo, LABEL_MAX_LENGTH+2+4, data);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			tuple_length = getEdgeLength();
		return this;
	}
	public Edge setDestination(NID destID )
	{
		destination=destID;
		try {
			destination.pageNo.writeToByteArray(data, LABEL_MAX_LENGTH+2+4+4);
			Convert.setIntValue(destination.slotNo, LABEL_MAX_LENGTH+2+4+4+4, data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return this;
	}
	public byte[] getEdgeByteArray()
	{
		 return getTupleByteArray();
	}
	public void print(AttrType type[]) throws IOException
	{
		  System.out.print("[");
		  	System.out.print("edge label : "+this.label);
		  	System.out.print("source : slotNo : "+this.source.slotNo +", pageNo :"+this.source.pageNo);
		  	System.out.print("Destination : slotNo : "+this.destination.slotNo +", pageNo :"+this.destination.pageNo);
		  	System.out.print("weight : "+this.weight);
		  	System.out.println("]");
	}
	public void edgeCopy(Node fromNode)
	{
		 byte [] temparray = fromNode.getNodeByteArray();
	     System.arraycopy(temparray, 0, data, tuple_offset, tuple_length);  
		
	}
	public void edgeInit(byte[] aEdge, int offset)
	{
		 tupleInit(aEdge, offset, aEdge.length);
	
	}
	public void edgeSet(byte[] fromEdge, int offset)
	{
		tupleSet(fromEdge, offset, fromEdge.length);
	     
	}
}
