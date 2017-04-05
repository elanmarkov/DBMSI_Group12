package heap;

import java.io.IOException;

import global.AttrType;
import global.Convert;
import global.NID;

public class Edge extends Tuple {
	private String label;
	private String sourceLabel;
	private String destinationLabel;
	private NID source;
	private NID destination;
	private int weight;
	public static final AttrType[] types = { new AttrType(AttrType.attrString), new AttrType(AttrType.attrInteger),
			new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger),
			new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrString), new AttrType(AttrType.attrString) };
	public static final short[] sizes = { LABEL_MAX_LENGTH, 4,4,4,4,4, LABEL_MAX_LENGTH, LABEL_MAX_LENGTH };
	public static final short numFld = 8;

	public Edge() {
		super();
		try {
			setHdr(numFld, types, sizes);
		} catch (InvalidTypeException | InvalidTupleSizeException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public Edge(byte[] anode, int offset) {
		super(anode, offset, anode.length);
		try {
			setHdr(numFld, types, sizes);
		} catch (InvalidTypeException | InvalidTupleSizeException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Edge(byte[] anode, int offset, int length) {
		super(anode, offset, length);
		try {
			setHdr(numFld, types, sizes);
		} catch (InvalidTypeException | InvalidTupleSizeException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Edge(Edge fromEdge) {
		super(fromEdge);
		try {
			setHdr(numFld, types, sizes);
		} catch (InvalidTypeException | InvalidTupleSizeException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.source = fromEdge.source;
		this.label = fromEdge.label;
		this.destination = fromEdge.destination;
		this.weight = fromEdge.weight;
	}

	public Edge(Tuple tp) {
		try {
			setHdr(numFld, types, sizes);
		} catch (InvalidTypeException | InvalidTupleSizeException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (tp != null) {
			this.data = tp.data;
			/*try {
				this.label = Convert.getStrValue(0, this.data, Edge.LABEL_MAX_LENGTH + 2);
				NID srcId = new NID();
				srcId.pageNo.pid = Convert.getIntValue(Edge.LABEL_MAX_LENGTH + 2, this.data);
				srcId.slotNo = Convert.getIntValue(Edge.LABEL_MAX_LENGTH + 2 + 4, this.data);
				this.source = srcId;
				NID destId = new NID();
				destId.pageNo.pid = Convert.getIntValue(Edge.LABEL_MAX_LENGTH + 2 + 4 + 4, this.data);
				destId.slotNo = Convert.getIntValue(Edge.LABEL_MAX_LENGTH + 2 + 4 + 4 + 4, this.data);
				this.destination = destId;
				this.weight = Convert.getIntValue(Edge.LABEL_MAX_LENGTH + 2 + 4 + 4 + 4 + 4, this.data);
			} catch (IOException e) {
				e.printStackTrace();
			}*/
		}
	}

	public String getLabel() {
		try {
			this.label = getStrFld(1);
		} catch (FieldNumberOutOfBoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return label;
	}

	public int getWeight() {
		try {
			this.weight = getIntFld(6);
		} catch (FieldNumberOutOfBoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return weight;
	}

	public String getSourceLabel() {
		try {
			this.sourceLabel = getStrFld(7);
		} catch (FieldNumberOutOfBoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return sourceLabel;
	}

	public String getDestinationLabel() {
		try {
			this.destinationLabel = getStrFld(8);
		} catch (FieldNumberOutOfBoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return destinationLabel;
	}

	public NID getSource() {
		source = new NID();
		try {
			source.pageNo.pid = getIntFld(2);
			source.slotNo = getIntFld(3);

		} catch (FieldNumberOutOfBoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return source;

	}

	public NID getDestination() {
		destination = new NID();
		try {
			destination.pageNo.pid = getIntFld(4);
			destination.slotNo = getIntFld(5);

		} catch (FieldNumberOutOfBoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return destination;
	}

	public Edge setLabel(String label) {
		this.label = getFixedLengthLable(label);
		try {
			setStrFld(1, this.label);
		} catch (FieldNumberOutOfBoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/*
		 * try { Convert.setStrValue(this.label, 0, data); } catch (IOException
		 * e) { // TODO Auto-generated catch block e.printStackTrace(); }
		 * tuple_length = getEdgeLength();
		 */
		return this;
	}

	public Edge setSourceLabel(String label) {
		this.sourceLabel = getFixedLengthLable(label);
		try {
			setStrFld(7, this.sourceLabel);
		} catch (FieldNumberOutOfBoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/*
		 * try { Convert.setStrValue(this.label, 0, data); } catch (IOException
		 * e) { // TODO Auto-generated catch block e.printStackTrace(); }
		 * tuple_length = getEdgeLength();
		 */
		return this;
	}

	public Edge setDestinationLabel(String label) {
		this.destinationLabel = getFixedLengthLable(label);
		try {
			setStrFld(8, this.destinationLabel);
		} catch (FieldNumberOutOfBoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/*
		 * try { Convert.setStrValue(this.label, 0, data); } catch (IOException
		 * e) { // TODO Auto-generated catch block e.printStackTrace(); }
		 * tuple_length = getEdgeLength();
		 */
		return this;
	}

	private int getEdgeLength() {
		// lable (length +2) +
		// (src.pageId+src.slotNo)+(dest.pageId+dest.slotNo)+ weight
		return (LABEL_MAX_LENGTH + 2) + 4 + 4 + 4 + 4 + 4;
	}

	public Edge setWeight(int Weight) {
		this.weight = Weight;
		/*
		 * try { Convert.setIntValue(weight, LABEL_MAX_LENGTH+2+4+4+4+4, data);
		 * } catch (IOException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); } tuple_length = getEdgeLength();
		 */
		try {
			setIntFld(6, this.weight);
		} catch (FieldNumberOutOfBoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return this;
	}

	public Edge setSource(NID sourceID) {
		this.source = sourceID;
		/*
		 * try { sourceID.pageNo.writeToByteArray(data, LABEL_MAX_LENGTH+2);
		 * Convert.setIntValue(sourceID.slotNo, LABEL_MAX_LENGTH+2+4, data); }
		 * catch (IOException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); } tuple_length = getEdgeLength();
		 */
		try {
			setIntFld(2, this.source.pageNo.pid);
			setIntFld(3, this.source.slotNo);
		} catch (FieldNumberOutOfBoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return this;
	}

	public Edge setDestination(NID destID) {
		destination = destID;
		try {
			setIntFld(4, this.destination.pageNo.pid);
			setIntFld(5, this.destination.slotNo);
		} catch (FieldNumberOutOfBoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/*
		 * try { destination.pageNo.writeToByteArray(data,
		 * LABEL_MAX_LENGTH+2+4+4); Convert.setIntValue(destination.slotNo,
		 * LABEL_MAX_LENGTH+2+4+4+4, data); } catch (IOException e) { // TODO
		 * Auto-generated catch block e.printStackTrace(); }
		 */
		return this;
	}

	public byte[] getEdgeByteArray() {
		return getTupleByteArray();
	}

	public void print(AttrType type[]) throws IOException {
		System.out.print("[");
		System.out.print("edge label : " + this.getLabel());
		System.out.print("source : slotNo : " + this.getSource().slotNo + ", pageNo :" + this.getSource().pageNo);
		System.out.print("Destination : slotNo : " + this.getDestination().slotNo + ", pageNo :" + this.getDestination().pageNo);
		System.out.print("weight : " + this.getWeight());
		System.out.println("]");
	}

	public void edgeCopy(Node fromNode) {
		byte[] temparray = fromNode.getNodeByteArray();
		System.arraycopy(temparray, 0, data, tuple_offset, tuple_length);

	}

	public void edgeInit(byte[] aEdge, int offset) {
		tupleInit(aEdge, offset, aEdge.length);

	}

	public void edgeSet(byte[] fromEdge, int offset) {
		tupleSet(fromEdge, offset, fromEdge.length);

	}
}
