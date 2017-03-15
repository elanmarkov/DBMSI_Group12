/*
Descriptor Key Class by _____
*/
package zindex;

import btree.KeyClass;
import global.Descriptor;

public class DescriptorKey extends KeyClass{

	private Descriptor key;

	public Descriptor getKey() {
		return key;
	}

	public void setKey(Descriptor key) {
		this.key = key;
	}

	public DescriptorKey(Descriptor key) {
		this.key = key;
	}

	@Override
	public String toString() {
		return "DesciptorKey [key=" + key + "]";
	}
	
}
