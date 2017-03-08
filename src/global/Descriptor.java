package global;

public class Descriptor {
    
    private int[] value;
    	
    	public Descriptor() {
        	value = new int[5];
    		value[0] = 0;
    	    value[1] = 0;
    	    value[2] = 0;
    	    value[3] = 0;
    	    value[4] = 0;
    	}
    
    public void set(int value0,int value1, int value2, int value3, int value4) {
        value[0] = value0;
        value[1] = value1;
        value[2] = value2;
        value[3] = value3;
        value[4] = value4;
    }
    
    public int get(int idx) {
        return value[idx];
    }
    
    //return 1 if equal, 0 if not
    public double equal(Descriptor desc) {
        return (value[0]==desc.value[0] && value[1]==desc.value[1] &&
        			value[2]==desc.value[2] && value[3]==desc.value[3] &&
        			value[4]==desc.value[4]) ? 1 : 0;
    }
    
    // This method returns the euclidean distance between the descriptors
    public double distance (Descriptor desc) {
       
    		double v1 = Math.pow(value[0] - desc.value[0],2);
    		double v2 = Math.pow(value[1] - desc.value[1],2);
    		double v3 = Math.pow(value[2] - desc.value[2],2);
    		double v4 = Math.pow(value[3] - desc.value[3],2);
    		double v5 = Math.pow(value[4] - desc.value[4],2);
    		
    		return Math.sqrt(v1+v2+v3+v4+v5);
    }
}
    