/*
Serialization utility by Elan Markov
Converts given object to byte array, or converts byte array to object.
Copied from: http://stackoverflow.com/questions/3736058/java-object-to-byte-and-byte-to-object-converter-for-tokyo-cabinet
*/
package global;
import java.io.*;

public class Serialize {
	public static byte[] serialize(Object obj) throws IOException {
    		ByteArrayOutputStream out = new ByteArrayOutputStream();
    		ObjectOutputStream os = new ObjectOutputStream(out);
    		os.writeObject(obj);
    		return out.toByteArray();
	}
	public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
    		ByteArrayInputStream in = new ByteArrayInputStream(data);
    		ObjectInputStream is = new ObjectInputStream(in);
    		return is.readObject();
	}


}
