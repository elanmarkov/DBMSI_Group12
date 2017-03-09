package zindex;

import java.io.IOException;

import btree.AddFileEntryException;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.ConvertException;
import btree.DeleteFashionException;
import btree.DeleteRecException;
import btree.FreePageException;
import btree.GetFileEntryException;
import btree.IndexFile;
import btree.IndexFileScan;
import btree.IndexFullDeleteException;
import btree.IndexInsertRecException;
import btree.IndexSearchException;
import btree.InsertException;
import btree.InsertRecException;
import btree.IteratorException;
import btree.KeyClass;
import btree.KeyNotMatchException;
import btree.KeyTooLongException;
import btree.LeafDeleteException;
import btree.LeafInsertRecException;
import btree.LeafRedistributeException;
import btree.NodeNotMatchException;
import btree.PinPageException;
import btree.RecordNotFoundException;
import btree.RedistributeException;
import btree.UnpinPageException;
import catalog.Utility;
import global.AttrType;
import global.RID;

public class ZCurve extends IndexFile{

	public static int KEY_SIZE = 80;
	private String fileName;
	private BTreeFile btree;
	
	public ZCurve(String fileName) throws GetFileEntryException, ConstructPageException, AddFileEntryException, IOException{
		this.fileName = fileName;
		this.btree = new BTreeFile(this.fileName, AttrType.attrString, KEY_SIZE, 0);
	}
	
	public IndexFileScan newZFileScan(KeyClass lowKey, KeyClass hiKey) throws KeyNotMatchException, IteratorException, ConstructPageException, PinPageException, UnpinPageException, IOException{
		return new ZFileScan(btree, lowKey, hiKey);
	}
	
	public IndexFileScan ZFileRangeScan(KeyClass key, int distance) throws KeyNotMatchException, IteratorException, ConstructPageException, PinPageException, UnpinPageException, IOException{
		return new ZFileScan(btree, key, distance);
	}

	@Override
	public void insert(KeyClass data, RID rid) throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException,
			IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException,
			NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException,
			LeafDeleteException, InsertException, IOException {
		// TODO Auto-generated method stub
		KeyClass key = Utility.conver5Dto1D(data);
		this.btree.insert(key, rid);
	}

	@Override
	public boolean Delete(KeyClass data, RID rid)
			throws DeleteFashionException, LeafRedistributeException, RedistributeException, InsertRecException,
			KeyNotMatchException, UnpinPageException, IndexInsertRecException, FreePageException,
			RecordNotFoundException, PinPageException, IndexFullDeleteException, LeafDeleteException, IteratorException,
			ConstructPageException, DeleteRecException, IndexSearchException, IOException {
		// TODO Auto-generated method stub
		KeyClass key = Utility.conver5Dto1D(data);
		return this.btree.Delete(key, rid);
	}
	
	
	
}
