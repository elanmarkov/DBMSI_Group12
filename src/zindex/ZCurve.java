package zindex;

/*
 * @(#) ZFileScan.java   
 *         Author: Jayanth Kumar M J
 *
 */

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
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import catalog.Utility;
import global.AttrType;
import global.RID;

/*
 * This classes uses creates a Z curve on multidimensional data.
 * Currently it supports 5 dimension data.
 * For indexing multidimensional data, it maps multidimensional 
 * data into single dimension.This data is then indexed using Btree file.
 * Currently key size is 80 bits(each dimension may have values from 0-10000)
 */

public class ZCurve extends IndexFile{

	public static int KEY_SIZE = 82;
	private String fileName;
	private BTreeFile btree;
	
	/**
	   *  if index file exists, open it; else create it.
	   *@param filename file name. Input parameter.
	   *@exception GetFileEntryException  can not get file
	   *@exception ConstructPageException page constructor failed
	   *@exception IOException error from lower layer
	   *@exception AddFileEntryException can not add file into DB
	   */
	
	public ZCurve(String fileName) throws GetFileEntryException, ConstructPageException, AddFileEntryException, IOException{
		this.fileName = fileName;
		this.btree = new BTreeFile(this.fileName, AttrType.attrString, KEY_SIZE, 0);
	}
	
	/** create a scan with given keys
	   * Cases:
	   *      (1) lo_key = null, hi_key = null
	   *              scan the whole index
	   *      (2) lo_key = null, hi_key!= null
	   *              range scan from min to the hi_key
	   *      (3) lo_key!= null, hi_key = null
	   *              range scan from the lo_key to max
	   *      (4) lo_key!= null, hi_key!= null, lo_key = hi_key
	   *              exact match ( might not unique)
	   *      (5) lo_key!= null, hi_key!= null, lo_key < hi_key
	   *              range scan from lo_key to hi_key
	   *@param lo_key the key where we begin scanning. Input parameter.
	   *@param hi_key the key where we stop scanning. Input parameter.
	   *@exception IOException error from the lower layer
	   *@exception KeyNotMatchException key is not string key
	   *@exception IteratorException iterator error
	   *@exception ConstructPageException error in BT page constructor
	   *@exception PinPageException error when pin a page
	   *@exception UnpinPageException error when unpin a page
	   */
	public IndexFileScan newZFileScan(KeyClass lowKey, KeyClass hiKey) throws KeyNotMatchException, IteratorException, ConstructPageException, PinPageException, UnpinPageException, IOException{
		return new ZFileScan(btree, lowKey, hiKey);
	}
	
	/** create a scan with given keys
	   *@param key : the key the center point of a Circle. Input parameter.
	   *@param distance : distance from the center point. Input parameter.
	   *@exception IOException error from the lower layer
	   *@exception KeyNotMatchException key is not string key
	   *@exception IteratorException iterator error
	   *@exception ConstructPageException error in BT page constructor
	   *@exception PinPageException error when pin a page
	   *@exception UnpinPageException error when unpin a page
	   */
	
	public IndexFileScan ZFileRangeScan(KeyClass key, int distance) throws KeyNotMatchException, IteratorException, ConstructPageException, PinPageException, UnpinPageException, IOException{
		return new ZFileScan(btree, key, distance);
	}

	/** insert record with the given key and nid
	   *@param data the key of the record(5D @Descriptor). Input parameter.
	   *@param rid the @NID of the record. Input parameter.
	   *@exception  KeyTooLongException key size exceeds the max keysize.
	   *@exception KeyNotMatchException key is not integer key nor string key
	   *@exception IOException error from the lower layer
	   *@exception LeafInsertRecException insert error in leaf page
	   *@exception IndexInsertRecException insert error in index page
	   *@exception ConstructPageException error in BT page constructor
	   *@exception UnpinPageException error when unpin a page
	   *@exception PinPageException error when pin a page
	   *@exception NodeNotMatchException  node not match index page nor leaf page
	   *@exception ConvertException error when convert between revord and byte 
	   *             array
	   *@exception DeleteRecException error when delete in index page
	   *@exception IndexSearchException error when search 
	   *@exception IteratorException iterator error
	   *@exception LeafDeleteException error when delete in leaf page
	   *@exception InsertException  error when insert in index page
	   */  
	@Override
	public void insert(KeyClass data, RID rid) throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException,
			IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException,
			NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException,
			LeafDeleteException, InsertException, IOException {
		// TODO Auto-generated method stub
		KeyClass key = Utility.conver5Dto1D(data);
		this.btree.insert(key, rid);
	}

	/** delete leaf entry  given its <key, nid> pair.
	   *  `nid' is IN the data entry; it is not the id of the data entry)
	   *@param data the key in pair <data, nid>. Input Parameter.
	   *@param rid the rid in pair <data, nid>. Input Parameter.
	   *@return true if deleted. false if no such record.
	   *@exception DeleteFashionException neither full delete nor naive delete
	   *@exception LeafRedistributeException redistribution error in leaf pages
	   *@exception RedistributeException redistribution error in index pages
	   *@exception InsertRecException error when insert in index page
	   *@exception KeyNotMatchException key is neither integer key nor string key
	   *@exception UnpinPageException error when unpin a page
	   *@exception IndexInsertRecException  error when insert in index page
	   *@exception FreePageException error in BT page constructor
	   *@exception RecordNotFoundException error delete a record in a BT page
	   *@exception PinPageException error when pin a page
	   *@exception IndexFullDeleteException  fill delete error
	   *@exception LeafDeleteException delete error in leaf page
	   *@exception IteratorException iterator error
	   *@exception ConstructPageException error in BT page constructor
	   *@exception DeleteRecException error when delete in index page
	   *@exception IndexSearchException error in search in index pages
	   *@exception IOException error from the lower layer
	   *
	   */
	
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
	
	public void close() {
		try {
		this.btree.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public String getFileName() {
		return this.fileName;
	}
	
	
	public void close(){
		try {
			this.btree.close();
		} catch (PageUnpinnedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidFrameNumberException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HashEntryNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ReplacerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
}
