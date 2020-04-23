/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{
	LeafNodeInt *BTreeIndex::CreateLeafNode(PageId &newPageId) {
		LeafNodeInt* newNode;
		bufMgr->allocPage(file,newPageId,(Page *&)newNode);
		newNode.num_keys = 1;
  		return newNode;
	}

	NonLeafNodeInt *BTreeIndex::CreateNonLeafNode(PageId &newPageId) {
		NonLeafNodeInt *newNode;
		bufMgr->allocPage(file, newPageId,(Page *&)newNode);
		newNode.num_keys = 1;
		return newNode;
	}
	// -----------------------------------------------------------------------------
	// BTreeIndex::BTreeIndex -- Constructor
	// -----------------------------------------------------------------------------

	BTreeIndex::BTreeIndex(const std::string & relationName,
			std::string & outIndexName,
			BufMgr *bufMgrIn,
			const int _attrByteOffset,
			const Datatype attrType)
	{
		//sets the relation name (code copied from pp3.pdf)
		std::ostringstream idxStr;
		idxStr << relationName << '.' << attrByteOffset;
		outIndexName = idxStr.str();

		//sets the information for the indexMetaInfo (first page of the index file)
		strcpy(indexMetaInfo.relationName,relationName.c_str);
		indexMetaInfo.attrByteOffset = attrByteOffset;
		indexMetaInfo.attrType = attrType;
		indexMetaInfo.isLeaf = true; //root is a leaf

		//sets btree variables based on input variables
		bufMgr = bufMgrIn;
		attrByteOffset = _attrByteOffset;
		attributeType = attrType;
		rootPageNum = indexMetaInfo.rootPageNo

		//creates a new BlobFile using the indexName
		file = new BlobFile(outIndexName, true);

		//creates a leaf node for the root of the index
		CreateLeafNode(indexMetaInfo.rootPageNo);
		
		//unPins the page that was pinned to create the leaf node
  		bufMgr->unPinPage(file, indexMetaInfo.rootPageNo, true);

		//creates a file scanner used to iterate through the record ids
		FileScan fileScanner(relationName, bufMgr);

		try {
			RecordId scanRid;
			while (true) {
				//gets the next record Id
				fileScanner.scanNext(scanRid);
				//gets the record using the record id
				std::string recordStr = fileScanner.getRecord();
				const char *record = recordStr.c_str();
				//creates the key using the record and the byte offset
				int key = *((int *)(record + attrByteOffset));
				//inserts the entry into the index
				insertEntry(&key, scanRid);
			}
		} catch (EndOfFileException e) {
		}
	}


	// -----------------------------------------------------------------------------
	// BTreeIndex::~BTreeIndex -- destructor
	// -----------------------------------------------------------------------------

	BTreeIndex::~BTreeIndex()
	{
		//ends any ongoing scans and flushes the file
		if (scanExecuting) endScan();
		bufMgr->flushFile(file);
		delete file;
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::insertEntry
	// -----------------------------------------------------------------------------

	const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
	{
		Page* page = file.ReadPage(rootPageNum);

		if(indexMetaInfo.isLeaf == true){
			LeafNodeInt* root = (LeafNodeInt*) page;
			root.parent = NULL;
			if(numKeys == INTARRAYLEAFSIZE){
				split( //NEEDS MORE
			}
			else{
				for(int i = 0; i < node.numKeys; i ++){
					if(key < root.keyArray[i]){
						int key_temp = root.keyArray[i];
						RecordId rid_temp = root.ridArray[i];

						root.keyArray[i] = key;
						root.ridArray[i] = rid;
						
						key = key_temp;
						rid = rid_temp;
					}
				}
				root.numKeys++;
			}
		}
		else{
			NonLeafNodeInt* root = (NonLeafNodeInt*) page;
			LeafNodeInt* = findLeafNode(key, rootPageNum);
			if(numKeys == INTARRAYLEAFSIZE){
				splitLeafNode( //NEEDS MORE
			}
			else{
				for(int i = 0; i < node.numKeys; i ++){
					if(key < root.keyArray[i]){
						int key_temp = root.keyArray[i];
						RecordId rid_temp = root.ridArray[i];

						root.keyArray[i] = key;
						root.ridArray[i] = rid;
						
						key = key_temp;
						rid = rid_temp;
					}
				}
				root.numKeys++;
			}

		}
	}

	const void splitLeafNode(const void *key, const RecordId rid, PageId* pageNo){
		Page* page = file.ReadPage(pageNo);
		LeafNodeInt* node = (LeafNodeInt*) page;
		int arr1[INTARRAYLEAFSIZE+1];
		RecordId arr2[INTARRAYLEAFSIZE+1];
		int offset = 0;
		for(int i = 0; i < node.numKeys; i++){
			if(key < node.keyArray[i] && offset == 0){
				arr1[i] = key;
				arr2[i] = rid;
				offset = 1;
				i--;
			}
			else{
				arr1[i+offset] = node.keyArray[i];
				arr2[i+offset] = node.ridArray[i];
			}
		}
		int splitIndex = (numkeys + 1) / 2

	}

	const LeafNodeInt* BTreeIndex::findLeafNode(const void *key, PageId* pageNo){
		Page* page = file.ReadPage(pageNo);
		NonLeafNodeInt* node = (NonLeafNodeInt*) page;
		int i;
		if(node.level == 1){
			for(i = 0; i < node.numKeys - 1; i++){
				if(key < node.keyArray[i]){
					return file.ReadPage(node.pageNoArray[i]);
				}
			}
			return file.ReadPage(node.pageNoArray[node.numKeys])
		}
		else if(node.level == 0){
			for(i = 0; i < node.numKeys - 1; i++){
				if(key < node.keyArray[i]){
					return findLeafNode(key, file.ReadPage(node.pageNoArray[i]));
				}
			}
			return findLeafNode(key, node.pageNoArray[node.numKeys])
		}
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::startScan
	// -----------------------------------------------------------------------------

	const void BTreeIndex::startScan(const void* lowValParm,
					const Operator lowOpParm,
					const void* highValParm,
					const Operator highOpParm)
	{

	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::scanNext
	// -----------------------------------------------------------------------------

	const void BTreeIndex::scanNext(RecordId& outRid) 
	{
		
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::endScan
	// -----------------------------------------------------------------------------
	//
	const void BTreeIndex::endScan() 
	{
		//throws ScanNotInitializedException if the scan hasn't been started yet
		if (!scanExecuting) throw ScanNotInitializedException();
		scanExecuting = false;
		//unpins the page associated with the scan
		bufMgr->unPinPage(file, currentPageNum, false);
	}

}
