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
		newNode->numKeys = 1;
  		return newNode;
	}

	NonLeafNodeInt *BTreeIndex::CreateNonLeafNode(PageId &newPageId) {
		NonLeafNodeInt *newNode;
		bufMgr->allocPage(file, newPageId,(Page *&)newNode);
		newNode->numKeys = 1;
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
		rootPageNum = indexMetaInfo.rootPageNo;

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
		// Begin searching for the place to insert at the root
		Page* page = &file->readPage(indexMetaInfo.rootPageNo);

		int keyInt = (int)key;

		RecordId currRid = rid;

		// Case: the root is a leaf (B+ tree has one node)
		if(indexMetaInfo.isLeaf == true){
			// In this case, we can safely cast the root page to a LeafNodeInt
			LeafNodeInt* root = (LeafNodeInt*) page;
			// This is a strange place to set the root's parent pointer but it works
			root->parent = NULL;
			
			// Case: The root node is full during the insert
			// perform leaf node split
			if(root->numKeys == INTARRAYLEAFSIZE){
				splitLeafNode(key,rid, indexMetaInfo.rootPageNo);
				indexMetaInfo.isLeaf = false;
			}
			
			// Case: root has space to insert
			// bubble insert the key and rid into their places
			else{
				for(int i = 0; i < root->numKeys; i ++){
					if(keyInt < root->keyArray[i]){
						int key_temp = root->keyArray[i];
						RecordId rid_temp = root->ridArray[i];

						root->keyArray[i] = keyInt;
						root->ridArray[i] = rid;
						
						keyInt = key_temp;
						currRid = rid_temp;
					}
				}
				// after loop, insert i = root.numKeys, and
				// key and rid need to be inserted at the end
				root->keyArray[root->numKeys] = keyInt;
				root->ridArray[root->numKeys] = currRid;
				// also, numKeys gets a new friend :)
				root->numKeys++;
			}
		}
		// Case: the root is not a leaf (i.e. tree level > 1)
		else{
			// keep track of root node
			NonLeafNodeInt* root = (NonLeafNodeInt*) page;
			// returns the leaf node where the data goes
			LeafNodeInt* node = findLeafNode(key, indexMetaInfo.rootPageNo);

			Page* nodePage = ((Page *&)node);

			// Case: leaf node is full
			// perform leaf node split 
			if(node->numKeys == INTARRAYLEAFSIZE){
				splitLeafNode(key,rid,nodePage->page_number());
			}

			// Case: leaf node has space
			// perform bubble insert of key and rid
			else{
				for(int i = 0; i < node->numKeys; i ++){
					if(keyInt < node->keyArray[i]){
						int key_temp = node->keyArray[i];
						RecordId rid_temp = node->ridArray[i];

						node->keyArray[i] = keyInt;
						node->ridArray[i] = currRid;
						
						keyInt = key_temp;
						currRid = rid_temp;
					}
				}
				// after insert, i = numKeys + 1 and key and rid need insertion
				node->keyArray[node->numKeys] = keyInt;
				node->ridArray[node->numKeys] = currRid;
				// numKeys makes a new friend
				node->numKeys++;
			}
		}
	}

	// -------------------------------------------------------------
	// @brief splitLeafNode performs the split of a leaf node into two
	// 	  when an insertion is performed on a full node
	// key:		the key being inserted
	// rid:		the rid being inserted
	// pageNo:	the pointer to the node being split
	// returns:	void
	// -------------------------------------------------------------
	const void BTreeIndex::splitLeafNode(const void *key, const RecordId rid,  PageId pageNo){
		// cast node being split into a leaf node struct
		Page* page = &file->readPage(pageNo);
		LeafNodeInt* node = (LeafNodeInt*) page;

		int keyInt = (int)key;
		RecordId currRid = rid;

		// initialize temporary arrays for key and rid storage
		// size = num of records in full array + 1 being added
		int arr1[INTARRAYLEAFSIZE+1];
		RecordId arr2[INTARRAYLEAFSIZE+1];
		// following code block inserts everything into arr1[] and arr2[]
		int offset = 0;
		for(int i = 0; i < node->numKeys; i++){
			if(keyInt < node->keyArray[i] && offset == 0){
				arr1[i] = keyInt;
				arr2[i] = currRid;
				offset = 1;
				i--;
			}
			else{
				arr1[i+offset] = node->keyArray[i];
				arr2[i+offset] = node->ridArray[i];
			}
		}

		// splitIndex is the index of the median key, and it points to
		// the first element in the new node
		int splitIndex = (node->numKeys + 1) / 2;

		// create the new node, a sibling page to the right of "node"
		PageId* newPageNo;
		LeafNodeInt* newNode = CreateLeafNode(*newPageNo);
		//unpin page ASAP
		// TODO: WHEN TO UNPIN PAGES ----- bufMgr->unPinPage(file, newPageNo, true);
		
		LeafNodeInt* oldNode = node;

		// set numKeys of each node to proper value
		int numKeysNewNode = (INTARRAYLEAFSIZE + 1) - splitIndex;
		newNode->numKeys = numKeysNewNode;
		oldNode->numKeys = splitIndex;

		// fill entries of newNode arrays
		for (int i = 0; i < numKeysNewNode; i++){
			newNode->keyArray[i] = arr1[splitIndex + i];
			newNode->ridArray[i] = arr2[splitIndex + i];
		}

		// update sibling pointers
		// newNode goes to the right of oldNode
		newNode->rightSibPageNo = oldNode->rightSibPageNo;
		oldNode->rightSibPageNo = (PageId)newNode;
		// give newNode a parent
		newNode->parent = oldNode->parent;

		// At this point, we have two leaf nodes
		// the split index needs to be inserted into the parent
		//
		// Case: oldNode was the root (and also a leaf)
		if (oldNode->parent == NULL) {
			// create a new NonLeafNode
			PageId* newRootPageNo;
			NonLeafNodeInt* newRoot = CreateNonLeafNode(*newRootPageNo);
			// set the info that makes it a root
			newRoot->parent = NULL;
			newRoot->level = 1;
			indexMetaInfo.rootPageNo = *newRootPageNo;
			// set each child's parent field
			newNode->parent = *newRootPageNo;
			oldNode->parent = *newRootPageNo;
			
			// insert new key and children pageNo's
			newRoot->keyArray[0] = arr1[splitIndex];
			newRoot->pageNoArray[0] = pageNo;
			newRoot->pageNoArray[1] = *newPageNo;
		}
		// Case: oldNode was NOT the root
		else{
			// call out parent
			PageId parentPageId = oldNode->parent;
			NonLeafNodeInt* parent = (NonLeafNodeInt*)&file->readPage(parentPageId);
			// Case: parent has space for new key
			if (parent->numKeys < INTARRAYNONLEAFSIZE) {
				// create variable for PageNo
				PageId* newPageNo = (PageId*) newNode;
				// perform a bubble insert of the key
				for (int i = 0; i < parent->numKeys; i++){

					if (keyInt < parent->keyArray[i]) {
						int key_temp = parent->keyArray[i];
						PageId* page_temp = &parent->pageNoArray[i];

						parent->keyArray[i] = keyInt;
						parent->pageNoArray[i] = *newPageNo;

						keyInt = key_temp;
						newPageNo = page_temp;
					}

				}
				parent->keyArray[parent->numKeys] = keyInt;
				PageId page_temp = parent->pageNoArray[parent->numKeys];
				parent->pageNoArray[parent->numKeys] = *newPageNo;
				parent->pageNoArray[parent->numKeys+1] = page_temp;
				parent->numKeys++;
			}
			// Case: parent doesn't have space for new key
			else{
				int* parentKey = &parent->keyArray[splitIndex];
				splitNonLeafNode(parentKey,parentPageId);
			}

		}
	}

	//--------------------------------------------------------------------
	// @brief	splitNonLeafNode is used for splitting a node
	// 		that is not a leaf node. Revolutionary!
	// key:		the key that causes overflow
	// pageNo:	something
	//--------------------------------------------------------------------
	const void BTreeIndex::splitNonLeafNode(const void *key, PageId pageNo) {
		// cast node being split into a leaf node struct
		Page* page = &file->readPage(pageNo);
		NonLeafNodeInt* node = (NonLeafNodeInt*) page;

		int keyInt = (int)key;

		// initialize temporary arrays for key and rid storage
		// size = num of records in full array + 1 being added
		int arr1[INTARRAYNONLEAFSIZE+1];
		PageId* arr2[INTARRAYNONLEAFSIZE+2];
		// following code block inserts everything into arr1[] and arr2[]
		int offset = 0;
		for(int i = 0; i < node->numKeys; i++){
			if(keyInt < node->keyArray[i] && offset == 0){
				arr1[i] = keyInt;
				arr2[i] = &pageNo;
				offset = 1;
				i--;
			}
			else{
				arr1[i+offset] = node->keyArray[i];
				arr2[i+offset] = &node->pageNoArray[i];
			}
		}
		//get the last item in pageNoArray, add it to last space in arr2
		arr2[node->numKeys+1] = &node->pageNoArray[node->numKeys]; //MAYBE SWITCH TO INTARRAYNONLEAFSIZE

		// splitIndex is the index of the median key, and it points to
		// the first element in the new node
		int splitIndex = (node->numKeys + 1) / 2;

		// create the new node, a sibling page to the right of "node"
		PageId* newPageNo;
		NonLeafNodeInt* newNode = CreateNonLeafNode(*newPageNo);
		NonLeafNodeInt* oldNode = node;

		// set numKeys of each node to proper value
		int numKeysNewNode = (INTARRAYNONLEAFSIZE + 1) - splitIndex;
		newNode->numKeys = numKeysNewNode;
		oldNode->numKeys = splitIndex;

		// fill entries of newNode arrays
		for (int i = 0; i < numKeysNewNode; i++){
			newNode->keyArray[i] = arr1[splitIndex + i];
			newNode->pageNoArray[i] = *arr2[splitIndex + i];
		}
		newNode->pageNoArray[numKeysNewNode] = *arr2[splitIndex+numKeysNewNode];


		// give newNode a parent
		newNode->parent = oldNode->parent;

		// At this point, we have two non-leaf nodes
		// the split index needs to be inserted into the parent
		//
		// Case: oldNode was the root (and also not a leaf)
		if (oldNode->parent == NULL) {
			// create a new NonLeafNode
			PageId* newRootPageNo;
			NonLeafNodeInt* newRoot = CreateNonLeafNode(*newRootPageNo);
			// set the info that makes it a root
			newRoot->parent = NULL;
			newRoot->level = 0;
			indexMetaInfo.rootPageNo = *newRootPageNo;
			// set each child's parent field
			newNode->parent = *newRootPageNo;
			oldNode->parent = *newRootPageNo;
			
			// insert new key and children pageNo's
			newRoot->keyArray[0] = arr1[splitIndex];
			newRoot->pageNoArray[0] = pageNo;
			newRoot->pageNoArray[1] = *newPageNo;
		}
		// Case: oldNode was NOT the root
		else{
			// call out parent
			PageId parentPageId = oldNode->parent;
			NonLeafNodeInt* parent = (NonLeafNodeInt*)&file->readPage(parentPageId);
			// Case: parent has space for new key
			if (parent->numKeys < INTARRAYNONLEAFSIZE) {
				// create variable for PageNo
				PageId* newPageNo = (PageId*) newNode;
				// perform a bubble insert of the key
				for (int i = 0; i < parent->numKeys; i++){
					if (keyInt < parent->keyArray[i]) {
						int key_temp = parent->keyArray[i];
						PageId* page_temp = &parent->pageNoArray[i];

						parent->keyArray[i] = keyInt;
						parent->pageNoArray[i] = *newPageNo;

						keyInt = key_temp;
						newPageNo = page_temp;
					}
				}
				parent->keyArray[parent->numKeys] = keyInt;
				PageId page_temp = parent->pageNoArray[parent->numKeys];
				parent->pageNoArray[parent->numKeys] = *newPageNo;
				parent->pageNoArray[parent->numKeys+1] = page_temp;
				parent->numKeys++;
			}
			// Case: parent doesn't have space for new key
			else{
				int* parentKey = &parent->keyArray[splitIndex];
				splitNonLeafNode(parentKey,parentPageId);
			}

		}

	}

	//--------------------------------------------------------------------
	// @brief	findLeafNode traverses the tree downwards to find the
	// 		leaf node that fits the given key
	// key:		the key to insert
	// pageNo:	a NonLeafNodeInt* that will serve as the start of the search
	// returns:	the LeafNodeInt* where the key is in range
	//--------------------------------------------------------------------
	LeafNodeInt* BTreeIndex::findLeafNode(const void *key, PageId pageNo){
		Page* page = &file->readPage(pageNo);
		NonLeafNodeInt* node = (NonLeafNodeInt*) page;
		int i;
		int keyInt = (int)key;
		if(node->level == 1){
			for(i = 0; i < node->numKeys - 1; i++){
				if(keyInt < node->keyArray[i]){
					return (LeafNodeInt*)&file->readPage(node->pageNoArray[i]);
				}
			}
			return (LeafNodeInt*)&file->readPage(node->pageNoArray[node->numKeys]);
		}
		else if(node->level == 0){
			for(i = 0; i < node->numKeys - 1; i++){
				if(keyInt < node->keyArray[i]){
					return findLeafNode(key, node->pageNoArray[i]);
				}
			}
			return findLeafNode(key, node->pageNoArray[node->numKeys]);
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
		//throw necessary exceptions given bad input
		if(lowValParm > highValParm){
			throw new BadScanrangeException;
		}
		if(lowOpParm != 'GT' || lowOpParm != 'GTE'){
			throw new BadOpcodesException;
		}
		if(highOpParm != 'LT' || highOpParm != 'LTE'){
			throw new BadOpcodesException;
		}



		scanExecuting = true; 
		lowValInt = (int) lowValParm;
		highValInt = (int) highValParm;
		lowOp = lowOpParm;
		highOp = highOpParm;
		


		if(lowOpParm == 'GT'){
			LeafNodeInt* currPage = findLeafNode((const void*)(lowValInt + 1) , indexMetaInfo.rootPageNo);
			currentPageData = (Page*) currPage;
			currentPageNum = ((Page*) currPage)->page_number();
			for(int i = 0; i < currPage->numKeys; i++){
				if(currPage->keyArray[i] > lowValInt){
					nextEntry = i;
					break;
				}
			}
		}
		else if(lowOpParm == 'GTE'){
			LeafNodeInt* currPage = findLeafNode(lowValParm, indexMetaInfo.rootPageNo);
			currentPageData = (Page*) currPage;
			currentPageNum = ((Page*) currPage)->page_number();
			for(int i = 0; i < currPage->numKeys; i++){
				if(currPage->keyArray[i] >= lowValInt){
					nextEntry = i;
					break;
				}
			}
		}
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
		
		nextEntry = NULL;
		lowValInt = NULL;
		highValInt = NULL;
		// lowOp = NULL;
		// highOp = NULL;

	}

}
