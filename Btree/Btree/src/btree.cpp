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
#include "exceptions/page_not_pinned_exception.h"


//#define DEBUG

namespace badgerdb
{
	LeafNodeInt *BTreeIndex::CreateLeafNode(PageId &newPageId) {
		Page* newNode;
		bufMgr->allocPage(file,newPageId, newNode);
		((LeafNodeInt*) newNode)->numKeys = 0;
  		return (LeafNodeInt*) newNode;
	}

	NonLeafNodeInt *BTreeIndex::CreateNonLeafNode(PageId &newPageId) {
		Page *newNode;
		bufMgr->allocPage(file, newPageId,newNode);
		((NonLeafNodeInt*) newNode)->numKeys = 0;
		((NonLeafNodeInt*) newNode)->level = 0;
		return (NonLeafNodeInt*) newNode;
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
		strcpy(indexMetaInfo.relationName,relationName.c_str());
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
		LeafNodeInt* node = CreateLeafNode(indexMetaInfo.rootPageNo);

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
			bufMgrIn = bufMgr;
			//LeafNodeInt* leaf = findLeafNode(1,indexMetaInfo.rootPageNo);
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
		Page* page;
		bufMgr->readPage(file,indexMetaInfo.rootPageNo,page);

		int keyInt = *(int*)key;

		RecordId currRid = rid;

		// Case: the root is a leaf (B+ tree has one node)
		if(indexMetaInfo.isLeaf == true){
			// In this case, we can safely cast the root page to a LeafNodeInt
			LeafNodeInt* root = (LeafNodeInt*) page;
			// This is a strange place to set the root's parent pointer but it works
			root->parent = -1;
			std::cout << "KeyInt: " << keyInt << std::endl;
			std::cout << "rid: " << rid.page_number << "-" << rid.slot_number << std::endl;
			std::cout << "root->keyArray: " << root->keyArray << std::endl;
			std::cout << "root->numKeys: " << root->numKeys << std::endl;
			std::cout << "root->ridArray: " << root->ridArray << std::endl;
			// Case: The root node is full during the insert
			// perform leaf node split
			if(root->numKeys == INTARRAYLEAFSIZE){
				splitLeafNode(keyInt,rid, indexMetaInfo.rootPageNo);
				indexMetaInfo.isLeaf = false;
			}
			else if (root->numKeys == 0) {
				root->keyArray[0] = keyInt;
				root->ridArray[0] = rid;
				root->numKeys = 1;
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
			//cause errors--taking out
			try {
				bufMgr->unPinPage(file,indexMetaInfo.rootPageNo,true);
			}
			catch (PageNotPinnedException e) {

			}
		}
		// Case: the root is not a leaf (i.e. tree level > 1)
		else{
			// keep track of root node
			//NonLeafNodeInt* root = (NonLeafNodeInt*) &page;
			// returns the leaf node where the data goes
			LeafNodeInt* node = findLeafNode(keyInt, indexMetaInfo.rootPageNo);

			Page* nodePage = (Page *) node;

			// Case: leaf node is full
			// perform leaf node split
			if(node->numKeys == INTARRAYLEAFSIZE){
				splitLeafNode(keyInt,rid,foundLeafPageNo);
			}
			else if (node->numKeys == 0) {
				node->keyArray[0] = keyInt;
				node->ridArray[0] = rid;
				node->numKeys = 1;
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

				bufMgr->unPinPage(file,foundLeafPageNo,true);
			}
			try {
				bufMgr->unPinPage(file,indexMetaInfo.rootPageNo,true);
			}
			catch (PageNotPinnedException e) {

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
	const void BTreeIndex::splitLeafNode(int key, const RecordId rid,  PageId pageNo){
		// cast node being split into a leaf node struct
		Page* bufMgrPage;
		bufMgr->readPage(file,pageNo,bufMgrPage);
		Page* page = bufMgrPage;//&file->readPage(pageNo);
		LeafNodeInt* node = (LeafNodeInt*) page;

		int keyInt = key;
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
		//key and rid were never entered!
		if (offset == 0) {
			arr1[node->numKeys] = keyInt;
			arr2[node->numKeys] = currRid;
		}

		// splitIndex is the index of the median key, and it points to
		// the first element in the new node
		int splitIndex = (node->numKeys + 1) / 2;

		// create the new node, a sibling page to the right of "node"
		PageId newPageNo;
		LeafNodeInt* newNode = CreateLeafNode(newPageNo);
		std::cout << "hello" << std::endl;
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
		oldNode->rightSibPageNo = newPageNo;
		// give newNode a parent
		newNode->parent = oldNode->parent;

		// At this point, we have two leaf nodes
		// the split index needs to be inserted into the parent
		//
		// Case: oldNode was the root (and also a leaf)
		if (oldNode->parent == -1) {
			// create a new NonLeafNode
			PageId newRootPageNo;
			NonLeafNodeInt* newRoot = CreateNonLeafNode(newRootPageNo);
			// set the info that makes it a root
			newRoot->parent = -1;
			newRoot->level = 1;
			std::cout << "Printing newRootPageNo: " << newRootPageNo << std::endl;
			indexMetaInfo.rootPageNo = newRootPageNo;
			// set each child's parent field
			newNode->parent = newRootPageNo;
			oldNode->parent = newRootPageNo;

			// insert new key and children pageNo's
			newRoot->keyArray[0] = arr1[splitIndex];
			newRoot->numKeys = 1;
			newRoot->pageNoArray[0] = pageNo;
			newRoot->pageNoArray[1] = newPageNo;

			bufMgr->unPinPage(file,pageNo,true);

			bufMgr->unPinPage(file,newPageNo,true);

			bufMgr->unPinPage(file,newRootPageNo,true);
		}
		// Case: oldNode was NOT the root
		else{
			// call out parent
			PageId parentPageId = oldNode->parent;
			Page* bufMgrPage;
			bufMgr->readPage(file,parentPageId,bufMgrPage);
			NonLeafNodeInt* parent = (NonLeafNodeInt*)bufMgrPage;//&file->readPage(parentPageId);
			// Case: parent has space for new key
			if (parent->numKeys < INTARRAYNONLEAFSIZE) {
				// create variable for PageNo
				//PageId* newPageNo = (PageId*) newNode;
				// perform a bubble insert of the key
				PageId tempNewPageNo = newPageNo;

				for (int i = 0; i < parent->numKeys; i++){

					if (keyInt < parent->keyArray[i]) {
						int key_temp = parent->keyArray[i];
						PageId page_temp = parent->pageNoArray[i];

						parent->keyArray[i] = keyInt;
						parent->pageNoArray[i] = tempNewPageNo;

						keyInt = key_temp;
						tempNewPageNo = page_temp;
					}

				}
				parent->keyArray[parent->numKeys] = keyInt;
				PageId lostPage = parent->pageNoArray[parent->numKeys];

				parent->pageNoArray[parent->numKeys] = tempNewPageNo;

				// PageId page_temp = parent->pageNoArray[parent->numKeys];
				// parent->pageNoArray[parent->numKeys] = newPageNo;
				parent->pageNoArray[parent->numKeys+1] = lostPage;
				parent->numKeys++;
				parent->level = 1;

				bufMgr->unPinPage(file,pageNo,true);

				bufMgr->unPinPage(file,newPageNo,true);

				bufMgr->unPinPage(file,parentPageId,true);
			}
			// Case: parent doesn't have space for new key
			else{
				int parentKey = arr1[splitIndex];//&parent->keyArray[splitIndex];
				splitNonLeafNode(parentKey,parentPageId, newPageNo);

				bufMgr->unPinPage(file,pageNo,true);

				bufMgr->unPinPage(file,newPageNo,true);

				bufMgr->unPinPage(file,parentPageId,true);
			}

		}
	}

	//--------------------------------------------------------------------
	// @brief	splitNonLeafNode is used for splitting a node
	// 		that is not a leaf node. Revolutionary!
	// key:		the key that causes overflow
	// pageNo:	something
	//--------------------------------------------------------------------
	const void BTreeIndex::splitNonLeafNode(int key, PageId parentPageNo, PageId previousNewPageNo) {
		// cast node being split into a leaf node struct
		Page* bufMgrPage;
		bufMgr->readPage(file,parentPageNo,bufMgrPage);
		Page* page = bufMgrPage;//&file->readPage(pageNo);
		NonLeafNodeInt* node = (NonLeafNodeInt*) page;

		int keyInt = key;

		// initialize temporary arrays for key and rid storage
		// size = num of records in full array + 1 being added
		int arr1[INTARRAYNONLEAFSIZE+1];
		PageId arr2[INTARRAYNONLEAFSIZE+2];
		// following code block inserts everything into arr1[] and arr2[]
		int offset = 0;
		for(int i = 0; i < node->numKeys; i++){
			if(keyInt < node->keyArray[i] && offset == 0){
				arr1[i] = keyInt;
				arr2[i] = previousNewPageNo;
				offset = 1;
				i--;
			}
			else{
				arr1[i+offset] = node->keyArray[i];
				arr2[i+offset] = node->pageNoArray[i];
			}
		}
		//key and rid were never entered!
		if (offset == 0) {
			arr1[node->numKeys] = keyInt;
			arr2[node->numKeys] = previousNewPageNo;
		}
		//get the last item in pageNoArray, add it to last space in arr2
		arr2[node->numKeys+1] = node->pageNoArray[node->numKeys]; //MAYBE SWITCH TO INTARRAYNONLEAFSIZE

		// splitIndex is the index of the median key, and it points to
		// the first element in the new node
		int splitIndex = (node->numKeys + 1) / 2;

		// create the new node, a sibling page to the right of "node"
		PageId newPageNo;
		NonLeafNodeInt* newNode = CreateNonLeafNode(newPageNo);
		NonLeafNodeInt* oldNode = node;

		// set numKeys of each node to proper value
		int numKeysNewNode = (INTARRAYNONLEAFSIZE + 1) - splitIndex;
		newNode->numKeys = numKeysNewNode;
		oldNode->numKeys = splitIndex;

		// fill entries of newNode arrays
		for (int i = 0; i < numKeysNewNode; i++){
			newNode->keyArray[i] = arr1[splitIndex + i];
			newNode->pageNoArray[i] = arr2[splitIndex + i];
		}
		newNode->pageNoArray[numKeysNewNode] = arr2[splitIndex+numKeysNewNode];


		// give newNode a parent
		newNode->parent = oldNode->parent;

		// At this point, we have two non-leaf nodes
		// the split index needs to be inserted into the parent
		//
		// Case: oldNode was the root (and also not a leaf)
		if (oldNode->parent == -1) {
			// create a new NonLeafNode
			PageId newRootPageNo;
			NonLeafNodeInt* newRoot = CreateNonLeafNode(newRootPageNo);
			// set the info that makes it a root
			newRoot->parent = -1;
			newRoot->level = 0;
			indexMetaInfo.rootPageNo = newRootPageNo;
			// set each child's parent field
			newNode->parent = newRootPageNo;
			oldNode->parent = newRootPageNo;

			// insert new key and children pageNo's
			newRoot->keyArray[0] = arr1[splitIndex];
			newRoot->pageNoArray[0] = parentPageNo;
			newRoot->pageNoArray[1] = newPageNo;
			newRoot->numKeys = 1;

			bufMgr->unPinPage(file,parentPageNo,true);

			bufMgr->unPinPage(file,newPageNo,true);

			bufMgr->unPinPage(file,newRootPageNo,true);
		}
		// Case: oldNode was NOT the root
		else{
			// call out parent
			PageId parentPageId = oldNode->parent;
			Page* bufMgrPage;
			bufMgr->readPage(file,parentPageId,bufMgrPage);
			NonLeafNodeInt* parent = (NonLeafNodeInt*)bufMgrPage;//(NonLeafNodeInt*)&file->readPage(parentPageId);
			// Case: parent has space for new key
			if (parent->numKeys < INTARRAYNONLEAFSIZE) {
				// create variable for PageNo
				//PageId* newPageNo = (PageId*) newNode;
				// perform a bubble insert of the key
				for (int i = 0; i < parent->numKeys; i++){
					if (keyInt < parent->keyArray[i]) {
						int key_temp = parent->keyArray[i];
						PageId page_temp = parent->pageNoArray[i];

						parent->keyArray[i] = keyInt;
						parent->pageNoArray[i] = newPageNo;

						keyInt = key_temp;
						newPageNo = page_temp;
					}
				}
				parent->keyArray[parent->numKeys] = keyInt;
				PageId page_temp = parent->pageNoArray[parent->numKeys];
				parent->pageNoArray[parent->numKeys] = newPageNo;
				parent->pageNoArray[parent->numKeys+1] = page_temp;
				parent->numKeys++;
				parent->level = 0;

				bufMgr->unPinPage(file,parentPageNo,true);

				bufMgr->unPinPage(file,newPageNo,true);

				bufMgr->unPinPage(file,parentPageId,true);
			}
			// Case: parent doesn't have space for new key
			else{
				int parentKey = arr1[splitIndex];//&parent->keyArray[splitIndex];
				splitNonLeafNode(parentKey,parentPageId,newPageNo);

				bufMgr->unPinPage(file,parentPageNo,true);

				bufMgr->unPinPage(file,newPageNo,true);

				bufMgr->unPinPage(file,parentPageId,true);
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
	LeafNodeInt* BTreeIndex::findLeafNode(int key, PageId pageNo){
		Page* bufMgrPage;
		bufMgr->readPage(file,pageNo,bufMgrPage);
		//Page* page = bufMgrPage; //&file->readPage(pageNo);
		NonLeafNodeInt* node = (NonLeafNodeInt*) bufMgrPage;

		std::cout << "node level: " << node->level << std::endl;

		if(node->level == 1){
			for(int i = 0; i < node->numKeys; i++){
				if(key < node->keyArray[i]){
					Page* new_bufMgrPage;
					bufMgr->readPage(file,node->pageNoArray[i],new_bufMgrPage);
					foundLeafPageNo = node->pageNoArray[i];
					//bufMgr->unPinPage(file, pageNo, false);
					return (LeafNodeInt*)new_bufMgrPage;//&file->readPage(node->pageNoArray[i]);
				}
			}
			Page* new_bufPage;
			bufMgr->readPage(file,node->pageNoArray[node->numKeys],new_bufPage);
			foundLeafPageNo = node->pageNoArray[node->numKeys];
			//bufMgr->unPinPage(file, pageNo, false);
			return (LeafNodeInt*)new_bufPage;//&file->readPage(node->pageNoArray[node->numKeys]);
		}
		//else, node level must be 0
		else if(node->level == 0) {
			for(int i = 0; i < node->numKeys; i++){
				if(key < node->keyArray[i]){
					bufMgr->unPinPage(file, pageNo, false);
					return findLeafNode(key, node->pageNoArray[i]);
				}
			}
			//bufMgr->unPinPage(file, pageNo, false);
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
		std::cout << "Reached start scan pre-exception check"<< std::endl;
		//throw necessary exceptions given bad input
		if(*(int*)lowValParm > *(int*)highValParm){
			std::cout << "Throwing Bad Scan Range Exception: lowValParm|highValParm" << *(int*)lowValParm << "|" << *(int*)highValParm<< std::endl;
			throw BadScanrangeException();
		}
		if(lowOpParm != GT  && lowOpParm != GTE){
			std::cout << "Throwing Bad OPCODES Exception"<< std::endl;
			throw BadOpcodesException();
		}
		if(highOpParm != LT && highOpParm != LTE){
			std::cout << "Throwing Bad OPCODES Exception"<< std::endl;
			throw BadOpcodesException();
		}

		std::cout << "Reached start scan (no badOpcodes or bad scanrange)"<< std::endl;

		scanExecuting = true;
		lowValInt = *((int*) lowValParm);
		highValInt = *((int*) highValParm);
		// lowOp = lowOpParm;
		// highOp = highOpParm;

		//std::cout << "Successfully set the global scan variables: " << "LowVal|HighVal|LowOp|HighOp -" << lowValInt << "|" << highValInt << "|" << lowOpParm << "|" << highOpParm << "|"<< std::endl;

		if(lowOpParm == GT){
			LeafNodeInt* currPage = findLeafNode(lowValInt + 1, indexMetaInfo.rootPageNo);
			currentPageData = (Page*) currPage;
			std::cout << "b4 current page number"<< std::endl;
			currentPageNum = foundLeafPageNo;
			std::cout << "after current page number"<< std::endl;
			std::cout << "currPage->numKeys"<< currPage->numKeys << std::endl;
			for(int i = 0; i < currPage->numKeys; i++){
				std::cout << "Setting next entry" << std::endl;
				if(currPage->keyArray[i] > lowValInt){
					nextEntry = i;
					std::cout << "NextEntry set to: " << nextEntry << std::endl;
					break;
				}
			}
		}
		else if(lowOpParm == GTE){
			LeafNodeInt* currPage = findLeafNode(lowValInt, indexMetaInfo.rootPageNo);
			currentPageData = (Page*) currPage;
			currentPageNum = foundLeafPageNo;//((Page*) currPage)->page_number();
			for(int i = 0; i < currPage->numKeys; i++){
				if(currPage->keyArray[i] >= lowValInt){
					nextEntry = i;
					std::cout << "NextEntry set to: " << nextEntry << std::endl;
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
		std::cout << "Entered ScanNext()" << nextEntry << std::endl;

		LeafNodeInt* currentLeafNode = (LeafNodeInt*)currentPageData;

		//if the next entry is larger than the keys in the array, a new page is needed
		if (nextEntry == currentLeafNode->numKeys) {
			bufMgr->unPinPage(file,currentPageNum,true);
			currentPageNum = currentLeafNode->rightSibPageNo;
			Page* bufMgrPage;
			bufMgr->readPage(file,currentLeafNode->rightSibPageNo,bufMgrPage);
			currentPageData = bufMgrPage; // &file->readPage(currentLeafNode->rightSibPageNo);
			currentLeafNode = (LeafNodeInt*) currentPageData;
			nextEntry = 0;
		}
		if (currentLeafNode == NULL) {
			throw IndexScanCompletedException();
		}

		int currentKey = currentLeafNode->keyArray[nextEntry];

		//get the next recordId on the current page
		if (highOp == LT) {
			if (currentKey < highValInt) {
				outRid = currentLeafNode->ridArray[nextEntry];
				nextEntry++;
			}
			else {
				throw IndexScanCompletedException();
			}
		}
		else if (highOp == LTE) {
			if (currentKey <= highValInt) {
				outRid = currentLeafNode->ridArray[nextEntry];
				nextEntry++;
			} else {
				throw IndexScanCompletedException();
			}
		}

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
