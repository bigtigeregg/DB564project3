/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }
  bufPool = new Page[bufs];
	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table
  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
//	// flushes out all dirty pages and deallocates the buffer pool and bufdesc table
//	// Deallocate Buffer DescTable
	delete[] bufDescTable;
	delete[] bufPool;
	delete hashTable;
}

void BufMgr::advanceClock()
{
	clockHand = (clockHand + 1) % numBufs;
}

/* Allocates a free frame using the clock algorithm;
 * if necessary, writing a dirty page back to disk.
 * Throws BufferExceededException if all buffer frames are pinned.
 * This private method will get called by the readPage() and allocPage() methods
 * described below. Make sure that if the buffer frame allocated has a valid page in it,
 * you remove the appropriate entry from the hash table.*/

	/**
	 * Allocate a free frame.
	 *
	 * @param frame   	Frame reference, frame ID of allocated frame returned via this variable
	 * @throws BufferExceededException If no such buffer is found which can be allocated
	 */


void BufMgr::allocBuf(FrameId & frame)
{
	for (FrameId i = 0; i <= numBufs; i++) {
		advanceClock();
		if (bufDescTable[clockHand].valid == false) {
			frame = clockHand;
			return;
		}else{
			if (bufDescTable[clockHand].refbit == true) {
				bufDescTable[clockHand].refbit = false;
				continue;
			} else {
				if (bufDescTable[clockHand].pinCnt > 0) {
					continue;
				} else {
					if(bufDescTable[clockHand].dirty == true) {
						bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
					}
					hashTable->remove(bufDescTable[clockHand].file,bufDescTable[clockHand].pageNo);
					bufDescTable[clockHand].Clear();
					frame = clockHand;
					return;
				}
			}
		}

	}
	throw BufferExceededException();
}


	/**
 * Reads the given page from the file into a frame and returns the pointer to page.
 * If the requested page is already present in the buffer pool pointer to that frame is returned
 * otherwise a new frame is allocated from the buffer pool for reading the page.
 *
 * @param file   	File object
 * @param PageNo  Page number in the file to be read
 * @param page  	Reference to page pointer. Used to fetch the Page object in which requested page from file is read in.
 */
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	FrameId retFrameID;
	try {
		hashTable->lookup(file, pageNo, retFrameID);
		bufDescTable[retFrameID].pinCnt++;
	}catch(HashNotFoundException){
		// isNotFound
		allocBuf(retFrameID);
		bufPool[retFrameID] = file->readPage(pageNo);
		hashTable->insert(file,pageNo,retFrameID);
		bufDescTable[retFrameID].Set(file,pageNo);
	}
	bufDescTable[retFrameID].refbit = true;
	page = &bufPool[retFrameID];
}


	/**
	 * Unpin a page from memory since it is no longer required for it to remain in memory.
	 *
	 * @param file   	File object
	 * @param PageNo  Page number
	 * @param dirty		True if the page to be unpinned needs to be marked dirty
   * @throws  PageNotPinnedException If the page is not already pinned
	 */

void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	FrameId retFrameID;
	try {
		hashTable->lookup(file, pageNo, retFrameID);
		if(bufDescTable[retFrameID].pinCnt == 0){
			throw PageNotPinnedException(file->filename(),pageNo,retFrameID);
		}
		bufDescTable[retFrameID].dirty = true;
		bufDescTable[retFrameID].pinCnt--;
		return;
	}catch(HashNotFoundException){
		return;
	}
}
	/**
	 * Writes out all dirty pages of the file to disk.
	 * All the frames assigned to the file need to be unpinned from buffer pool before this function can be successfully called.
	 * Otherwise Error returned.
	 *
	 * @param file   	File object
   * @throws  PagePinnedException If any page of the file is pinned in the buffer pool
   * @throws BadBufferException If any frame allocated to the file is found to be invalid
	 */
void BufMgr::flushFile(const File* file) 
{

	for (FrameId i = 0; i < numBufs; i++) {
		if (bufDescTable[i].file == file) {
			if(bufDescTable[i].pinCnt >0){
				throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, i);
			}
			if (bufDescTable[i].dirty == true) {
				bufDescTable[i].file->writePage(bufPool[i]);
				bufDescTable[i].dirty = false;
			}
			// remove the page from the hashtable
			hashTable->remove(file, bufDescTable[i].pageNo);
			bufDescTable[i].Clear();
		}
	}
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	FrameId newFrameId;
	PageId newPageId = file->allocatePage().page_number();
	allocBuf(newFrameId);
	hashTable->insert(file,newPageId,newFrameId);
	bufPool[newFrameId] = file->readPage(newPageId);
	bufDescTable[newFrameId].Set(file,newPageId);
	pageNo = newPageId;
	page = &bufPool[newFrameId];

}

	/**
	 * Delete page from file and also from buffer pool if present.
	 * Since the page is entirely deleted from file, its unnecessary to see if the page is dirty.
	 *
	 * @param file   	File object
	 * @param PageNo  Page number
	 */
void BufMgr::disposePage(File* file, const PageId PageNo)
{
    FrameId retFrameId;
	try {
		hashTable->lookup(file,PageNo,retFrameId);
		bufDescTable[retFrameId].Clear();
		hashTable->remove(file,PageNo);
	}catch (HashNotFoundException){

	}
	file->deletePage(PageNo);

}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
