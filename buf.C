#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)                                            \
  {                                                          \
    if (!(c)) {                                              \
      cerr << "At line " << __LINE__ << ":" << endl << "  "; \
      cerr << "This condition should hold: " #c << endl;     \
      exit(1);                                               \
    }                                                        \
  }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs) {
  numBufs = bufs;

  bufTable = new BufDesc[bufs];
  memset(bufTable, 0, bufs * sizeof(BufDesc));
  for (int i = 0; i < bufs; i++) {
    bufTable[i].frameNo = i;
    bufTable[i].valid = false;
  }

  bufPool = new Page[bufs];
  memset(bufPool, 0, bufs * sizeof(Page));

  int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
  hashTable = new BufHashTbl(htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}

BufMgr::~BufMgr() {
  // flush out all unwritten pages
  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &bufTable[i];
    if (tmpbuf->valid == true && tmpbuf->dirty == true) {
#ifdef DEBUGBUF
      cout << "flushing page " << tmpbuf->pageNo << " from frame " << i << endl;
#endif

      tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
    }
  }

  delete[] bufTable;
  delete[] bufPool;
}

const Status BufMgr::allocBuf(int& frame) {
  // the value of this variable `status` will be returned
  // we hope that it will not be changed into any value
  // initialize the status of the function as OK
  auto status = OK;

  // following the clock algorithm, advance clock first
  advanceClock();

  // we will be recording where we start to know when we completed a loop
  const auto initialClockHand = clockHand;

  // at most two loops of the clock can happen
  // any unpinned and not recently refered page will be returned in the first loop
  // any unpinned page with set refbit will have its refbit clearly in the first loop
  // thus, in the second loop, it will be returned
  // so if we cannot find a free buffer after two loops, we will have to resign
  for (auto i = 0; i < 2; i++)
    for (; clockHand != initialClockHand; advanceClock()) {
      // these are the pointers to the current buffer description and the current buffer page
      auto desc = bufTable + clockHand;
      auto page = bufPool + clockHand;

      // skip to the next loop if the buffer fails any tests described in the clock algorithm
      if (desc->valid) {
        if (desc->refbit) {
          desc->refbit = false;
          continue;
        } else if (desc->pinCnt) {
          continue;
        }
      }

      // if the buffer page is dirty, then we write it into the memory
      if (desc->dirty) status = desc->file->writePage(desc->pageNo, page);
      // then we remove the page from the hash table and memory
      if (status == OK) status = disposePage(desc->file, desc->pageNo);
      // finally, if everything goes all right, we can provide the freshly freed frame
      if (status == OK) frame = desc->frameNo;

      return status;
    }

  // if the execution reaches here, there must be no buffer that we can allocate
  // so an BUFFEREXCEEDED error is returned
  return BUFFEREXCEEDED;
}

const Status BufMgr::readPage(File* file, const int pageNo, Page*& page) {
    int frameNo = -1;
    Status status = OK;
    status = hashTable->lookup(file, pageNo, frameNo);
    // Case 1: page in the buffer
    if(status == OK){
        bufTable[frameNo]->refbit = true;
        bufTable[frameNo]->pinCnt += 1;
        page = bufPool[frameNo];
        return status;
    } else { // Case 2: page not in the buffer
        // allocate a buffer
        status = allocBuf(frameNo);
        if(status != OK) // No available frame
            return status;

        // read in the page
        status = file->readPage(pageNo, &bufPool[frameNo]);
        if(status != OK) { // Read file error
            if(status == UNIXERR){ // Bad input
                return status;
            } else {
                std::cout << "Bad input from BufMgr::readPage" << std::endl;
            }
        }

        // set hashtable
        status = hashTable->insert(file, pageNo, frameNo);
        if(status != OK) return status;

        // set buf desc
        bufTable[frameNo].Set(file, pageNo);
        page = &bufPool[frameNo];
    }
    return status;
}

const Status BufMgr::unPinPage(File* file, const int pageNo, const bool dirty) {
  // the value of this variable `status` will be returned
  // we hope that it will not be changed into any value
  // initialize the status of the function as OK
  auto status = OK;

  // look up the file and the page number in the hash table
  int frameNo = 0;
  status = hashTable->lookup(file, pageNo, frameNo);

  // decrement the frame if it is found
  if (status == OK) bufTable[frameNo].pinCnt--;
  // also, if parameter `dirty` is set, then the frame's dirty bit is set
  if (status == OK) bufTable[frameNo].dirty |= dirty;

  return status;
}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) {}

const Status BufMgr::disposePage(File* file, const int pageNo) {
  // see if it is in the buffer pool
  Status status = OK;
  int frameNo = 0;
  status = hashTable->lookup(file, pageNo, frameNo);
  if (status == OK) {
    // clear the page
    bufTable[frameNo].Clear();
  }
  status = hashTable->remove(file, pageNo);

  // deallocate it in the file
  return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) {
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {
      if (tmpbuf->pinCnt > 0) return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
        cout << "flushing page " << tmpbuf->pageNo << " from frame " << i << endl;
#endif
        if ((status = tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]))) != OK) return status;

        tmpbuf->dirty = false;
      }

      hashTable->remove(file, tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }

  return OK;
}

void BufMgr::printSelf(void) {
  BufDesc* tmpbuf;

  cout << endl << "Print buffer...\n";
  for (int i = 0; i < numBufs; i++) {
    tmpbuf = &(bufTable[i]);
    cout << i << "\t" << (char*)(&bufPool[i]) << "\tpinCnt: " << tmpbuf->pinCnt;

    if (tmpbuf->valid == true) cout << "\tvalid\n";
    cout << endl;
  };
}
