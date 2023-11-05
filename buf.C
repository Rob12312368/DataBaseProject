#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}


const Status BufMgr::allocBuf(int & frame) 
{
    unsigned int start = clockHand;
    int times = 0;
    Status status = OK;
    while(true)
    {
        advanceClock();
        if (bufTable[clockHand].valid)
        {
            if (bufTable[clockHand].refbit)
            {
                bufTable[clockHand].refbit = false;
            }
            else
            {
                if (bufTable[clockHand].pinCnt <= 0)
                {
                    if (bufTable[clockHand].dirty)
                    {
                        //flush page to disk
                        status = bufTable[clockHand].file->writePage(bufTable[clockHand].pageNo,&(bufPool[clockHand])); 
                        if(status != OK)
                            break;                  
                    }
                    hashTable->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo);
                    frame = clockHand;
                    //bufTable[clockHand].Set(NULL, frame);
                    break;
                }
            }
        }
        else
        {
            frame = clockHand;
            //bufTable[clockHand].Set(NULL, frame);
            break;
        }
        if (times == 2)
        {
            //loop two times already, all pinned
            status = BUFFEREXCEEDED;
            break;
        }
        if (clockHand == start)
        {
            times+=1;
        }
    }
    //only return point
    return status;
}

	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    Status status;
    int ans;
    status = hashTable->lookup(file, PageNo, ans);
    if(status == OK)
    {
        bufTable[ans].refbit = true;
        bufTable[ans].pinCnt += 1;
        Page* p = &(bufPool[ans]);
        page = p;
    }
    else{
        int frame;
        status = allocBuf(frame);
        if (status != OK)
        {
            cout << "allocfail";
            return status;
        }
        status = file->readPage(PageNo, &bufPool[frame]); //got wrong here, the second parameter
        if (status != OK)
            return status;
        status = hashTable->insert(file,PageNo,frame);
        if (status != OK)
            return status;
        bufTable[frame].Set(file, PageNo);
        Page* p = &(bufPool[frame]);
        page = p;
    }
    return status;
}


const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
    Status status;
    int frame;
    status = hashTable->lookup(file, PageNo, frame);
    if(status != OK)
        return status;
    if(bufTable[frame].pinCnt == 0)
    {
        status = PAGENOTPINNED;
        return status;
    }
    bufTable[frame].pinCnt -= 1;
    if (dirty == true)
    {
        bufTable[frame].dirty = true;
    }
    return OK;
}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    Status status;
    status = file->allocatePage(pageNo);
    if(status != OK)
    {
        return status;
    }
    int frame;
    status = allocBuf(frame);
    if(status != OK)
    {
        return status;
    }
    //cout << "here" << endl;
    //cout << pageNo << " " << frame << endl;
    status = hashTable->insert(file, pageNo, frame);
    if(status != OK)
    {
        cout << "NOT OK3!!!";
        return status;
    }

    bufTable[clockHand].Set(file, pageNo);

    page = &bufPool[frame];
    return status;





}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


