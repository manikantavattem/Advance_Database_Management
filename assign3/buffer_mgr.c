#include "buffer_mgr.h"
#include "buffer_list.h"
#include "storage_mgr.h"
#include "dt.h"
#include <stdio.h>
#include <stdlib.h>

static EntryPointer entry_ptr_bp = NULL;
static long double time_uni = -32674;
static char *initFrames(const int numPages);
static Buffer_page_info *findReplace(BM_BufferPool *const bm, BufferPool_Entry *entry_bp);

RC initBufferPool(BM_BufferPool *const bm, const char *const pg_file_name, const int numPages, ReplacementStrategy strategy, void *stratData)
{
    BufferPool_Entry *existingEntry = checkPoolsUsingFile(entry_ptr_bp, pg_file_name);

    if (existingEntry != NULL) {
        bm->pageFile = pg_file_name;
        bm->numPages = numPages;
        bm->strategy = strategy;
        bm->mgmtData = existingEntry->buffer_pool_ptr;  // Ensure buffer_pool_ptr is a valid pointer to BM_BufferPool
        return insert_bufpool(&entry_ptr_bp, bm, existingEntry->buffer_page_info);
    }

    SM_FileHandle *fileHandle = malloc(sizeof(SM_FileHandle));
    if (!fileHandle) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    RC status = openPageFile(pg_file_name, fileHandle);
    if (status != RC_OK) {
        free(fileHandle);
        return status;
    }

    Buffer_page_info *pageInfos = calloc(numPages, sizeof(Buffer_page_info));
    if (!pageInfos) {
        free(fileHandle);
        return RC_MEMORY_ALLOCATION_FAIL;
    }

    for (int i = 0; i < numPages; i++) {
        pageInfos[i].pageframes = initFrames(numPages);
        pageInfos[i].fixcounts = 0;
        pageInfos[i].isdirty = FALSE;
        pageInfos[i].pagenums = NO_PAGE;
    }

    bm->pageFile = pg_file_name;
    bm->numPages = numPages;
    bm->strategy = strategy;
    bm->mgmtData = fileHandle;

    return insert_bufpool(&entry_ptr_bp, bm, pageInfos);
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
static char *initFrames(const int numPages)
{
    // Allocate and zero-initialize an array of PAGE_SIZE elements, each of size 1 byte.
    char *frame = (char *)calloc(PAGE_SIZE, sizeof(char));
    
    // The calloc function automatically sets the memory to zero,
    // so there is no need for a manual loop to initialize the memory.
    
    return frame;  // Return the allocated and initialized frame.
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
RC shutdownBufferPool(BM_BufferPool *const bm)
{
    int num_pools;
    Buffer_page_info *pg_info;
    BufferPool_Entry *buff_entry;
    char *frame;
    bool is_any_page_fixed = FALSE;

    // Check if any page is currently being used (fix count > 0)
    for (int i = 0; i < bm->numPages; i++) {
        int fix_count = getFixCounts(bm)[i];
        if (fix_count > 0) {
            is_any_page_fixed = TRUE;
            break;  // Break as soon as a fixed page is found
        }
    }

    // If no pages are fixed, proceed with shutdown
    if (!is_any_page_fixed) {
        buff_entry = find_bufferPool(entry_ptr_bp, bm);
        num_pools = getPoolsUsingFile(entry_ptr_bp, bm->pageFile);
        pg_info = buff_entry->buffer_page_info;

        for (int i = 0; i < bm->numPages; i++) {
            frame = pg_info[i].pageframes;
            if (pg_info[i].isdirty) {
                if (writeBlock(pg_info[i].pagenums, bm->mgmtData, frame) != RC_OK) {
                    return RC_WRITE_FAILED;
                }
            }
            if (num_pools == 1) {
                free(frame);  // Free memory if this is the last pool using this page
            }
        }

        if (num_pools == 1) {
            free(pg_info);  // Free the array of page info if this is the last pool
        }

        delete_bufpool(&entry_ptr_bp, bm);  // Remove the buffer pool from the pool list

        if (num_pools == 1) {
            closePageFile((SM_FileHandle *) bm->mgmtData);  // Close the file if this is the last pool
            free(bm->mgmtData);  // Free the management data
        }
    }

    return RC_OK;
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
RC forceFlushPool(BM_BufferPool *const bm)
{
    BufferPool_Entry *bufEntry = find_bufferPool(entry_ptr_bp, bm);
    if (bufEntry == NULL) {
        return RC_BUFFER_POOL_NOT_FOUND;  // Return an error if the buffer pool is not found
    }

    Buffer_page_info *pageInfo = bufEntry->buffer_page_info;

    for (int i = 0; i < bm->numPages; i++) {
        char *frame = pageInfo[i].pageframes;
        
        if (pageInfo[i].fixcounts == 0 && pageInfo[i].isdirty) {
            RC status = writeBlock(pageInfo[i].pagenums, bm->mgmtData, frame);
            if (status != RC_OK) {
                return RC_WRITE_FAILED;  // Return immediately if writing to disk fails
            }
            pageInfo[i].isdirty = FALSE;  // Mark the page as not dirty after writing to disk
            bufEntry->numwriteIO++;  // Increment the number of write operations
        }
    }
    return RC_OK;
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
PageNumber *getFrameContents(BM_BufferPool *const bm)
{
    EntryPointer bufferEntry = find_bufferPool(entry_ptr_bp, bm);
    if (bufferEntry == NULL) {
        return NULL; // Return NULL if the buffer pool entry is not found
    }

    // Allocate memory for storing page numbers
    PageNumber *pageNums = (PageNumber *)calloc(bm->numPages, sizeof(PageNumber));
    if (pageNums == NULL) {
        return NULL; // Return NULL if memory allocation fails
    }

    Buffer_page_info *bufferPgInfo = bufferEntry->buffer_page_info;
    
    // Iterate through the buffer pool and collect page numbers
    for (int i = 0; i < bm->numPages; i++) {
        pageNums[i] = bufferPgInfo[i].pagenums;
    }

    return pageNums; // Return the array of page numbers
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
int *getFixCounts(BM_BufferPool *const bm)
{
    EntryPointer bufferEntry = find_bufferPool(entry_ptr_bp, bm);
    if (bufferEntry == NULL || bufferEntry->buffer_page_info == NULL) {
        return NULL; // Return NULL if the buffer pool entry or page info is not found
    }

    int *fixCounts = (int *)calloc(bm->numPages, sizeof(int));
    if (fixCounts == NULL) {
        return NULL; // Return NULL if memory allocation fails
    }

    Buffer_page_info *bufferPgInfo = bufferEntry->buffer_page_info;
    for (int i = 0; i < bm->numPages; i++) {
        fixCounts[i] = bufferPgInfo[i].fixcounts;
    }

    return fixCounts; // Return the array of fix counts
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
bool *getDirtyFlags(BM_BufferPool *const bm)
{
    EntryPointer bufferEntry = find_bufferPool(entry_ptr_bp, bm);
    if (bufferEntry == NULL || bufferEntry->buffer_page_info == NULL) {
        return NULL; // Return NULL if the buffer pool entry or page info is not found
    }

    bool *dirtyFlags = (bool *)calloc(bm->numPages, sizeof(bool));
    if (dirtyFlags == NULL) {
        return NULL; // Return NULL if memory allocation fails
    }

    Buffer_page_info *bufferPgInfo = bufferEntry->buffer_page_info;
    for (int i = 0; i < bm->numPages; i++) {
        dirtyFlags[i] = bufferPgInfo[i].isdirty;
    }

    return dirtyFlags; // Return the array of dirty flags
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
int getNumReadIO (BM_BufferPool *const bm)
{
    EntryPointer buffer_entry = find_bufferPool(entry_ptr_bp,bm);
    return buffer_entry->numreadIO;
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
int getNumWriteIO (BM_BufferPool *const bm)
{    
    EntryPointer buffer_entry=find_bufferPool(entry_ptr_bp,bm);
    return buffer_entry->numwriteIO;
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
RC markDirty(BM_BufferPool * const bm, BM_PageHandle * const page) 
{
    BufferPool_Entry *pageEntry = find_bufferPool(entry_ptr_bp, bm);
    if (pageEntry == NULL || pageEntry->buffer_page_info == NULL) {
        return RC_BUFFER_POOL_NOT_FOUND;  // Appropriate error if buffer pool entry is not found
    }

    Buffer_page_info *bufferPgInfo = pageEntry->buffer_page_info;

    // Using a for loop for clarity and to tightly scope the index variable 'i'
    for (int i = 0; i < bm->numPages; i++) {
        if (bufferPgInfo[i].pagenums == page->pageNum) {
            bufferPgInfo[i].isdirty = TRUE;
            return RC_OK;
        }
    }

    return RC_MARK_DIRTY_FAILED;
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
RC forcePage(BM_BufferPool * const bm, BM_PageHandle * const page)
{
    BufferPool_Entry *entryBP = find_bufferPool(entry_ptr_bp, bm);

    // Ensure the buffer pool entry is valid
    if (entryBP == NULL) {
        return RC_BUFFER_POOL_NOT_FOUND; // Appropriate error if the buffer pool entry is not found
    }

    // Ensure management data is available before attempting to write
    if (bm->mgmtData == NULL) {
        return RC_FORCE_PAGE_ERROR; // Return error if management data is not initialized
    }

    // Attempt to write the block to disk
    RC status = writeBlock(page->pageNum, bm->mgmtData, page->data);
    if (status != RC_OK) {
        return status; // Propagate the error from writeBlock
    }

    // Increment the I/O write counter
    entryBP->numwriteIO++;

    return RC_OK;
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
RC unpinPage(BM_BufferPool * const bm, BM_PageHandle * const page) 
{
    BufferPool_Entry *entryBP = find_bufferPool(entry_ptr_bp, bm);
    if (entryBP == NULL || entryBP->buffer_page_info == NULL) {
        return RC_BUFFER_POOL_NOT_FOUND; // Return an error if the buffer pool or its page info is not found
    }

    Buffer_page_info *pageInfo = entryBP->buffer_page_info;

    // Use a for loop for better readability and scope management of the loop variable 'i'
    for (int i = 0; i < bm->numPages; i++) {
        if (pageInfo[i].pagenums == page->pageNum) {
            if (pageInfo[i].fixcounts > 0) {
                pageInfo[i].fixcounts--;
            }
            return RC_OK;
        }
    }

    return RC_UNPIN_FAILED; // Return error if no matching page number is found
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
RC pinPage(BM_BufferPool * const bm, BM_PageHandle * const page, const PageNumber pageNum) 
{
    BufferPool_Entry *entryBP = find_bufferPool(entry_ptr_bp, bm);
    if (entryBP == NULL || entryBP->buffer_page_info == NULL) {
        return RC_BUFFER_POOL_NOT_FOUND;
    }

    Buffer_page_info *pageInfo = entryBP->buffer_page_info;
    Buffer_page_info *repPossible = NULL;

    // First loop to find if the page is already pinned
    for (int i = 0; i < bm->numPages; i++) {
        if (pageInfo[i].pagenums == pageNum) {
            pageInfo[i].timeStamp = time_uni++; // Update the timestamp for LRU
            pageInfo[i].fixcounts++;
            page->pageNum = pageNum;
            page->data = pageInfo[i].pageframes;
            return RC_OK;
        }
    }

    // Second loop to find an empty slot or determine replacement needed
    for (int i = 0; i < bm->numPages; i++) {
        if (pageInfo[i].pagenums == NO_PAGE) {
            repPossible = &pageInfo[i];
            break;
        }
    }

    if (repPossible != NULL) {
        // Handle empty slot available
        page->pageNum = pageNum;
        page->data = repPossible->pageframes;

        RC readStatus = readBlock(pageNum, bm->mgmtData, repPossible->pageframes);
        if (readStatus != RC_OK) {
            if (readStatus == RC_READ_NON_EXISTING_PAGE || readStatus == RC_OUT_OF_BOUNDS) {
                readStatus = appendEmptyBlock(bm->mgmtData);
            }
            return readStatus;
        }

        entryBP->numreadIO++;
        repPossible->fixcounts = 1;
        repPossible->pagenums = pageNum;
        return RC_OK;
    } else {
        // Apply replacement strategy
        if (bm->strategy == RS_FIFO) 
            return applyFIFO(bm, page, pageNum);
        else if (bm->strategy == RS_LRU) 
            return applyLRU(bm, page, pageNum);
        else if (bm->strategy == RS_LFU)
            return applyLFU(bm, page, pageNum);
        else 
            return RC_PIN_FAILED; 
    }
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
RC applyFIFO(BM_BufferPool * const bm, BM_PageHandle * const page, const PageNumber pageNum) 
{
    BufferPool_Entry *entry_bp;
    entry_bp = find_bufferPool(entry_ptr_bp, bm);
    Buffer_page_info *rep_possible = NULL;
   
    rep_possible = findReplace(bm, entry_bp);
    
    RC write_ok = RC_OK;
    RC read_ok = RC_OK;
    
    if (rep_possible->isdirty == TRUE) 
    {
        write_ok = writeBlock(rep_possible->pagenums, bm->mgmtData, rep_possible->pageframes);
        rep_possible->isdirty = FALSE;
        entry_bp->numwriteIO++;
    }
    
    read_ok = readBlock(pageNum, bm->mgmtData, rep_possible->pageframes);
    if((read_ok == RC_READ_NON_EXISTING_PAGE) || (read_ok == RC_OUT_OF_BOUNDS) || (read_ok == RC_READ_FAILED))
        read_ok = appendEmptyBlock(bm->mgmtData);

    entry_bp->numreadIO++;
    page->pageNum  = pageNum;
    page->data = rep_possible->pageframes;
    rep_possible->pagenums = pageNum;
    rep_possible->fixcounts = rep_possible->fixcounts + 1;
    rep_possible->weight = rep_possible->weight + 1;

    if((read_ok == RC_OK) && (write_ok == RC_OK))
        return RC_OK;
    else
        return RC_FIFO_FAILED;
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
RC applyLRU(BM_BufferPool * const bm, BM_PageHandle * const page, const PageNumber pageNum) 
{
    BufferPool_Entry *entryBP = find_bufferPool(entry_ptr_bp, bm);
    if (entryBP == NULL || entryBP->buffer_page_info == NULL) {
        return RC_BUFFER_POOL_NOT_FOUND; // Return error if buffer pool or page info is not found
    }

    Buffer_page_info *repPossible = findReplace(bm, entryBP);
    if (repPossible == NULL) {
        return RC_LRU_FAILED; // Return error if no replacement candidate is found
    }

    RC status = RC_OK;

    // Write the page to disk if it is dirty
    if (repPossible->isdirty) {
        status = writeBlock(repPossible->pagenums, bm->mgmtData, repPossible->pageframes);
        if (status != RC_OK) {
            return status; // Propagate the error from writeBlock
        }
        repPossible->isdirty = FALSE;
        entryBP->numwriteIO++;
    }

    // Read the new page into the buffer
    status = readBlock(pageNum, bm->mgmtData, repPossible->pageframes);
    if (status != RC_OK) {
        return status; // Return error if read fails
    }

    // Update management fields
    entryBP->numreadIO++;
    repPossible->pagenums = pageNum;
    repPossible->fixcounts++;
    repPossible->weight++; // Increment weight assuming it may be used for LFU
    repPossible->timeStamp = (long double)time_uni++; // Update LRU timestamp

    // Set the page handle to return to the client
    page->pageNum = pageNum;
    page->data = repPossible->pageframes;

    return RC_OK; // If all operations succeed, return OK
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
RC applyLFU(BM_BufferPool * const bm, BM_PageHandle * const page, const PageNumber pageNum)
{
    BufferPool_Entry *entryBP = find_bufferPool(entry_ptr_bp, bm);
    if (entryBP == NULL || entryBP->buffer_page_info == NULL) {
        return RC_BUFFER_POOL_NOT_FOUND; // Return error if buffer pool or page info is not found
    }

    Buffer_page_info *repPossible = findReplace(bm, entryBP);
    if (repPossible == NULL) {
        return RC_LFU_FAILED; // Return error if no replacement candidate is found
    }

    RC writeStatus = RC_OK;
    if (repPossible->isdirty) {
        writeStatus = writeBlock(repPossible->pagenums, bm->mgmtData, repPossible->pageframes);
        if (writeStatus != RC_OK) {
            return writeStatus; // Propagate the error from writeBlock
        }
        repPossible->isdirty = FALSE;
        entryBP->numwriteIO++;
    }

    RC readStatus = readBlock(pageNum, bm->mgmtData, repPossible->pageframes);
    if (readStatus != RC_OK) {
        if (readStatus == RC_READ_NON_EXISTING_PAGE || readStatus == RC_OUT_OF_BOUNDS) {
            readStatus = appendEmptyBlock(bm->mgmtData);
            if (readStatus != RC_OK) {
                return readStatus; // Handle potential error from appending an empty block
            }
        } else {
            return readStatus; // Propagate other read errors
        }
    }

    entryBP->numreadIO++;
    repPossible->pagenums = pageNum;
    repPossible->fixcounts++;
    repPossible->weight++; // Increment weight for LFU tracking
    page->pageNum = pageNum;
    page->data = repPossible->pageframes;

    return RC_OK; // If all operations succeed, return OK
}
//--------------------------------------------------------------------------------------------------------------------------------------------------
Buffer_page_info *findReplace(BM_BufferPool *const bm, BufferPool_Entry *entry_bp) 
{
    Buffer_page_info *bf_page_info = entry_bp->buffer_page_info;
    Buffer_page_info *replace_bf_page_info = NULL;

    // Find the first un-fixed page as a potential candidate
    for (int i = 0; i < bm->numPages; i++) {
        if (bf_page_info[i].fixcounts == 0) {
            replace_bf_page_info = &bf_page_info[i];
            break;
        }
    }

    // If no un-fixed page found, return NULL
    if (replace_bf_page_info == NULL) {
        return NULL;
    }

    // Iterate through the buffer to find the most suitable replacement page
    for (int i = 0; i < bm->numPages; i++) {
        if (bf_page_info[i].fixcounts == 0) {
            switch (bm->strategy) {
                case RS_FIFO:
                case RS_LFU:
                    // For FIFO and LFU, select the least frequently used page
                    if (bf_page_info[i].weight < replace_bf_page_info->weight) {
                        replace_bf_page_info = &bf_page_info[i];
                    }
                    break;
                case RS_LRU:
                    // For LRU, select the least recently used page
                    if (bf_page_info[i].timeStamp < replace_bf_page_info->timeStamp) {
                        replace_bf_page_info = &bf_page_info[i];
                    }
                    break;
            }
        }
    }

    return replace_bf_page_info;
}
//--------------------------------------------------------------------------------------------------------------------------------------------------s