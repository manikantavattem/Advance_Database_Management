#include "buffer_mgr.h"
#include <stdio.h>
#include <stdlib.h>
#include "dberror.h"
#include "storage_mgr.h"

// Initializes a buffer pool for an existing page file
typedef struct BM_BufferPool BM_BufferPool;
typedef struct BM_PageHandle BM_PageHandle;
typedef enum ReplacementStrategy ReplacementStrategy;
#define RC_OK 0
#define RC_FILE_NOT_FOUND 1
#define NO_PAGE -1

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData) {
    // Use a block to limit the scope of 'fileExists'
    bool fileExists = false;
    {
        FILE *file = fopen(pageFileName, "r+");
        if (file) {
            fclose(file);
            fileExists = true;
        }
    }

    if (!fileExists) {
        return RC_FILE_NOT_FOUND;
    }

    bm->pageFile = (char *)pageFileName; // Consider copying the string to avoid potential issues with external changes
    bm->numPages = numPages;
    bm->strategy = strategy;

    // Direct allocation and initialization using calloc
    bm->mgmtData = calloc(numPages, sizeof(BM_PageHandle));
    if (!bm->mgmtData) {
        return RC_MEMORY_ALLOCATION_FAIL; // Define this error code for memory allocation failures
    }

    // Initialize page handles
    for (int i = 0; i < numPages; ++i) {
        BM_PageHandle *pageHandle = &((BM_PageHandle*)bm->mgmtData)[i];
        pageHandle->dirty = 0;
        pageHandle->fixCounts = 0;
        pageHandle->data = NULL;
        pageHandle->pageNum = NO_PAGE;
    }

    // Initialize IO counters
    bm->buffertimer = 0;
    bm->numberReadIO = 0;
    bm->numberWriteIO = 0;

    return RC_OK;
}


// Shuts down the buffer pool
RC shutdownBufferPool(BM_BufferPool *const bm) {
    int *fixCnt = getFixCounts(bm);
    for (int i = 0; i < bm->numPages; i++) {
        if (fixCnt[i] != 0) { // Check if any page is still pinned
            free(fixCnt); // Free fix counts array
            return RC_SHUTDOWN_POOL_FAILED; // Cannot shutdown if any page is pinned
        }
    }
    free(fixCnt); // Now we can free fixCnt here, removing the need to free it in multiple places

    // Attempt to flush dirty pages back to disk
    RC flushStatus = forceFlushPool(bm);
    if (flushStatus != RC_OK) {
        return flushStatus; // Propagate error from forceFlushPool
    }

    // If we reach here, flushing was successful, so we can clean up
    freePagesForBuffer(bm); // Assuming this function correctly frees individual page data
    free(bm->mgmtData); // Free the buffer pool management data
    bm->mgmtData = NULL;

    return RC_OK;
}

// Forces the buffer pool to write all dirty pages back to disk
RC forceFlushPool(BM_BufferPool *const bm) {
    bool *dFlg = getDirtyFlags(bm);
    int *fixCnt = getFixCounts(bm);
    RC RCflg = RC_OK;

    for (int i = 0; i < bm->numPages; i++) {
        if (dFlg[i] && fixCnt[i] == 0) { // Check for dirty pages with no fix counts
            BM_PageHandle *page = &bm->mgmtData[i];
            RCflg = forcePage(bm, page);
            if (RCflg != RC_OK) {
                break; // Exit on first error encountered
            }
            page->dirty = 0; // Mark page as clean after successful write
        }
    }

    free(dFlg);
    free(fixCnt);
    return RCflg;
}

// Marks a page as dirty
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page) {
    BM_PageHandle *pageArray = (BM_PageHandle*)bm->mgmtData; // Cast mgmtData to the correct type
    for (int i = 0; i < bm->numPages; i++) {
        if (pageArray[i].pageNum == page->pageNum) {
            page->dirty = 1; // Mark the provided page handle as dirty
            pageArray[i].dirty = 1; // Also mark the corresponding page in the pool as dirty
            return RC_OK;
        }
    }
    return RC_OK; // Consider returning a different code if page not found
}


// Unpins a page from the buffer pool
RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    BM_PageHandle* pageArray = (BM_PageHandle*) bm->mgmtData; // Properly cast mgmtData for easier access

    for (int i = 0; i < bm->numPages; i++) {
        if (pageArray[i].pageNum == page->pageNum) {
            // Ensure the fix count does not become negative
            if (pageArray[i].fixCounts > 0) {
                pageArray[i].fixCounts -= 1;
            }
            return RC_OK;
        }
    }

    return RC_OK; // Consider returning a different code if page not found
}


// Forces a single page to be written back to the disk
RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    FILE *f = fopen(bm->pageFile, "rb+");
    if (f == NULL) return RC_FILE_NOT_FOUND; // Check if file is accessible

    fseek(f, page->pageNum * PAGE_SIZE, SEEK_SET);
    fwrite(page->data, PAGE_SIZE, 1, f);
    fclose(f);
    bm->numberWriteIO++; // Increment write IO count
    page->dirty = 0; // Mark page as clean after writing
    return RC_OK;
}

// Pins a page with the specified page number into the buffer pool
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {
    int foundEmptyOrMatchingPage = 0, pageIndex = 0;
    
    // Iterate through all pages in the buffer to find an empty slot or a matching page
    for (int i = 0; i < bm->numPages; i++) {
        // Case 1: An empty page slot is found
        if ((bm->mgmtData + i)->pageNum == NO_PAGE) { // Using NO_PAGE for clarity
            // Allocate memory for the page data if not already allocated
            (bm->mgmtData + i)->data = (char*)calloc(PAGE_SIZE, sizeof(char));
            foundEmptyOrMatchingPage = 1; // Indicate that an empty page slot is found
            pageIndex = i;
            break;
        }
        // Case 2: The requested page is already in the buffer
        else if ((bm->mgmtData + i)->pageNum == pageNum) {
            foundEmptyOrMatchingPage = 2; // Indicate that the page is already in the buffer
            pageIndex = i;
            // Update buffer attributes if using LRU strategy
            if (bm->strategy == RS_LRU)
                updateBfrAtrbt(bm, bm->mgmtData + pageIndex);
            break;
        }
        // If we reached the last page and haven't found an empty slot or the page, apply replacement strategy
        else if (i == bm->numPages - 1) {
            foundEmptyOrMatchingPage = 1; // Indicate the need to replace a page
            // Apply the replacement strategy (FIFO or LRU) to find the page to replace
            pageIndex = strategyForFIFOandLRU(bm);
            // If the chosen page for replacement is dirty, write its contents to disk
            if ((bm->mgmtData + pageIndex)->dirty)
                forcePage(bm, bm->mgmtData + pageIndex);
        }
    }
        
    // Handle the found page based on the earlier check
    switch (foundEmptyOrMatchingPage) {
        case 1: // An empty slot is found or page needs to be replaced
        {
            // Load the requested page from disk into the buffer
            FILE *fp = fopen(bm->pageFile, "r");
            if (!fp) return RC_FILE_NOT_FOUND; // Check if the page file can be opened
            fseek(fp, pageNum * PAGE_SIZE, SEEK_SET);
            fread((bm->mgmtData + pageIndex)->data, sizeof(char), PAGE_SIZE, fp);
            fclose(fp);
            // Update the page handle and the management data
            page->data = (bm->mgmtData + pageIndex)->data;
            bm->numberReadIO += 1;
            (bm->mgmtData + pageIndex)->fixCounts += 1;
            (bm->mgmtData + pageIndex)->pageNum = pageNum;
            page->fixCounts = (bm->mgmtData + pageIndex)->fixCounts;
            page->dirty = (bm->mgmtData + pageIndex)->dirty;
            page->pageNum = pageNum;
            updateBfrAtrbt(bm, bm->mgmtData + pageIndex); // Update buffer attributes for replacement strategy
            break;
        }
        case 2: // The page is already present in the buffer
        {
            // Simply update the fix counts and return the existing page data
            page->data = (bm->mgmtData + pageIndex)->data;
            (bm->mgmtData + pageIndex)->fixCounts += 1;
            page->fixCounts = (bm->mgmtData + pageIndex)->fixCounts;
            page->dirty = (bm->mgmtData + pageIndex)->dirty;
            page->pageNum = pageNum;
            break;
        }
    }
    return RC_OK;
}

// Retrieves an array of PageNumbers indicating the content of each frame in the buffer pool.
// If a frame contains a page, the corresponding element in the array is the page number.
// If a frame is empty, the corresponding element is set to NO_PAGE.
PageNumber *getFrameContents(BM_BufferPool *const bm) {
    // Allocate memory for an array to store page numbers, one per frame.
    PageNumber *pageNoArray = (PageNumber *)malloc(bm->numPages * sizeof(PageNumber));
    if (pageNoArray == NULL) {
        // Handle memory allocation failure if necessary
        return NULL; // Or another approach as per your error handling strategy
    }

    // Iterate through each frame in the buffer pool.
    for (int i = 0; i < bm->numPages; i++) {
        BM_PageHandle *currentFrame = &((BM_PageHandle *)bm->mgmtData)[i];
        // If the frame is not empty, store the page number in the array.
        pageNoArray[i] = currentFrame->data != NULL ? currentFrame->pageNum : NO_PAGE;
    }

    // Return the array of page numbers.
    return pageNoArray;
}

// Retrieves an array of boolean values indicating whether each frame in the buffer pool is dirty.
// A dirty frame has been modified and needs to be written back to disk.
bool *getDirtyFlags(BM_BufferPool *const bm) {
    // Allocate memory for an array of boolean values, one per frame.
    bool *dirtyFlagsArray = (bool*)malloc(bm->numPages * sizeof(bool));
    if (dirtyFlagsArray == NULL) {
        // Handle memory allocation failure if necessary
        return NULL; // Or another approach as per your error handling strategy
    }

    BM_PageHandle *pageArray = (BM_PageHandle *)bm->mgmtData; // Cast once for easier access
    // Iterate through each frame in the buffer pool.
    for (int i = 0; i < bm->numPages; i++) {
        // Set each element of the array to the dirty status of the corresponding frame.
        dirtyFlagsArray[i] = pageArray[i].dirty;
    }

    // Return the array of dirty flags.
    return dirtyFlagsArray;
}

// Retrieves an array of integers indicating the fix count for each frame in the buffer pool.
// The fix count is the number of clients currently using that frame.
int *getFixCounts(BM_BufferPool *const bm) {
    // Allocate memory for an array to store fix counts, one per frame.
    int *fixCountsArray = (int*)malloc(bm->numPages * sizeof(int));
    if (fixCountsArray == NULL) {
        // Handle memory allocation failure if necessary
        return NULL; // Or implement another error handling strategy as needed
    }

    BM_PageHandle *pageArray = (BM_PageHandle *)bm->mgmtData; // Cast once for easier access
    // Iterate through each frame in the buffer pool.
    for (int i = 0; i < bm->numPages; i++) {
        // Store the fix count of each frame in the array.
        fixCountsArray[i] = pageArray[i].fixCounts;
    }

    // Return the array of fix counts.
    return fixCountsArray;
}

// Returns the total number of read operations performed by the buffer manager.
// This typically includes the number of pages that have been read from disk into the buffer pool.
int getNumReadIO (BM_BufferPool *const bm) 
{
    return bm->numberReadIO;
}

// Returns the total number of write operations performed by the buffer manager.
// This typically includes the number of pages that have been written back to disk from the buffer pool.
int getNumWriteIO (BM_BufferPool *const bm) 
{
    return bm->numberWriteIO;
}

// Determines the page to replace according to FIFO and LRU strategies for the given buffer pool.
int strategyForFIFOandLRU(BM_BufferPool *bm) {
    int min, pageToReplace, i;
    int *attributes = getAttributionArray(bm); // Assume this dynamically allocates memory
    int *fixCounts = getFixCounts(bm); // Assume this dynamically allocates memory

    min = bm->buffertimer;
    pageToReplace = -1;

    for (i = 0; i < bm->numPages; ++i) {
        if (fixCounts[i] != 0) // Skip pages currently in use.
            continue;

        if (min >= attributes[i]) {
            pageToReplace = i;
            min = attributes[i];
        }
    }

    if (bm->buffertimer > 32000) {
        for (i = 0; i < bm->numPages; ++i) {
            BM_PageHandle *page = (BM_PageHandle *)(bm->mgmtData + i);
            page->strategyAttribute -= min; // Correctly adjust strategy attributes for each page.
        }
        bm->buffertimer -= min;
    }

    free(attributes); // Free the dynamically allocated memory.
    free(fixCounts);

    return pageToReplace;
}

// Generates an array of integers representing some attributes of pages in the buffer pool, likely for use in replacement strategies.
int *getAttributionArray(BM_BufferPool *bm) 
{
    int *flag;
    int i = 0;

    // Allocates memory initialized to zero for storing attributes.
    int *atrbts = (int*)calloc(bm->numPages, sizeof((bm->mgmtData)->strategyAttribute));
    while (i < bm->numPages) 
    {
        flag = atrbts + i;
        // Copies the attribute (e.g., timestamp or order of insertion) for each page from the management data.
        *flag = *((bm->mgmtData + i)->strategyAttribute);
        i++;
    }
    // Returns the array of attributes.
    return atrbts;
}

// Frees allocated memory for all pages within a buffer pool
void freePagesForBuffer(BM_BufferPool *bm) {
    if (bm == NULL || bm->mgmtData == NULL) {
        return; // Safeguard against NULL pointers
    }

    // Iterate through each page in the buffer pool
    for (int i = 0; i < bm->numPages; i++) {
        // Free the data associated with each page, if it's not NULL
        if ((bm->mgmtData + i)->data != NULL) {
            free((bm->mgmtData + i)->data);
            (bm->mgmtData + i)->data = NULL; // Nullify the pointer after freeing
        }

        // Note: Do not attempt to free strategyAttribute as it's not a pointer
    }

    // Optionally, if bm->mgmtData itself is dynamically allocated, free it here
    // free(bm->mgmtData);
    // bm->mgmtData = NULL;
}

//----------------------------------------------------------------------------------------------------------
// Updates the strategy attributes of a page handle within a buffer pool
RC updateBfrAtrbt(BM_BufferPool *bm, BM_PageHandle *pageHandle) 
{
    // Check if the page handle's strategy attribute is uninitialized and the buffer pool uses LRU or FIFO strategy
    if (pageHandle->strategyAttribute == NULL) 
    {
        // Allocate memory for the strategy attribute if using LRU or FIFO replacement strategy
        if (bm->strategy == RS_LRU || bm->strategy == RS_FIFO)
            pageHandle->strategyAttribute = calloc(1, sizeof(int));
    }

    // If the buffer pool's strategy is LRU or FIFO, update the page handle's strategy attribute
    if (bm->strategy == RS_LRU || bm->strategy == RS_FIFO)
    {
        int *attribute;
        // Get the pointer to the strategy attribute
        attribute = (int*) pageHandle->strategyAttribute;
        // Update the attribute with the current buffer timer value
        *attribute = bm->buffertimer;
        // Increment the buffer timer for the next usage
        bm->buffertimer = bm->buffertimer + 1;
        return RC_OK;
    }
    // Return an error if the buffer pool's strategy is not supported
    return RC_STRATEGY_NOT_FOUND;
}
