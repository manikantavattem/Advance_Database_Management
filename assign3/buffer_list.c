#include "buffer_list.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

RC insert_bufpool(EntryPointer *entry, void *buffer_pool_ptr, void *buffer_page_handle)
{
    EntryPointer newptr = (EntryPointer)malloc(sizeof(BufferPool_Entry));
    if (newptr == NULL) {
        return RC_INSERT_BUFPOOL_FAILED;  // Memory allocation failed
    }

    // Initialize the new buffer pool entry
    newptr->buffer_pool_ptr = buffer_pool_ptr;
    newptr->buffer_page_info = buffer_page_handle;
    newptr->numreadIO = 0;
    newptr->numwriteIO = 0;
    newptr->nextBufferEntry = NULL;

    // If the list is empty, insert the new entry at the start
    if (*entry == NULL) {
        *entry = newptr;
    } else {
        // Traverse to the end of the list
        EntryPointer last = *entry;
        while (last->nextBufferEntry != NULL) {
            last = last->nextBufferEntry;
        }
        // Insert the new entry at the end of the list
        last->nextBufferEntry = newptr;
    }
    
    return RC_OK;  // Successfully inserted
}
//-------------------------------------------------------------------------------------------------
BufferPool_Entry *find_bufferPool(EntryPointer entryptr, void *buffer_pool_ptr)
{
    // Iterate over each entry in the list using a for loop
    for (EntryPointer current = entryptr; current != NULL; current = current->nextBufferEntry) {
        if (current->buffer_pool_ptr == buffer_pool_ptr) {
            return current;  // Found the matching buffer pool, return the entry
        }
    }
    return NULL;  // No matching buffer pool found, return NULL
}
//-------------------------------------------------------------------------------------------------
bool delete_bufpool(EntryPointer *entryptr, void *buffer_pool_ptr)
{
    if (entryptr == NULL || *entryptr == NULL) {
        return FALSE;  // Return FALSE if the list is empty or entryptr is NULL
    }

    EntryPointer current = *entryptr;
    EntryPointer previous = NULL;

    // Find the entry to delete
    while (current != NULL && current->buffer_pool_ptr != buffer_pool_ptr) {
        previous = current;
        current = current->nextBufferEntry;
    }

    // If no entry is found, return FALSE
    if (current == NULL) {
        return FALSE;
    }

    // Unlink the entry from the list
    if (previous != NULL) {
        previous->nextBufferEntry = current->nextBufferEntry;
    } else {
        *entryptr = current->nextBufferEntry;  // Update the head if the first entry is removed
    }

    // Free the found entry
    free(current);
    return TRUE;
}
//-------------------------------------------------------------------------------------------------
BufferPool_Entry *checkPoolsUsingFile(EntryPointer entry, int *filename)
{
    // Iterate over each entry using a more concise for loop
    for (EntryPointer current = entry; current != NULL; current = current->nextBufferEntry) {
        BM_BufferPool *bufferpool = (BM_BufferPool *)current->buffer_pool_ptr;
        if (bufferpool->pageFile == filename) {
            return current;  // Found the matching buffer pool entry, return it
        }
    }
    return NULL; // No matching buffer pool entry found
}
//-------------------------------------------------------------------------------------------------
int getPoolsUsingFile(EntryPointer entry, char *filename)
{
    int count = 0;
    // Iterate through each buffer pool entry using a for loop
    for (EntryPointer current = entry; current != NULL; current = current->nextBufferEntry) {
        BM_BufferPool *bufferpool = (BM_BufferPool *)current->buffer_pool_ptr;
        if (bufferpool->pageFile == filename) {
            count++;  // Increment count if the pageFile pointer matches the given filename pointer
        }
    }
    return count;  // Return the total count of matching buffer pools
}
//-------------------------------------------------------------------------------------------------