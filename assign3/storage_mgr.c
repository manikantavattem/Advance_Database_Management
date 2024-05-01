#include "dberror.h"
#include "storage_mgr.h"
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

// Initialize storage manager
RC initializeStorageManager() {
    return RC_OK;
}

// Create a new page file
RC createPageFile(char *fileName) {
    FILE *file = fopen(fileName, "w+b"); // Open file in write mode

    if (file == NULL) return RC_FILE_NOT_FOUND;

    // Create an empty page of size PAGE_SIZE
    SM_PageHandle emptyPage = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));
    if (fwrite(emptyPage, sizeof(char), PAGE_SIZE, file) < PAGE_SIZE) {
        fclose(file);
        free(emptyPage);
        return RC_WRITE_FAILED;
    }

    fclose(file);
    free(emptyPage);
    return RC_OK;
}

// Open an existing page file
RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
    FILE *file = fopen(fileName, "r+b"); // Open file in read/write mode

    if (file == NULL) return RC_FILE_NOT_FOUND;

    // Get file size and calculate total number of pages
    struct stat fileStat;
    stat(fileName, &fileStat);
    int totalPages = fileStat.st_size / PAGE_SIZE;

    // Initialize file handle
    fHandle->fileName = fileName;
    fHandle->totalNumPages = totalPages;
    fHandle->curPagePos = 0;
    fHandle->mgmtInfo = file;

    return RC_OK;
}

// Close an open page file
RC closePageFile(SM_FileHandle *fHandle) {
    if (fclose(fHandle->mgmtInfo) != 0) return RC_FILE_NOT_FOUND;
    return RC_OK;
}

// Delete a page file
RC destroyPageFile(char *fileName) {
    if (remove(fileName) != 0) return RC_FILE_NOT_FOUND;
    return RC_OK;
}

// Read a page from the file into memory
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (pageNum >= fHandle->totalNumPages || pageNum < 0) return RC_READ_NON_EXISTING_PAGE;

    FILE *file = fHandle->mgmtInfo;
    fseek(file, pageNum * PAGE_SIZE, SEEK_SET); // Move file pointer to the beginning of the page
    fread(memPage, sizeof(char), PAGE_SIZE, file); // Read the page into memory
    fHandle->curPagePos = pageNum; // Update current page position

    return RC_OK;
}

// Get the current page position
RC getBlockPos(SM_FileHandle *fHandle) {
    return fHandle->curPagePos;
}

// Read the first page from the file into memory
RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(0, fHandle, memPage);
}

// Read the last page from the file into memory
RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->totalNumPages - 1, fHandle, memPage);
}

// Read the current page from the file into memory
RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->curPagePos, fHandle, memPage);
}

// Read the previous page from the file into memory
RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    int prevPageNum = fHandle->curPagePos - 1;
    return readBlock(prevPageNum, fHandle, memPage);
}

// Read the next page from the file into memory
RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    int nextPageNum = fHandle->curPagePos + 1;
    return readBlock(nextPageNum, fHandle, memPage);
}

// Write a page from memory into the file
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (pageNum >= fHandle->totalNumPages || pageNum < 0) return RC_WRITE_FAILED;

    FILE *file = fHandle->mgmtInfo;
    fseek(file, pageNum * PAGE_SIZE, SEEK_SET); // Move file pointer to the beginning of the page
    fwrite(memPage, sizeof(char), PAGE_SIZE, file); // Write the page from memory into the file

    return RC_OK;
}

// Write the current page from memory into the file
RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return writeBlock(fHandle->curPagePos, fHandle, memPage);
}

// Append an empty page at the end of the file
RC appendEmptyBlock(SM_FileHandle *fHandle) {
    FILE *file = fHandle->mgmtInfo;
    fseek(file, 0, SEEK_END); // Move file pointer to the end of the file

    SM_PageHandle emptyPage = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char)); // Allocate memory for an empty page
    if (fwrite(emptyPage, sizeof(char), PAGE_SIZE, file) < PAGE_SIZE) {
        free(emptyPage);
        return RC_WRITE_FAILED;
    }

    fHandle->totalNumPages++; // Update total number of pages
    free(emptyPage);
    return RC_OK;
}

// Ensure that the file has at least the specified number of pages
RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
    if (numberOfPages > fHandle->totalNumPages) {
        int pagesToAdd = numberOfPages - fHandle->totalNumPages;
        for (int i = 0; i < pagesToAdd; i++) {
            RC status = appendEmptyBlock(fHandle);
            if (status != RC_OK) return status;
        }
    }

    return RC_OK;
}
