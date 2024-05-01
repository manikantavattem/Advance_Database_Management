#include "record_scan.h"
#include <stdlib.h>
#include <stdio.h>

AUX_Scan *search(RM_ScanHandle *sHandle, PTR_Scan sEntry)
{
    PTR_Scan curr_node = sEntry;
    while (curr_node != NULL) {
        if (curr_node->sHandle == sHandle) {
            // Found the node with the matching handle
            return curr_node->auxScan;
        }
        curr_node = curr_node->nextScan;
    }
    return NULL; // Return NULL if no matching node is found
}
//------------------------------------------------------------------------------------
RC insert(RM_ScanHandle *sHandle, PTR_Scan *sEntry, AUX_Scan *auxScan)
{
    // Allocate memory for the new node
    PTR_Scan new_node = (Scan_Entry *)malloc(sizeof(Scan_Entry));
    if (new_node == NULL) {
        return RC_MEMORY_ALLOCATION_FAIL; // Return an error code if memory allocation fails
    }

    // Initialize the new node
    new_node->auxScan = auxScan;
    new_node->sHandle = sHandle;
    new_node->nextScan = NULL;  // Ensure the new node points to NULL

    // Traverse to the end of the list
    PTR_Scan curr_node = *sEntry;
    if (curr_node == NULL) {
        // If the list is empty, set the new node as the first node
        *sEntry = new_node;
    } else {
        // Find the last node in the list
        while (curr_node->nextScan != NULL) {
            curr_node = curr_node->nextScan;
        }
        // Attach the new node at the end of the list
        curr_node->nextScan = new_node;
    }

    return RC_OK;  // Return success
}
//------------------------------------------------------------------------------------
RC delete(RM_ScanHandle *sHandle, PTR_Scan *sEntry)
{
    if (sEntry == NULL || *sEntry == NULL) {
        return RC_FILE_NOT_FOUND; // No entries to delete
    }

    // Directly remove the first node from the list
    PTR_Scan toDelete = *sEntry; // Store the node to be deleted
    *sEntry = toDelete->nextScan; // Set the head to the next node in the list

    free(toDelete); // Free the memory of the node that was removed
    return RC_OK; // Return success
}
//------------------------------------------------------------------------------------