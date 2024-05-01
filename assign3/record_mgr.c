#include "storage_mgr.h"
#include "record_scan.h"
#include "record_mgr.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>


// Global schema to be used across different functions
static Schema globalSchema;
// Global scan pointer, initialized to NULL
static PTR_Scan scan_pointer = NULL;

// Initializes the record manager
RC initRecordManager(void *mgmtData)
{
    return RC_OK;
}


// Shuts down the record manager
RC shutdownRecordManager()
{
    return RC_OK;
}


// Creates a table with a given name and schema
RC createTable(char *tableName, Schema *schema)
{
    char fileName[64];
    snprintf(fileName, sizeof(fileName), "%s.bin", tableName);
    createPageFile(fileName);  // Create the binary file for the table
    globalSchema = *schema;    // Assign the provided schema to the global schema
    return RC_OK;
}


// Opens a table by loading its data into the buffer pool
RC openTable(RM_TableData *tableData, char *tableName)
{
    char fileName[64];
    snprintf(fileName, sizeof(fileName), "%s.bin", tableName);
    
    // Allocate memory for a local schema and create a buffer pool
    Schema *localSchema = malloc(sizeof(Schema));
    BM_BufferPool *bufferPool = MAKE_POOL();
    
    initBufferPool(bufferPool, fileName, 4, RS_FIFO, NULL);
    
    // Set up the table data structure
    tableData->name = tableName;           // Assign table name
    tableData->mgmtData = bufferPool;      // Assign buffer pool to management data
    *localSchema = globalSchema;           // Copy global schema to local schema
    tableData->schema = localSchema;       // Assign schema to table data
    
    return RC_OK;
}


// Closes a table and frees the associated buffer pool resources
RC closeTable(RM_TableData *tableData)
{
    BM_BufferPool *bufferPool = (BM_BufferPool *)tableData->mgmtData;
    shutdownBufferPool(bufferPool);  // Terminate buffer pool
    free(bufferPool);                // Release memory allocated for the buffer pool
    return RC_OK;
}


// Deletes a table by removing its associated file
RC deleteTable(char *tableName)
{
    char fileName[50];
    snprintf(fileName, sizeof(fileName), "%s.bin", tableName);
    destroyPageFile(fileName);  // Destroy the binary file associated with the table
    return RC_OK;
}


// Returns the number of tuples stored in a table
int getNumTuples(RM_TableData *tableData)
{
    int tupleCount = 0; // Initialize tuple count
    PageNumber pageNumber = 1; // Start from the first page
    BM_PageHandle *pageHandle = MAKE_PAGE_HANDLE(); // Create a page handle
    BM_BufferPool *bufferPool = (BM_BufferPool *)tableData->mgmtData; // Get buffer pool from table data
    SM_FileHandle *fileHandle = (SM_FileHandle *)bufferPool->mgmtData; // Get file handle from buffer pool

    // Iterate through all pages in the file
    while (pageNumber < fileHandle->totalNumPages)
    {
        pinPage(bufferPool, pageHandle, pageNumber); // Pin the current page

        // Process each byte in the page
        for (int i = 0; i < PAGE_SIZE; i++)
        {
            // Check for tuple delimiter and count tuples
            if (pageHandle->data[i] == '-')
                tupleCount++;
        }

        pageNumber++;  // Move to the next page
    }

    return tupleCount;  // Return the total number of tuples
}


// Function to insert a record into the table
RC insertRecord(RM_TableData *rel, Record *record)
{
  int slot_number = 0, page_length, total_rec_length;
  char *sp = NULL;
  RID id;
  
  PageNumber page_number;
  BM_BufferPool *buffer_pool = (BM_BufferPool *)rel->mgmtData;
  BM_PageHandle *page_handle = MAKE_PAGE_HANDLE();
  SM_FileHandle *sm_handle = (SM_FileHandle *)buffer_pool->mgmtData;
  page_number = 1;
  
  // Calculate the total record length
  total_rec_length = getRecordSize(rel->schema);
  
  // Iterate over each page to find available space
  while (page_number < sm_handle->totalNumPages)
  {
    // Pin the current page
    pinPage(buffer_pool, page_handle, page_number);
    // Calculate the current page's used length
    page_length = strlen(page_handle->data);

    // Check for sufficient space to insert the record
    if (PAGE_SIZE - page_length > total_rec_length)
    {
      // Determine the slot number based on used space
      slot_number = page_length / total_rec_length;
      // Unpin the page after finding sufficient space
      unpinPage(buffer_pool, page_handle);
      break; // Exit loop since appropriate space was found
    }

    // Unpin the page if not enough space and proceed to the next
    unpinPage(buffer_pool, page_handle);
    page_number++;
  }

  // If no slots available in existing pages, prepare a new page
  if (slot_number == 0)
  {
    // Create and pin a new page beyond the current page count
    pinPage(buffer_pool, page_handle, page_number + 1);
    // Unpin the newly created page
    unpinPage(buffer_pool, page_handle);
  }

  // Pin the page to insert the record
  pinPage(buffer_pool, page_handle, page_number);
  // Position pointer at the end of the page's content
  sp = page_handle->data + strlen(page_handle->data);

  // Copy record data to the end of the page
  strcpy(sp, record->data);
  // Mark the page as dirty due to the modification
  markDirty(buffer_pool, page_handle);
  // Unpin the page after modification
  unpinPage(buffer_pool, page_handle);

  // Set record ID with the page and slot number
  id.page = page_number;
  id.slot = slot_number;
  record->id = id;
  
  // Return success
  return RC_OK;
}


// Deletes a record from a table using its RID
RC deleteRecord(RM_TableData *tableData, RID id)
{
    BM_BufferPool *bufferPool = (BM_BufferPool *)tableData->mgmtData;
    BM_PageHandle *pageHandle = MAKE_PAGE_HANDLE();
    PageNumber pageNumber = id.page;

    pinPage(bufferPool, pageHandle, pageNumber);
    markDirty(bufferPool, pageHandle);
    unpinPage(bufferPool, pageHandle);
    free(pageHandle);

    return RC_OK;
}


// Updates a record in the table with new data
RC updateRecord(RM_TableData *tableData, Record *record)
{
    BM_BufferPool *bufferPool = (BM_BufferPool *)tableData->mgmtData;
    BM_PageHandle *pageHandle = MAKE_PAGE_HANDLE();
    RID id = record->id;
    int slotNumber = id.slot;
    PageNumber pageNumber = id.page;
    int recordLength = getRecordSize(tableData->schema);

    pinPage(bufferPool, pageHandle, pageNumber);
    char *startPos = pageHandle->data + (recordLength * slotNumber);
    memcpy(startPos, record->data, recordLength); // Using memcpy for binary data safety
    markDirty(bufferPool, pageHandle);
    unpinPage(bufferPool, pageHandle);
    free(pageHandle);

    return RC_OK;
}


// Retrieves a record from the table based on its RID and stores it in the record structure
RC getRecord(RM_TableData *tableData, RID id, Record *record)
{
    BM_BufferPool *bufferPool = (BM_BufferPool *)tableData->mgmtData;
    BM_PageHandle *pageHandle = MAKE_PAGE_HANDLE();
    PageNumber pageNumber = id.page;
    int slotNumber = id.slot;
    int recordLength = getRecordSize(tableData->schema);

    pinPage(bufferPool, pageHandle, pageNumber);
    char *startPos = pageHandle->data + (recordLength * slotNumber);
    memcpy(record->data, startPos, recordLength); // Using memcpy for binary data safety
    unpinPage(bufferPool, pageHandle);
    free(pageHandle);

    return RC_OK;
}


// Initiates a scan operation on a given table with specific conditions.
RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    // Retrieve buffer pool and file handle from the relation's management data.
    BM_BufferPool *buffer_pool = (BM_BufferPool *)rel->mgmtData;
    SM_FileHandle *file_handle = (SM_FileHandle *)buffer_pool->mgmtData;
  
    // Allocate and initialize memory for the auxiliary scan data structure.
    AUX_Scan *aux_scan = (AUX_Scan *)malloc(sizeof(AUX_Scan));
    if (aux_scan == NULL) {
        return RC_MEMORY_ALLOCATION_FAIL; // Check for memory allocation failure
    }
    aux_scan->_slotID = 1;
    aux_scan->_sPage = 1;
    aux_scan->_numPages = file_handle->totalNumPages;
    aux_scan->pHandle = MAKE_PAGE_HANDLE();
    aux_scan->_recLength = getRecordSize(rel->schema);
 
    // Set scan management and table data in the scan handle.
    scan->mgmtData = cond;  // Set condition expression in scan management data.
    scan->rel = rel;        // Associate the relation with the scan.

    // Add the scan to the scan list and return the result.
    return insert(scan, &scan_pointer, aux_scan);
}


// Function to retrieve the next tuple in the scan
RC next(RM_ScanHandle *scan, Record *record)
{
  RID id;
  Expr *exp = (Expr *)scan->mgmtData, *len, *rec, *aux_expression;
  RM_TableData *rm_data = scan->rel;
  Operator *res, *new_res;
  AUX_Scan *aux_scan = search(scan, scan_pointer);
  
  // Allocate memory for comparison value
  Value **cValue = (Value **)malloc(sizeof(Value *));
  *cValue = NULL;  
  res = exp->expr.op;
  
  switch(res->type)
  {
    case OP_COMP_SMALLER: 
    {
      len = res->args[0];
      rec = res->args[1];
      while(aux_scan->_sPage < aux_scan->_numPages)
      {
        // Pin the page
        pinPage(rm_data->mgmtData, aux_scan->pHandle, aux_scan->_sPage);
        aux_scan->_recsPage = strlen(aux_scan->pHandle->data) / aux_scan->_recLength;
        while(aux_scan->_slotID < aux_scan->_recsPage)
        {
          // Get the record ID
          id.slot = aux_scan->_slotID;
          id.page = aux_scan->_sPage;
          
          // Retrieve the record
          getRecord(rm_data, id, record);
          // Get attribute value
          getAttr(record, rm_data->schema, rec->expr.attrRef, cValue); 
          // Check condition
          if((rm_data->schema->dataTypes[rec->expr.attrRef] == DT_INT) && (len->expr.cons->v.intV > cValue[0]->v.intV)) 
          {
            aux_scan->_slotID++;
            // Unpin the page
            unpinPage(rm_data->mgmtData, aux_scan->pHandle);
            return RC_OK;
          }
          aux_scan->_slotID++;
        }
        break;
      }
      break;
    }
    case OP_COMP_EQUAL:
    {
      len = res->args[0];
      rec = res->args[1];
      while(aux_scan->_sPage < aux_scan->_numPages)
      {
        // Pin the page
        pinPage(rm_data->mgmtData, aux_scan->pHandle, aux_scan->_sPage);
        aux_scan->_recsPage = strlen(aux_scan->pHandle->data) / aux_scan->_recLength;
        while(aux_scan->_slotID < aux_scan->_recsPage)
        {
          // Get the record ID
          id.page = aux_scan->_sPage;
          id.slot = aux_scan->_slotID;
          
          // Retrieve the record
          getRecord(rm_data, id, record);
          // Get attribute value
          getAttr(record, rm_data->schema, rec->expr.attrRef, cValue);
          // Check condition
          if((rm_data->schema->dataTypes[rec->expr.attrRef] == DT_STRING) && (strcmp(cValue[0]->v.stringV , len->expr.cons->v.stringV) == 0))
          {
            aux_scan->_slotID++;
            // Unpin the page
            unpinPage(rm_data->mgmtData, aux_scan->pHandle);
            return RC_OK;
          } 
          else if((rm_data->schema->dataTypes[rec->expr.attrRef] == DT_INT)&&(cValue[0]->v.intV == len->expr.cons->v.intV))
          {
            aux_scan->_slotID++;
            // Unpin the page
            unpinPage(rm_data->mgmtData, aux_scan->pHandle);
            return RC_OK;
          } 
          else if((cValue[0]->v.floatV == len->expr.cons->v.floatV) && (rm_data->schema->dataTypes[rec->expr.attrRef] == DT_FLOAT))
          {
            aux_scan->_slotID++;
            // Unpin the page
            unpinPage(rm_data->mgmtData, aux_scan->pHandle);
            return RC_OK;
          } 
          aux_scan->_slotID++;
        }
        break;     
      }
      break;
    }
    case OP_BOOL_NOT:
    {
      aux_expression = exp->expr.op->args[0];
      new_res = aux_expression->expr.op;
      rec = new_res->args[0];
      len = new_res->args[1];
      if (new_res->type == OP_COMP_SMALLER)
      {
        while(aux_scan->_numPages > aux_scan->_sPage)
        {
          // Pin the page
          pinPage(rm_data->mgmtData, aux_scan->pHandle, aux_scan->_sPage);
          aux_scan->_recsPage = strlen(aux_scan->pHandle->data) / aux_scan->_recLength;
          while(aux_scan->_slotID < aux_scan->_recsPage)
          {
            id.slot = aux_scan->_slotID;
            id.page = aux_scan->_sPage;
            // Retrieve the record
            getRecord(rm_data, id, record);
            // Get attribute value
            getAttr(record, rm_data->schema, rec->expr.attrRef, cValue);
            if((cValue[0]->v.intV > len->expr.cons->v.intV) && (rm_data->schema->dataTypes[rec->expr.attrRef] == DT_INT))
            {
              aux_scan->_slotID++;
              // Unpin the page
              unpinPage(rm_data->mgmtData, aux_scan->pHandle);
              return RC_OK; 
            }
            aux_scan->_slotID++;
          }
          break; 
        }
        break;
      }
      break;
    }
    case OP_BOOL_AND:
    case OP_BOOL_OR:
      break;
  }
  // No more tuples found, return appropriate code
  return RC_RM_NO_MORE_TUPLES;
}


// Closes a scan operation by freeing its associated resources
RC closeScan(RM_ScanHandle *scanHandle)
{
    AUX_Scan *auxScan = search(scanHandle, scan_pointer);
    // Assuming delete function handles freeing of auxScan internally
    return delete(scanHandle, &scan_pointer);
}


// Calculates the total size of a record based on its schema
int getRecordSize(Schema *schema)
{
    int totalSize = 0;
    for (int i = 0; i < schema->numAttr; i++)
    {
        switch(schema->dataTypes[i])
        {
            case DT_INT:
                totalSize += sizeof(int);
                break;
            case DT_FLOAT:
                totalSize += sizeof(float);
                break;
            case DT_BOOL:
                totalSize += sizeof(bool);
                break;
            case DT_STRING:
                totalSize += schema->typeLength[i];
                break;
        }
    }
    // Include the space required for null indicators
    return totalSize + schema->numAttr;
}


// Creates a new schema with the given characteristics
Schema *createSchema(int numAttributes, char **attributeNames, DataType *dataTypes, int *typeLengths, int keySize, int *keys)
{
    Schema *schema = malloc(sizeof(Schema));
    if (schema == NULL)
        return NULL;  // Return NULL if memory allocation fails

    schema->numAttr = numAttributes;
    schema->attrNames = attributeNames;
    schema->dataTypes = dataTypes;
    schema->typeLength = typeLengths;
    schema->keySize = keySize;
    schema->keyAttrs = keys;

    return schema;
}


// Frees the memory allocated for a schema
RC freeSchema(Schema *schema)
{
    free(schema);
    return RC_OK;
}


// Function to create a new record
RC createRecord(Record **record, Schema *schema)
{
    // Allocate memory for the record structure
    Record *newRecord = malloc(sizeof(Record));
    if (!newRecord) {
        return RC_MEMORY_ALLOCATION_FAIL;
    }

    // Calculate the size of the record based on the schema
    int recordSize = getRecordSize(schema);

    // Allocate and zero-initialize memory for the record data
    newRecord->data = calloc(recordSize, sizeof(char));
    if (!newRecord->data) {
        free(newRecord); // Clean up if allocation fails
        return RC_MEMORY_ALLOCATION_FAIL;
    }

    // Assign the created record to the provided pointer
    *record = newRecord;
    return RC_OK;
}


// Function to free memory allocated for a record
RC freeRecord(Record *record)
{
    if (record) {
        // First free the data within the record
        free(record->data);
        // Then free the record structure itself
        free(record);
    }
    return RC_OK;
}


RC getAttr(Record *record, Schema *schema, int attrNum, Value **value)
{
    // Variable initialization
    DataType *dataTypes = schema->dataTypes;
    int *typeLengths = schema->typeLength;
    int offset = 0, i = 0;
    char *data = record->data;
    Value *attrValue = (Value*) malloc(sizeof(Value));

    // Error check for malloc failure
    if (attrValue == NULL) {
        return RC_ERR; // Handle memory allocation failure
    }

    // Calculate offset to target attribute
    while(i < attrNum) 
    {
        switch(dataTypes[i])
        {
            case DT_INT: 
                offset += sizeof(int); 
                break;
            case DT_FLOAT: 
                offset += sizeof(float); 
                break;
            case DT_BOOL: 
                offset += sizeof(bool); 
                break;
            case DT_STRING: 
                offset += typeLengths[i]; 
                break;
        }
        i++;
    }
  
    // Adjust offset
    offset += (attrNum + 1);
    char *temp = NULL;  

    // Extract attribute value based on its type
    switch(dataTypes[attrNum])
    {
        case DT_INT:
            attrValue->dt = DT_INT;
            temp = malloc(sizeof(int) + 1);
            strncpy(temp, data + offset, sizeof(int)); 
            temp[sizeof(int)] = '\0';
            attrValue->v.intV = atoi(temp);
            break;

        case DT_FLOAT:
            attrValue->dt = DT_FLOAT;
            temp = malloc(sizeof(float) + 1);
            strncpy(temp, data + offset, sizeof(float)); 
            temp[sizeof(float)] = '\0'; 
            attrValue->v.floatV = strtof(temp, NULL);
            break;

        case DT_BOOL:
            attrValue->dt = DT_BOOL;
            temp = malloc(sizeof(bool) + 1);
            strncpy(temp, data + offset, sizeof(bool)); 
            temp[sizeof(bool)] = '\0';
            attrValue->v.boolV = (bool) *temp;
            break;

        case DT_STRING:
            attrValue->dt = DT_STRING;
            int size = typeLengths[attrNum];
            temp = malloc(size + 1);
            strncpy(temp, data + offset, size); 
            temp[size] = '\0';
            attrValue->v.stringV = temp;
            break;
    }

    // Output the extracted value
    *value = attrValue;
    return RC_OK;
}


RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) 
{
    int offset = 0, *typeLengths = schema->typeLength;
    DataType *dataTypes = schema->dataTypes;
  
    // Calculate the offset to the attribute in the data array
    for (int i = 0; i < attrNum; i++) 
    {
        switch (dataTypes[i])
        {
            case DT_INT:    offset += sizeof(int);    break;
            case DT_FLOAT:  offset += sizeof(float);  break;
            case DT_BOOL:   offset += sizeof(bool);   break;
            case DT_STRING: offset += typeLengths[i]; break;
        }
    }
  
    // Adjust the offset for the attribute delimiter
    char *recordOffset = record->data + offset + (attrNum > 0 ? (attrNum + 1) : 0);
    if (attrNum == 0)
    {
        *recordOffset = '-';  // First attribute starts with a dash
        recordOffset++;
    }
    else
    {
        *(recordOffset - 1) = ' ';  // Separator for non-first attributes
    }
  
    // Set the attribute value in the record
    switch (value->dt)
    {
        case DT_INT:
            sprintf(recordOffset, "%d", value->v.intV);
            break;

        case DT_FLOAT:
            sprintf(recordOffset, "%f", value->v.floatV);
            break;
				 
        case DT_BOOL:
            sprintf(recordOffset, "%i", value->v.boolV);
            break;
			
        case DT_STRING:
            sprintf(recordOffset, "%s", value->v.stringV);
            break;
    }

    // Add padding for numeric types to maintain field width
    if (value->dt == DT_INT || value->dt == DT_FLOAT)
    {
        int fieldLength = (value->dt == DT_INT) ? sizeof(int) : sizeof(float);
        int currentLength = strlen(recordOffset);
        for (int i = currentLength; i < fieldLength; i++) {
            strcat(recordOffset, "0");
        }
        // Reverse the field to fit the fixed width (remove if not needed)
        int j;
        for (int i = 0, j = strlen(recordOffset) - 1; i < j; i++, j--)
        {
            char temp = recordOffset[i];
            recordOffset[i] = recordOffset[j];
            recordOffset[j] = temp;
        }
    }
  
    return RC_OK;
}
