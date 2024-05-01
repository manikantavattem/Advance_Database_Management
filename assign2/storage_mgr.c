#include "storage_mgr.h"
#include <sys/stat.h>
#include <string.h>
#include <stdlib.h>
#include "test_helper.h"

//Global Data Structures 
//Transform the data structure into metadata, represented as a linked list comprising maps.
typedef struct _metaDataList 
{
		char key[50];
		char value[50];
		struct _metaDataList *nextMetaDataNode;
} metaDataList;

metaDataList *firstNode = NULL;
metaDataList *currentNode = NULL;
metaDataList *previousNode = NULL;
//If the condition is 1, then execute the test case calls; otherwise, execute any other function.
int whoIsCallingCreate = 1; 
//Global Data Structures [END]

/* manipulating page files */
/*
 * To set up global data in the following improvements.
 */
void initStorageManager()
{

}
/*
 * Retrieves the key-value pair at the nth position within the metadata.
 *
 */
void getMeNthMetaData(int n, char * string,char *nthKeyValuePair)
{
	char newString[PAGE_SIZE];
	memset(newString, '\0', PAGE_SIZE);
	newString[0] = ';';
	strcat(newString, string);

	//Save the positions of semicolons in an array named delimiterPosition(START).
	char delimiterPosition[1000];
	int iLoop;
	int delPostion = 0;

	for (iLoop = 0; iLoop < strlen(newString); iLoop++)
	{
		if (newString[iLoop] == ';')
		{
			delimiterPosition[delPostion] = iLoop;
			delPostion++;
		}
	}
	//Save the positions of semicolons in an array named delimiterPosition(END).

	int currentPos = 0;
	for (iLoop = delimiterPosition[n - 1] + 1;
			iLoop <= delimiterPosition[n] - 1; iLoop++)
	{
		nthKeyValuePair[currentPos] = newString[iLoop];
		currentPos++;
	}
	nthKeyValuePair[currentPos] = '\0';
}



/*
 * Create a LinkedList structure that represents the metadata using maps.
 *
 */
metaDataList * constructMetaDataLinkedList(char *metaInformation,
		int noOfNodesToBeConstructed)
{
	int iLoop;
	char currentMetaKeyValue[100];

	char currentKey[50];
	memset(currentKey,'\0',50);

	char currentValue[50];
	memset(currentValue,'\0',50);

	for (iLoop = 1; iLoop <= noOfNodesToBeConstructed; iLoop++)
	{
		memset(currentMetaKeyValue,'\0',100);
		getMeNthMetaData(iLoop, metaInformation,currentMetaKeyValue);

		char colonFound = 'N';
		int keyCounter = 0;
		int ValueCounter = 0;
		int i;
		for (i = 0; i < strlen(currentMetaKeyValue); i++)
		{
			if (currentMetaKeyValue[i] == ':')
				colonFound = 'Y';

			if (colonFound == 'N')
				currentKey[keyCounter++] = currentMetaKeyValue[i];
			else if (currentMetaKeyValue[i] != ':')
				currentValue[ValueCounter++] = currentMetaKeyValue[i];
		}
		currentKey[keyCounter] = '\0';
		currentValue[ValueCounter] = '\0';

	

		currentNode = (metaDataList *) malloc(sizeof(metaDataList));

		strcpy(currentNode->value,currentValue);
		strcpy(currentNode->key,currentKey);
		currentNode->nextMetaDataNode = NULL;

		if (iLoop == 1)
		{
			firstNode= currentNode;
			previousNode = NULL;
		}
		else
		{
			previousNode->nextMetaDataNode = currentNode;
		}
		previousNode = currentNode;
	}
	return firstNode;
}
/*
 * This method is utilized for either creating pages or appending pages. The AppendPage function, in turn, calls this method.
 *
 */
RC createPageFile(char *filename)
{
	FILE *fp;
	fp = fopen(filename, "a+b"); 	//Generate and open the file for both reading and writing purposes.
	if (whoIsCallingCreate == 1) 	//If the test case calls this function, allocate three blocks.
	{
		if (fp != NULL)
		{
			char nullString2[PAGE_SIZE]; // metaBlock
			char nullString3[PAGE_SIZE]; // Initial Data Block

			char stringPageSize[5];
			sprintf(stringPageSize, "%d", PAGE_SIZE);

			char strMetaInfo[PAGE_SIZE * 2];
			strcpy(strMetaInfo, "PS:"); // PS == PageSize
			strcat(strMetaInfo, stringPageSize);
			strcat(strMetaInfo, ";");
			strcat(strMetaInfo, "NP:0;"); //NP == No of Pages
			

			int i;
			for (i = strlen(strMetaInfo); i < (PAGE_SIZE * 2); i++)
				strMetaInfo[i] = '\0';
			memset(nullString2, '\0', PAGE_SIZE);
			memset(nullString3, '\0', PAGE_SIZE);

			fwrite(strMetaInfo, PAGE_SIZE, 1, fp);
			fwrite(nullString2, PAGE_SIZE, 1, fp);
			fwrite(nullString3, PAGE_SIZE, 1, fp);

			fclose(fp);
			return RC_OK;
		} else
		{
			return RC_FILE_NOT_FOUND;
		}
	}
	else
	{
		if (fp != NULL)
		{
			char nullString[PAGE_SIZE];

			memset(nullString, '\0', PAGE_SIZE);
			fwrite(nullString, PAGE_SIZE, 1, fp);

			fclose(fp);
			return RC_OK;
		} else
		{
			return RC_FILE_NOT_FOUND;
		}
	}

}
/* There are three conditions in this header.
 * 1. Open the pageFile in read mode.
 * 2. If the file exists, populate the details of the file into the fHandle structure and return RC_OK.
 * 3. Otherwise, return RC_FILE_NOT_FOUND.
 */

RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{
	struct stat statistics;
	FILE *fp;

	fp = fopen(fileName, "r");
	if (fp != NULL)
	{
		//Initialize our structure with the necessary values.
		fHandle->fileName = fileName;
		fHandle->curPagePos = 0;
		stat(fileName, &statistics);
		fHandle->totalNumPages = (int) statistics.st_size / PAGE_SIZE;
		fHandle->totalNumPages -= 2; // 2 pages are reserved for metaInfo. Hence Subtracting.

		//Read MetaData Information and dump it into a Linked List.
		char metaDataInformationString[PAGE_SIZE * 2];
		fgets(metaDataInformationString, (PAGE_SIZE * 2), fp);
		//Read metadata information and store it in a string format.

		//Determine the total number of metadata nodes to be created.
		int iLoop;
		int noOfNodes = 0;
		for (iLoop = 0; iLoop < strlen(metaDataInformationString); iLoop++)
			if (metaDataInformationString[iLoop] == ';')
				noOfNodes++;
		//Calculate the count of metadata nodes that need to be constructed.

		fHandle->mgmtInfo = constructMetaDataLinkedList(
				metaDataInformationString, noOfNodes);
		//The fileHandle now contains all the necessary meta information.
		//Read MetaData Information and dump it into a Linked List.
		fclose(fp);

		return RC_OK;
	}
	else
	{
		return RC_FILE_NOT_FOUND;
	}
}


/*
 * Convert an integer to a character pointer.
 */
	
void convertToString(int someNumber,char * reversedArray)
{
	char array[4];
	memset(array, '\0', 4);
	int i = 0;
	while (someNumber != 0)
	{
		array[i++] = (someNumber % 10) + '0';
		someNumber /= 10;
	}
	array[i] = '\0';

	int j=0;
	int x;
	for(x = strlen(array)-1;x>=0;x--)
	{
		reversedArray[j++] = array[x];
	}
	reversedArray[j]='\0';
}
/*
 * To write meta information to the file.
 */
RC writeMetaListOntoFile(SM_FileHandle *fHandle,char *dataToBeWritten)
{
	FILE *fp = fopen(fHandle->fileName,"r+b");

	if(fp != NULL)
	{
		fwrite(dataToBeWritten,1,PAGE_SIZE,fp);
		fclose(fp);
		return RC_OK;
	}
	else
	{
		return RC_WRITE_FAILED;
	}
}

/*
 * Deallocate the memory used by the linked list.
 */
void freeMemory()
{
	metaDataList *previousNode;
	metaDataList *current  = firstNode;
	previousNode = firstNode;
	while(current != NULL)
	{
		current = current->nextMetaDataNode;
		if(previousNode!=NULL)
			free(previousNode);
		previousNode = current;
	}
	previousNode = NULL;
	firstNode = NULL;
}
/*
 * To write MetaData and close the PageFileWrite the metadata and close the page file.
 */
RC closePageFile(SM_FileHandle *fHandle)
{
	if (fHandle != NULL)
	{
		//update the NP to totalPages.
		//write LL to Disk.

		metaDataList *temp = firstNode;
		char string[4];
		memset(string,'\0',4);
		while (1 == 1)
		{
			if(temp != NULL)
			{
				if(temp->key != NULL)
				{
					if (strcmp(temp->key, "NP") == 0)
					{
						convertToString(fHandle->totalNumPages,string);
						strcpy(temp->value,string);
						break;
					}
				}
				temp = temp->nextMetaDataNode;
			}
			else
				break;
		}
		temp = firstNode;

		char metaData[2 * PAGE_SIZE];
		memset(metaData, '\0', 2 * PAGE_SIZE);
		int i = 0;
		while (temp != NULL)
		{
			int keyCounter = 0;
			int valueCounter = 0;
			while (temp->key[keyCounter] != '\0')
				metaData[i++] = temp->key[keyCounter++];
			metaData[i++] = ':';
			while (temp->value[valueCounter] != '\0')
				metaData[i++] = temp->value[valueCounter++];
			metaData[i++] = ';';
			temp = temp->nextMetaDataNode;
		}
		writeMetaListOntoFile(fHandle,metaData);
		fHandle->curPagePos = 0;
		fHandle->fileName = NULL;
		fHandle->mgmtInfo = NULL;
		fHandle->totalNumPages = 0;
		fHandle = NULL;
		freeMemory();
		return RC_OK;
	}
	else
	{
		return RC_FILE_HANDLE_NOT_INIT;
	}
}


/*
 * Delete a pageFile from the hard disk.
 */


RC destroyPageFile(char *fileName)
{
	if (remove(fileName) == 0)
		return RC_OK;
	else
		return RC_FILE_NOT_FOUND;
}

/* reading blocks from disc */
/* 
 * There are 3 condition for the header.
 * 1. If pageNum is greater than the total number of pages in the Page File, return RC_READ_NON_EXISTING_PAGE.
 * 2. Open the Page File.
 * 3. Read the desired page using fread().
 */
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{

	if (fHandle->totalNumPages < pageNum)
	{
		return RC_READ_NON_EXISTING_PAGE;
	}
	else
	{
		FILE *fp;
		fp = fopen(fHandle->fileName, "r");
		if (fp != NULL)
		{
			if (fseek(fp, ((pageNum * PAGE_SIZE) + 2 * PAGE_SIZE), SEEK_SET)== 0)
			{
				fread(memPage, PAGE_SIZE, 1, fp);
				fHandle->curPagePos = pageNum;
				fclose(fp);
				return RC_OK;
			}
			else
			{
				return RC_READ_NON_EXISTING_PAGE;
			}
		} else
		{
			return RC_FILE_NOT_FOUND;
		}
	}
}


/*
 * Return the current page position.
 */
 
 
int getBlockPos(SM_FileHandle *fHandle)
{
	return fHandle->curPagePos;
}
/*
 * Invoke the readBlock function to read the first block, which is the 0th block, in the Page File.
 */
RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (readBlock(0, fHandle, memPage) == RC_OK)
		return RC_OK;
	else
		return RC_READ_NON_EXISTING_PAGE;
}

/*
 * Invoke the readBlock function to read the previous block using the position returned by getBlockPos() decremented by 1.
 */
RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (readBlock(getBlockPos(fHandle) - 1, fHandle, memPage) == RC_OK)
		return RC_OK;
	else
		return RC_READ_NON_EXISTING_PAGE;
}
/*
 * Invoke the readBlock function to read the current block using the position returned by getBlockPos().
 */
RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (readBlock(getBlockPos(fHandle), fHandle, memPage) == RC_OK)
		return RC_OK;
	else
		return RC_READ_NON_EXISTING_PAGE;
}


/*
 * Invoke the readBlock function to read the next block using the position returned by getBlockPos() incremented by 1.
 */
RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (readBlock(getBlockPos(fHandle) + 1, fHandle, memPage) == RC_OK)
		return RC_OK;
	else
		return RC_READ_NON_EXISTING_PAGE;
}

/*
 * Invoke the readBlock function to read the last block, which is the (totalNumPages - 1)th block, in the page file.
 */
RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (readBlock(fHandle->totalNumPages - 1, fHandle, memPage) == RC_OK)
		return RC_OK;
	else
		return RC_READ_NON_EXISTING_PAGE;
}


/* writing blocks to a page file */

/*
 * Write the content of memPage to the file specified by fHandle->fileName at the block number pageNum.
 */
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{

	if (pageNum < 0 || pageNum > fHandle->totalNumPages)//
		return RC_WRITE_FAILED;
	else
	{
		int startPosition = (pageNum * PAGE_SIZE) + (2 * PAGE_SIZE);

		FILE *fp = fopen(fHandle->fileName, "r+b");
		if (fp != NULL)
		{
			if (fseek(fp, startPosition, SEEK_SET) == 0)
			{
				fwrite(memPage, 1, PAGE_SIZE, fp);
				if (pageNum > fHandle->curPagePos)
					fHandle->totalNumPages++;
				fHandle->curPagePos = pageNum;
				fclose(fp);
				return RC_OK;
			}
			else
			{
				return RC_WRITE_FAILED;
			}
		} else
		{
			return RC_FILE_HANDLE_NOT_INIT;
		}
	}
}
/*
 * Write the content of memPage to the file specified by fHandle->fileName at the block number fHandle->currentPagePos.
 */

RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (writeBlock(fHandle->curPagePos, fHandle, memPage) == RC_OK)
		return RC_OK;
	else
		return RC_WRITE_FAILED;
}
/*
 * Add an empty block at the end of the file.
 */

RC appendEmptyBlock(SM_FileHandle *fHandle)
{
	whoIsCallingCreate = 2;
	if (createPageFile(fHandle->fileName) == RC_OK)
	{

		fHandle->totalNumPages++;
		fHandle->curPagePos = fHandle->totalNumPages - 1;
		whoIsCallingCreate = 1;
		return RC_OK;
	}
	else
	{
		whoIsCallingCreate = 1;
		return RC_WRITE_FAILED;
	}
}

/*
 * Add a number of blocks at the end of the file equal to (numberOfPages - fHandle->totalNumPages).
 */

RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle)
{
	int extraPagesToBeAdded = numberOfPages - (fHandle->totalNumPages);
	int iLoop;
	if (extraPagesToBeAdded > 0)
	{
		for (iLoop = 0; iLoop < extraPagesToBeAdded; iLoop++)
		{
			whoIsCallingCreate = 3;
			createPageFile(fHandle->fileName);
			whoIsCallingCreate = 1;
			fHandle->totalNumPages++;
			fHandle->curPagePos = fHandle->totalNumPages - 1;
		}
		return RC_OK;
	} else
		return RC_READ_NON_EXISTING_PAGE; 
}

