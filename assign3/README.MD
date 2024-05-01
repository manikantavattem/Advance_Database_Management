Record Manager

Group 4- Lokesh Kamineni, Manikanta Vattem, Dipen Reddy Patil, Gayatri Valluru

**Aim:**

To develop a robust system for managing database tables with a fixed schema. This system facilitates various operations on records such as insertion, deletion, updating, and querying (scanning) based on specified conditions. Each table in your system is stored in a separate page file and accessed through a buffer manager.


**Starting steps:**

1: Clone the repository from the Gitlab to the desired location

2: Navigate to the "assign3" folder

3: Execute the Makefile using the command make

**How to Execute:**

1: Open Command Promt/PowerShell/Terminal.

2: Navigate to the assign3 directory.

3: Type command --> **make** and hit enter. (Files are complied and ready to be executed)

4: To view the testcase results type command **./record_mgr**

5: Type command --> **make clean** for clean up 


Table and Record Manager Functions: This encompasses methods such as creating, deleting, opening, and closing tables. The Storage Manager from PA1 and Buffer Manager from PA2 are utilized to access pages and carry out operations, respectively. The following methods are included:

initRecordManager()    : Initializes the Record Manager.
shutdownRecordManager(): Shuts down the Record Manager.
createTable()          : Creates and opens a table based on a given schema.
openTable()            : Initializes the buffer pool with the initBufferPool() function.
closeTable()           : Closes the table using the shutdownBufferPool() function and releases the buffer pool space.
deleteTable()          : Removes the table name, page, and deallocates memory using the destroyPageFile() function.
getNumTuples()         : Retrieves the count of records in the table.

**Record Functions:** Facilitate manipulative operations on records such as insertion, updates, and deletions.

insertRecord(): Inserts a record into the table based on the Record ID. An empty page slot is pinned and marked as dirty. The data is copied to the new record and the page is then unpinned.
updateRecord(): Updates an existing record in the table. The new data is copied to the record and the page is updated using the pinPage() function.
deleteRecord(): Removes a record from the table based on a specified Record ID.
getRecord()   : Retrieves a record from the table based on the Record ID and assigns the value to the record. The record size is determined by creating pointers.

**Scan Functions**: Employed to fetch records from the table.

startScan() : Initiates a scan operation by passing an argument such as RM_ScanHandle to this function. Auxiliary scan values are retrieved and assigned to the scan.
closeScan() : Terminates the scan operation.
next()      : Scans records until the desired record is located. Iterates through the record list to find the required record. The page containing the tuple is pinned and all tuples are scanned for the specified record. After scanning the entire page, it is unpinned.

**Schema Functions**: Concerned with managing schemas, retrieving record sizes, and freeing allocated schema space.

createSchema() : Creates a new schema.
freeSchema()   : Releases space allocated to a schema.
getRecordSize(): Retrieves the size of a record as specified in the schema.

**Attribute Functions:** Related to record creation and attribute management.

createRecord(): Creates a new record, allocates memory for the record, and retrieves the size of the record.
freeRecord()  : Releases space allocated to the record.
getAttr()     : Retrieves an attribute value from a record.
setAttr()     : Assigns an attribute value to a record.

****Contributions:****

All team members collaborated on integrating the various components of the record manager, ensuring seamless interaction between different functionalities.

**Lokesh** was instrumental in developing and overseeing the Schema, Scan, and Attribute components of the project. He was responsible for creating key functions such as createSchema, freeSchema, startScan, closeScan, next, createRecord, freeRecord, getAttr, and setAttr. These methods are central to managing schemas, scanning records, and manipulating attributes within the database. Additionally, Lokesh played a pivotal role in ensuring thorough testing of these components. He developed comprehensive test methods to validate the functionality and reliability of the schema and record scanning operations. His documentation work provided clear and detailed guidance on how to use and implement these methods, aiding in the overall understanding and usability of the system.

**Manikanta** focused on the core functionalities related to Record Management within the database system. He implemented various Record methods including insertRecord, updateRecord, deleteRecord, and getRecord, which are essential for the manipulation and maintenance of records in the database. Furthermore, Manikanta developed the dberror function, buffer_list, and record_scan modules, providing robust backend support for error handling and buffer management. His documentation not only detailed the operational aspects of record methods but also provided insights into error management and buffer list operations, ensuring that future developers and users can easily understand and utilize these features.

**Dipen** contributed to the project by handling the Table and Record Manager functions. He was tasked with the implementation of foundational database operations such as createTable, openTable, closeTable, and deleteTable. These functions are crucial for the creation, management, and removal of tables within the database, forming the backbone of the database's structure and functionality. Dipen also managed the documentation for these components, ensuring that each function was well described and that their usage and integration into the database were clearly understood. His contributions were vital in setting up the structural framework that supports the database operations.

**Gayatri** played a crucial role in the integration phase of the project by focusing on debugging and resolving issues that emerged during integration testing. Her work involved meticulous examination of the interactions between different database components to identify and fix bugs that were not apparent during unit testing. Gayatri's debugging efforts ensured that the database system worked seamlessly as a whole and met all operational requirements. Her contributions were critical in enhancing the stability and reliability of the database, making it ready for deployment and use in production environments.

All of us participated in meetings to discuss design decisions, review code, resolve integration challenges, and work together to optimize the record manager for performance and reliability.
	
