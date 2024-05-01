Title: CoreDB: Essential Database Management System

Project Description:

CoreDB is designed to be an educational tool for understanding the inner workings of a database management system. This project aims to develop a lightweight and minimalistic DBMS by implementing three key components crucial to any full-fledged database system. The project is structured into three progressive assignments (PAs), each focusing on a different aspect of database functionality.

PA1: Storage Manager
The first component involves creating a Storage Manager, which is the foundation of the database system. This module will handle the physical file storage on disk, allowing the database to read and write data blocks efficiently. Key functionalities will include initializing and managing a database file, ensuring data persistence by handling disk operations, and providing a mechanism to manage data blocks within the file.

PA2: Buffer Manager
The second component focuses on the Buffer Manager, which acts as a cache for the blocks read from the disk. The Buffer Manager's primary goal is to optimize database operations by reducing the number of disk accesses. This module will manage memory allocation for the blocks, implement replacement strategies for when the buffer is full, and ensure that modified data is eventually written back to the disk to maintain data integrity.

PA3: Record Manager
The final component is the Record Manager, which provides a higher level of abstraction for managing records within the database. This module will allow users to perform operations such as navigating between records, inserting new records, and deleting existing ones. The Record Manager will handle the organization of records in the database, provide support for different data types, and ensure that records are correctly stored and retrieved from the blocks managed by the Buffer Manager.
