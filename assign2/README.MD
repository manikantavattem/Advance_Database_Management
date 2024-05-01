*Assignment 2* 

Buffer Manager

Group 4
Lokesh
Dipen Reddy Patil
Gayatri Valluru
Manikanta Vatem

*Aim*:
To implement a robust Buffer Manager that efficiently manages the caching of disk pages in memory through the use of buffer pools. This involves creating a system that can handle multiple buffer pools concurrently, each with its own page replacement strategy to optimize memory usage and access times. The Buffer Manager must implement mechanisms for pinning and unpinning pages, handling dirty pages to ensure data integrity, and efficiently managing memory through strategies like FIFO and LRU. 

*Starting Steps:*
1)Clone the repository from Gitlab to the desired location.
2)Navigate to the "assign2" folder.
3)Execute the Makefile using the command make testcase.

*Functionality Description:*

*initBufferPool:* Initializes a buffer pool for caching disk pages in memory. It sets up the number of pages the buffer pool can hold and decides the page replacement strategy (like FIFO or LRU) to use.

*shutdownBufferPool:* Cleans up and releases all resources related to a buffer pool. It ensures any modified pages (dirty pages) are saved back to disk before closing, and checks that no pages are currently in use (pinned).

*forceFlushPool:* Forces all modified but not yet saved pages in the buffer pool to be written back to their respective disk files, provided they are not currently in use.

*pinPage:* Loads a specific page from disk into the buffer pool if it's not already there, and marks it as "in use" by increasing its pin count. This function ensures the requested page is available for reading or writing.

*unpinPage:* Marks a page as no longer in use by a client by decreasing its pin count, indicating it can be replaced if needed.

*markDirty:* Flags a page that has been modified (in memory) as "dirty," indicating it needs to be written back to disk to save changes.

*forcePage:* Immediately writes a specific modified page back to disk, ensuring any changes are not lost.

*getFrameContents:* Provides a snapshot of the current state of the buffer pool, showing which pages are loaded in which frame.

*getDirtyFlags:* Returns information on which pages in the buffer pool have been modified and not yet saved back to disk.

*getFixCounts:* Shows how many clients are currently using each page in the buffer pool, with a count for each page.

*getNumReadIO:* Reports the total number of disk reads performed since the buffer pool was initialized, highlighting the buffer pool's efficiency in reducing disk reads.

*getNumWriteIO:* Indicates the total number of disk writes performed since the buffer pool's initiation, reflecting how often the buffer pool saves changes back to disk.  

*Test Cases:*
The implementation is tested against various cases, including those in the test file (test_assign2_1). The program verifies these cases, ensuring there are no errors. Additionally, measures have been taken to prevent memory leaks in the design.


*Contributions:*

All team members collaborated on integrating the various components of the buffer manager, ensuring seamless interaction between different functionalities.

*Lokesh:* Led the design and implementation of the core buffer manager functionalities, including the initBufferPool and shutdownBufferPool functions. He helped design designed the overall structure for the buffer pool, including the initialization process and resource management during shutdown. It creates a buffer pool structure in memory, ready to load pages from disk as needed.

*Dipen:* Implemented the pinPage, unpinPage, markDirty, and forceFlushPool getFrameContents, getDirtyFlags, getFixCounts, getNumReadIO, and getNumWriteIO. These functions ensure efficient, reliable operation of the buffer pool, facilitating optimized data access and integrity within database systems.

*Gayatri:* Developed and integrated FIFO and LRU page replacement algorithms into the buffer manager where, FIFO should push out the oldest page in the buffer pool, while LRU should push out the least recently used page. 

*Manikanta:* Led the documentation efforts and developed robust error handling mechanisms, and helped create a comprehensive test suite.

All of us: Participated in regular meetings to discuss design decisions, review code, and resolve integration challenges and worked together on optimizing the buffer manager for performance and reliability and sharing insights.
    
