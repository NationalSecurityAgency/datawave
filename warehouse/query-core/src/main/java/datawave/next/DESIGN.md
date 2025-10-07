### Document Scheduler Design

The Document Scheduler splits query execution into two distinct phases, candidate search and 
candidate retrieval. The QueryIterator today both finds candidates and retrieves candidates 
in a single scan until the scan is complete or interrupted.

The Document Scheduler aims to reduce overall execution time by running a larger volume of 
scans that have a shorter lifespan. This should allow for greater resource utilization by 
providing more opportunities to time slice. This also allows fairer and more consistent 
access to the underlying resources, which prevents large, slow scans from affecting smaller,
faster scans.

The scheduler has three distinct thread pools: a small threadpool for consumers, a threadpool
for candidate search and a threadpool for candidate retrieval. Internal queues exist for 
candidate retrieval and results. 

- The QueryData iterator feeds the candidate search executor pool
- The candidate search pushes results to the candidate retrieval queue
- The candidate search consumer feeds document range scans to the candidate retrieval executor
- The document range scans push results, if any, to the result queue
