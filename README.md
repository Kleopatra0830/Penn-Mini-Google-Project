# PennGO - A Cloud-Based Search Engine

## Overview
PennGO is a robust and scalable cloud-based search engine developed as part of the CIS 5550 course at the University of Pennsylvania. Our system efficiently crawls, indexes, and ranks web pages, providing users with relevant search results through a user-friendly frontend. We focused on scalability, fault tolerance, and an optimized ranking algorithm to enhance performance.

## Features
### Core Functionality
- **Web Crawling**: Efficient crawling of web pages with URL prioritization and adherence to web standards.
- **Indexing & PageRank**: Data processing pipeline using FlameRDD to generate indices and PageRank scores.
- **Ranking Algorithm**: Combining TF-IDF, PageRank, and title matching for accurate search results.
- **Frontend Interface**: Google-like UI with features such as autocomplete, predictive search, and spell check.

### Advanced Features
- **Scalable Architecture**: Distributed key-value store (KVS) with replication for high availability.
- **Fault Tolerance**: System can recover from failures using state-saving techniques.
- **Parallel Processing**: Multi-threaded crawling and indexing to handle large-scale data.
- **Voice Search & Location-Based Results (Future Enhancements)**

## System Architecture
### 1. Search Service
- Stores indices, PageRank values, and raw web pages in a distributed storage cluster.
- Scalable by adding more workers and frontend servers.
- KVS Coordinator serves as a central control but may become a bottleneck at scale.

### 2. Data Processing Pipeline
- Uses FlameRDD and distributed workers for parallel execution.
- Steps:
  1. **Crawling** - Gathering raw data.
  2. **Indexing** - Generating structured indices and storing intermediate data.
  3. **PageRank Calculation** - Ranking pages based on links and relevance.
  4. **Final Storage** - Processed data stored in the search system database.

### 3. Ranking Algorithm
- Uses **TF-IDF**, **PageRank**, and **Title Match Score**.
- Final ranking score formula:
  
  ```
  Score = (TF-IDF * PageRank) + Title Match Score
  ```
- Results sorted in descending order of score.

## Performance Evaluation
### Single Node Performance
- **10 Threads**: ~2-3 pages/sec
- **20 Threads**: ~4-5 pages/sec
- **30 Threads**: ~6-7 pages/sec (plateau due to resource limits)

### Multi-Node Performance
- **2 Nodes (10 threads each)**: ~4-6 pages/sec (near-linear scaling)
- **3 Nodes (10 threads each)**: ~6-8 pages/sec
- **More Nodes**: Continued scaling, but performance varied based on website complexity.

### Search Performance
- Results consistently returned within ~3 seconds.
- High relevance of search results with no crashes observed.

## Setup and Deployment
### Prerequisites
Ensure you have:
- Java Development Kit (JDK)
- Apache Spark (for FlameRDD)
- AWS EC2 or local machine for deployment

### Running the Crawler
```sh
javac -cp "libs/*" src/cis5550/*/*.java
jar cvf crawler.jar -C src/ .
java -cp src cis5550.kvs.Coordinator 8000
rm -rf worker1; java -cp src cis5550.kvs.Worker 8001 worker1 localhost:8000
java -cp src cis5550.flame.Coordinator 9000 localhost:8000
java -cp src cis5550.flame.Worker 9001 localhost:9000
java -cp src cis5550.flame.FlameSubmit localhost:9000 crawler.jar cis5550.jobs.Crawler [seed url]
```

### Running the Indexer
```sh
javac -cp lib/webserver.jar:lib/kvs.jar:lib/flame.jar --source-path src src/cis5550/jobs/Indexer.java
jar cvf indexer.jar -C src .
java -cp lib/kvs.jar:lib/webserver.jar:classes cis5550.kvs.Coordinator 8000
java -cp lib/kvs.jar:lib/webserver.jar:classes cis5550.kvs.Worker 8001 worker1 localhost:8000
java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar:classes cis5550.flame.Coordinator 9000 localhost:8000
java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar:classes cis5550.flame.Worker 9001 localhost:9000
java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar cis5550.flame.FlameSubmit localhost:9000 indexer.jar cis5550.jobs.Indexer
```

### Running PageRank Calculation
```sh
javac -cp lib/webserver.jar:lib/kvs.jar:lib/flame.jar --source-path src src/cis5550/jobs/PageRank.java
jar cvf pagerank.jar -C src .
java -cp lib/kvs.jar:lib/webserver.jar:classes cis5550.kvs.Coordinator 8000
java -cp lib/kvs.jar:lib/webserver.jar:classes cis5550.kvs.Worker 8001 worker1 localhost:8000
java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar:classes cis5550.flame.Coordinator 9000 localhost:8000
java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar:classes cis5550.flame.Worker 9001 localhost:9000
java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar cis5550.flame.FlameSubmit localhost:9000 pagerank.jar cis5550.jobs.PageRank
```

### Running the Frontend Search Engine
```sh
javac -cp "libs/*" src/cis5550/*/*.java
java -cp "libs/*:src" cis5550.ranking.RankerDriver 2333 localhost:8000
```

## Deployment Information
### Search Service
- **Search Endpoint**: `penngo.cis5550.net:2333`
- **KVS Coordinator**: `penngo.cis5550.net:8000`
- **KVS Workers**: `penngo.cis5550.net:8001-8003`

### Data Processing (Now Closed)
- **KVS Coordinator 2**: `penngo.cis5550.net:2000`
- **Flame Coordinator**: `penngo.cis5550.net:3000`
- **Flame Workers**: `penngo.cis5550.net:3001-3003`

## Future Enhancements
- **Voice Search**: Enables search via voice commands.
- **Location-Based Search**: Results tailored to user location.
- **Improved JavaScript Handling**: Enhance crawler capability for dynamic web pages.
- **Optimized Memory Management**: Reduce overhead during high-load operations.

## Conclusion
PennGO successfully demonstrated an efficient, scalable, and distributed search engine architecture. Future improvements will focus on enhancing the crawler, refining ranking algorithms, and introducing advanced search features.


