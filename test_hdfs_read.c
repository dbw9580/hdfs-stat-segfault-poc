/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "hdfs.h" 

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void get_stats(hdfsFile file);

int main(int argc, char **argv) {
    hdfsFS fs;
    const char *rfile = argv[1];
    tSize bufferSize = strtoul(argv[3], NULL, 10);
    hdfsFile readFile;
    char* buffer;
    tSize curSize;

    if (argc != 4) {
        fprintf(stderr, "Usage: hdfs_read <filename> <filesize> <buffersize>\n");
        exit(-1);
    }

    struct hdfsBuilder *builder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(builder, rfile);
    
    fs = hdfsBuilderConnect(builder);
    if (!fs) {
        fprintf(stderr, "Oops! Failed to connect to hdfs!\n");
        exit(-1);
    } 

    readFile = hdfsOpenFile(fs, rfile, O_RDONLY, bufferSize, 0, 0);
    if (!readFile) {
        fprintf(stderr, "Failed to open %s for writing!\n", rfile);
        exit(-2);
    }

    // data to be written to the file
    buffer = malloc(sizeof(char) * (bufferSize + 1));
    if(buffer == NULL) {
        return -2;
    }
    memset(buffer, 0, bufferSize + 1);
    
    // read from the file
    curSize = bufferSize;
    for (; curSize == bufferSize;) {
        curSize = hdfsRead(fs, readFile, (void*)buffer, curSize);
        buffer[curSize] = '\0';
	    printf("%s", buffer);
    }
    printf("\n");

    //get_stats(readFile);
    

    free(buffer);
    hdfsCloseFile(fs, readFile);
    hdfsDisconnect(fs);

    return 0;
}

void get_stats(hdfsFile hdfs_file) {
  struct hdfsReadStatistics* stats;
  int success = hdfsFileGetReadStatistics(hdfs_file, &stats);
  if (success == 0) {
     printf("totalBytesRead=%d ", stats->totalBytesRead);
     printf("totalLocalBytesRead=%d ", stats->totalLocalBytesRead);
     printf("totalShortCircuitBytesRead=%d ", stats->totalShortCircuitBytesRead);
     printf("totalZeroCopyBytesRead=%d\n", stats->totalZeroCopyBytesRead);
  } else {
     fprintf(stderr, "failed to get stats\n");
  }
  hdfsFileFreeReadStatistics(stats);
  hdfsFileClearReadStatistics(hdfs_file);
}


/**
 * vim: ts=4: sw=4: et:
 */

