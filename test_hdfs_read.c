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

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void get_stats(hdfsFile file);

int main(int argc, char **argv) {
    hdfsFS fs;
    const char *rfile = argv[1];
    tSize fileSize = strtoul(argv[2], NULL, 10);
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
    curSize = 0;
    int bytesRead = 0;
    for (; bytesRead < fileSize; bytesRead += curSize) {
        curSize = hdfsRead(fs, readFile, (void*)buffer, bufferSize);
        if (curSize < 0) {
            break;
        }
        buffer[curSize] = '\0';
	    printf("%s", buffer);
    }
    printf("\n");

    get_stats(readFile);
    

    free(buffer);
    hdfsCloseFile(fs, readFile);
    hdfsDisconnect(fs);

    printf("bye\n");
    return 0;
}


int do_gc() {
  JavaVM *vmBuf[1] = { NULL };
  jsize vmNum;
  JNI_GetCreatedJavaVMs(vmBuf, 1, &vmNum);
  if (vmNum < 1) {
    fprintf(stderr, "No JVM has been created\n");
    return -1;
  }

  JavaVM *vm = vmBuf[0];
  JNIEnv *env = NULL;
  JavaVMAttachArgs args = {
    .version = JNI_VERSION_1_8,
    .name = NULL,
    .group = NULL
  };
  (*vm)->AttachCurrentThread(vm, (void **) &env, &args); 
  if (env == NULL) {
    fprintf(stderr, "JNIEnv is null\n");
    return -1;
  }

  jclass    systemClass    = NULL;
  jmethodID systemGCMethod = NULL;
  jthrowable jthr = NULL;
  systemClass    = (*env)->FindClass(env, "java/lang/System");
  jthr = (*env)->ExceptionOccurred(env);
  if (jthr != NULL) {
    fprintf(stderr, "cannot locate java.lang.System\n");
    return -1;
  }

  systemGCMethod = (*env)->GetStaticMethodID(env, systemClass, "gc", "()V");
  jthr = (*env)->ExceptionOccurred(env);
  if (jthr != NULL) {
    fprintf(stderr, "cannot find method System.gc()\n");
    return -1;
  }

  (*env)->CallStaticVoidMethod(env, systemClass, systemGCMethod);
  jthr = (*env)->ExceptionOccurred(env);
  if (jthr != NULL) {
    fprintf(stderr, "System.gc() failed\n");
    return -1;
  }

  printf("System.gc() finished\n");
  return 0;
}

void get_stats(hdfsFile hdfs_file) {
  struct hdfsReadStatistics* stats;
  do_gc();
  int success = hdfsFileGetReadStatistics(hdfs_file, &stats);

  do_gc();
  if (success == 0) {
     printf("totalBytesRead=%d ", stats->totalBytesRead);
     printf("totalLocalBytesRead=%d ", stats->totalLocalBytesRead);
     printf("totalShortCircuitBytesRead=%d ", stats->totalShortCircuitBytesRead);
     printf("totalZeroCopyBytesRead=%d\n", stats->totalZeroCopyBytesRead);
     hdfsFileFreeReadStatistics(stats);
  } else {
     fprintf(stderr, "failed to get stats\n");
  }
  hdfsFileClearReadStatistics(hdfs_file);
}


/**
 * vim: ts=4: sw=4: et:
 */

