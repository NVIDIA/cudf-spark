/**
 * Copyright (c) 2022, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <new>
#include <stdio.h>
#include <execinfo.h>
#include <cstdlib>
#include <pthread.h>

static bool include_traces = getenv("LEAK_DETECT_TRACE") != nullptr;
static pthread_mutex_t mutex_lock;

extern "C" {
  void init_leak_detect() {
    pthread_mutex_init(&mutex_lock, NULL);
  }
}

static void print_trace(unsigned long this_id) {
    if (include_traces) {
      char **strings;
      size_t i, size;
      enum Constexpr { MAX_SIZE = 48 };
      void *array[MAX_SIZE];
      size = backtrace(array, MAX_SIZE);
      strings = backtrace_symbols(array, size);
      for (i = 0; i < size; i++) {
        fprintf(stderr, "\nTRACE\t%lu\t%s\t%lu\n", i, strings[i], this_id);
      }
      free(strings);
    }
}

void* operator new (std::size_t size) {
  void *ptr = malloc(size);
  if (ptr == nullptr) {
    throw std::bad_alloc();
  }
  auto this_id = pthread_self();
  pthread_mutex_lock(&mutex_lock);
  fprintf(stderr, "\nTRACK\t%p\tMALLOC\t%lu\t%lu\n", ptr, size, this_id);
  print_trace(this_id);
  fflush(stderr);
  pthread_mutex_unlock(&mutex_lock);
  return ptr;
}

void* operator new (std::size_t size, const std::nothrow_t& nothrow_value) noexcept {
  void *ptr = malloc(size);
  if (ptr != nullptr) {
    auto this_id = pthread_self();
    pthread_mutex_lock(&mutex_lock);
    fprintf(stderr, "\nTRACK\t%p\tMALLOC\t%lu\t%lu\n", ptr, size, this_id);
    print_trace(this_id);
    fflush(stderr);
    pthread_mutex_unlock(&mutex_lock);
  }
  return ptr;
}

//void* operator new (std::size_t size, void* ptr) noexcept;

void* operator new[] (std::size_t size) {
  void *ptr = malloc(size);
  if (ptr == nullptr) {
    throw std::bad_alloc();
  }
  auto this_id = pthread_self();
  pthread_mutex_lock(&mutex_lock);
  fprintf(stderr, "\nTRACK\t%p\tMALLOC\t%lu\t%lu\n", ptr, size, this_id);
  print_trace(this_id);
  fflush(stderr);
  pthread_mutex_unlock(&mutex_lock);
  return ptr;
}

void* operator new[] (std::size_t size, const std::nothrow_t& nothrow_value) noexcept {
  void *ptr = malloc(size);
  if (ptr != nullptr) {
    auto this_id = pthread_self();
    pthread_mutex_lock(&mutex_lock);
    fprintf(stderr, "\nTRACK\t%p\tMALLOC\t%lu\t%lu\n", ptr, size, this_id);
    print_trace(this_id);
    fflush(stderr);
    pthread_mutex_unlock(&mutex_lock);
  }
  return ptr;
}

//void* operator new[] (std::size_t size, void* ptr) noexcept;

void operator delete (void* ptr) noexcept {
  if (ptr != nullptr) {
    pthread_mutex_lock(&mutex_lock);
    fprintf(stderr, "\nTRACK\t%p\tFREE\n", ptr);
    fflush(stderr);
    pthread_mutex_unlock(&mutex_lock);
  }
  free(ptr);
}

void operator delete (void* ptr, const std::nothrow_t& nothrow_constant) noexcept {
  if (ptr != nullptr) {
    pthread_mutex_lock(&mutex_lock);
    fprintf(stderr, "\nTRACK\t%p\tFREE\n", ptr);
    fflush(stderr);
    pthread_mutex_unlock(&mutex_lock);
  }
  free(ptr);
}

//void operator delete (void* ptr, void* voidptr2) noexcept;

void operator delete[] (void* ptr) noexcept {
  if (ptr != nullptr) {
    pthread_mutex_lock(&mutex_lock);
    fprintf(stderr, "\nTRACK\t%p\tFREE\n", ptr);
    fflush(stderr);
    pthread_mutex_unlock(&mutex_lock);
  }
  free(ptr);
}

void operator delete[] (void* ptr, const std::nothrow_t& nothrow_constant) noexcept {
  if (ptr != nullptr) {
    pthread_mutex_lock(&mutex_lock);
    fprintf(stderr, "\nTRACK\t%p\tFREE\n", ptr);
    fflush(stderr);
    pthread_mutex_unlock(&mutex_lock);
  }
  free(ptr);
}

//void operator delete[] (void* ptr, void* voidptr2) noexcept;



