/*
 * Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <string>

namespace pstd {

/*
 * InputMemoryFile is used to abstract the
 *  operation of reading file
 * content at the memory level.
 * Refer to system calls such as mmap.
 */
class InputMemoryFile {
 public:
  /*------------------------
   * InputMemoryFile()
   * Initialize some private value.
   */
  InputMemoryFile();

  /*------------------------
   * ~InputMemoryFile()
   * Destroy a memory file's instance, close the file
   * and free the memory.
   */
  ~InputMemoryFile();

  /*------------------------
   * Open()
   * Open a file and establish a mapping
   * relationship in memory.
   */
  bool Open(const char* file);

  /*------------------------
   * Close()
   * Close a file and terminate the memory mapping
   */
  void Close();

  /*------------------------
   * Read(std::size_t& len)
   * Load the content of the file starting from byte
   * length, and automatically expand the file
   * when the size is exceeded.
   */
  const char* Read(std::size_t& len);

  // Read the buffer using pointers of different sizes.
  template <typename T>
  T Read();

  /*------------------------
   * Skip(std::size_t len)
   * Move the offset pointer backward.
   */
  void Skip(std::size_t len);

  /*------------------------
   * IsOpen()
   * Is fd valid?
   */
  bool IsOpen() const;

 private:
  /*------------------------
   * MapReadOnly()
   * Establish a mapping relation between memory and file.
   */
  bool MapReadOnly();

  // The fd
  int file_ = -1;
  // The memory of file
  char* pMemory_ = nullptr;
  // The file offset_ at memory
  std::size_t offset_ = 0;
  // The size of file, can extend
  std::size_t size_ = 0;
};

template <typename T>
inline T InputMemoryFile::Read() {
  T res(*reinterpret_cast<T*>(pMemory_ + offset_));
  offset_ += sizeof(T);

  return res;
}

/*
 * OutputMemoryFile is a wrapper for system calls such as mmap,
 * supporting file writing as well as capacity adjustment,
 * thereby making operations on files on disk at the memory level simple and easy.
 */
class OutputMemoryFile {
 public:
  /*------------------------
   * OutputMemoryFile()
   * Initialize some private value.
   */
  OutputMemoryFile();
  
  /*------------------------
   * ~OutputMemoryFile()
   * Close file and free the memory.
   * If fd is invilad, do nothing.
   */
  ~OutputMemoryFile();

  /*------------------------
   * Open(const std::string& file, bool bAppend = true)
   * Open a file and establish a mapping relation.
   */
  bool Open(const std::string& file, bool bAppend = true);

  /*------------------------
   * Open(const char* file, bool bAppend = true)
   * Open a file and establish a mapping relation.
   */
  bool Open(const char* file, bool bAppend = true);

  /*------------------------
   * Close()
   * Close file and end the mapping relation.
   */
  void Close();
  
  /*------------------------
   * Sync()
   * Synchronize memory content to disk.
   */
  bool Sync();

  /*------------------------
   * Truncate(std::size_t size)
   * Adjust the file block size to the specified bytes 
   * and rebuild the mapping relationship.
   */
  void Truncate(std::size_t size);

  /*------------------------
   * TruncateTailZero()
   * Equal to Truncate(0), if process terminated abnormally,
   * erase the trash data.
   */ 
  void TruncateTailZero();

  /*------------------------
   * Write(const void* data, std::size_t len)
   * Write the content of the specified length into the buffer 
   * and adjust the offset.
   */ 
  void Write(const void* data, std::size_t len);

  // Write data to file buffer and adjust the offset.
  template <typename T>
  void Write(const T& t);

  /*------------------------
   * Offset()
   * Return the offset of the file buffer.
   */
  std::size_t Offset() const { return offset_; }

  /*------------------------
   * IsOpen()
   * Is fd valid?
   */
  bool IsOpen() const;

 private:
  /*------------------------
   * MapWriteOnly()
   * Build the mapping relation.
   */
  bool MapWriteOnly();

  /*------------------------
   * ExtendFileSize(std::size_t size)
   * Equal to Truncate(size).
   */
  void ExtendFileSize(std::size_t size);

  /*------------------------
   * AssureSpace(std::size_t size)
   * Make sure there's enough space for data to write.
   * If not, extend the buffer size.
   */
  void AssureSpace(std::size_t size);

  // fd
  int file_ = -1;
  
  // The file content buffer
  char* pMemory_ = nullptr;

  // The offset of the buffer
  std::size_t offset_ = 0;

  // The size of file
  std::size_t size_ = 0;

  // The sync position of the buffer
  std::size_t syncPos_ = 0;
};

template <typename T>
inline void OutputMemoryFile::Write(const T& t) {
  this->Write(&t, sizeof t);
}

}  // namespace pstd
