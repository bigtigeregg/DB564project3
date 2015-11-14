cmake_minimum_required(VERSION 3.3)
project(BufMgr)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES
    src/exceptions/bad_buffer_exception.cpp
    src/exceptions/bad_buffer_exception.h
    src/exceptions/badgerdb_exception.cpp
    src/exceptions/badgerdb_exception.h
    src/exceptions/buffer_exceeded_exception.cpp
    src/exceptions/buffer_exceeded_exception.h
    src/exceptions/file_exists_exception.cpp
    src/exceptions/file_exists_exception.h
    src/exceptions/file_not_found_exception.cpp
    src/exceptions/file_not_found_exception.h
    src/exceptions/file_open_exception.cpp
    src/exceptions/file_open_exception.h
    src/exceptions/hash_already_present_exception.cpp
    src/exceptions/hash_already_present_exception.h
    src/exceptions/hash_not_found_exception.cpp
    src/exceptions/hash_not_found_exception.h
    src/exceptions/hash_table_exception.cpp
    src/exceptions/hash_table_exception.h
    src/exceptions/insufficient_space_exception.cpp
    src/exceptions/insufficient_space_exception.h
    src/exceptions/invalid_page_exception.cpp
    src/exceptions/invalid_page_exception.h
    src/exceptions/invalid_record_exception.cpp
    src/exceptions/invalid_record_exception.h
    src/exceptions/invalid_slot_exception.cpp
    src/exceptions/invalid_slot_exception.h
    src/exceptions/page_not_pinned_exception.cpp
    src/exceptions/page_not_pinned_exception.h
    src/exceptions/page_pinned_exception.cpp
    src/exceptions/page_pinned_exception.h
    src/exceptions/slot_in_use_exception.cpp
    src/exceptions/slot_in_use_exception.h
    src/buffer.cpp
    src/buffer.h
    src/bufHashTbl.cpp
    src/bufHashTbl.h
    src/file.cpp
    src/file.h
    src/file_iterator.h
    src/main.cpp
    src/main.hpp
    src/page.cpp
    src/page.h
    src/page_iterator.h
    src/types.h)

add_executable(BufMgr ${SOURCE_FILES})