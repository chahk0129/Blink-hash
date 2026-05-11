# Makefile for pg_blinkhash PostgreSQL extension
# Uses PGXS build infrastructure

MODULE_big = blinkhash
EXTENSION  = blinkhash
DATA       = blinkhash--1.0.sql

# -- PostgreSQL C sources --
SRCS_C = src/blinkhash_handler.c \
         src/blinkhash_build.c \
         src/blinkhash_insert.c \
         src/blinkhash_scan.c \
         src/blinkhash_vacuum.c \
         src/blinkhash_cost.c \
         src/blinkhash_validate.c \
         src/blinkhash_utils.c \
         src/blinkhash_am.c \
         src/blinkhash_page.c
SRCS_C += src/blinkhash_wal.c
OBJS = $(SRCS_C:.c=.o)

# -- C++ bridge source --
SRCS_CXX = src/blinkhash_core.cpp src/shared_mmap.cpp
CXX_OBJS = $(SRCS_CXX:.cpp=.o)
OBJS += $(CXX_OBJS)

# -- B^link-hash core C++ library (linked as a static lib) --
SHLIB_LINK += -L../build/lib -L../build/wal -L/opt/homebrew/opt/tbb/lib -lblinkhash -lblinkhash_wal -lstdc++ -ltbb

# -- Include paths --
PG_CPPFLAGS += -I../lib -I../wal -Isrc
PG_CPPFLAGS += -DBH_USE_PG_WAL

ifdef USE_BUFFER_POOL
PG_CPPFLAGS += -DBH_USE_BUFFER_POOL
endif
# -- PGXS --
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# -- Compile C++ bridge with clang++ --
src/blinkhash_core.o: src/blinkhash_core.cpp
	$(CXX) -std=c++17 -fPIC $(PG_CPPFLAGS) -I$(shell $(PG_CONFIG) --includedir-server) -I../lib -I../wal -I$(shell brew --prefix tbb)/include -DFINGERPRINT -DSAMPLING -DLINKED -c -o $@ $<

ifdef USE_SHARED_MEMORY
PG_CPPFLAGS += -DBH_USE_SHARED_MEMORY
endif
