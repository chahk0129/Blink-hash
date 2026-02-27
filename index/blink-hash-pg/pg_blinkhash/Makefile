# Makefile for pg_blinkhash PostgreSQL extension
# Uses PGXS build infrastructure

MODULE_big = blinkhash
EXTENSION  = blinkhash
DATA       = blinkhash--1.0.sql

# ── PostgreSQL C sources ──
SRCS_C = src/blinkhash_handler.c \
         src/blinkhash_build.c \
         src/blinkhash_insert.c \
         src/blinkhash_scan.c \
         src/blinkhash_vacuum.c \
         src/blinkhash_cost.c \
         src/blinkhash_validate.c \
         src/blinkhash_utils.c \
         src/blinkhash_wal.c \
         src/blinkhash_page.c

OBJS = $(SRCS_C:.c=.o)

# ── B^link-hash core C++ library (linked as a static lib) ──
# The core library is built separately via CMake in ../lib/
# and linked here as libblinkhash_core.a
SHLIB_LINK += -L../build/lib -lblinkhash_core -lstdc++ -ltbb

# ── Include paths ──
PG_CPPFLAGS += -I../lib -I../wal -Isrc

# ── PGXS ──
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# ── Custom target: build the core C++ library first ──
.PHONY: core
core:
	cd .. && mkdir -p build && cd build && cmake .. && make -j

# Make the PG extension depend on the core library
$(OBJS): core
