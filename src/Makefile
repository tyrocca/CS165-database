# Column store database - client and server
#
#   make              optimized build (-O2)
#   make debug        unoptimized build with AddressSanitizer + UBSan
#   make test         build, then run the end-to-end DSL tests
#   make fuzz         build, then run the randomized differential tests
#   make clean

CC ?= cc
O ?= 2
override CFLAGS += -std=c99 -D_DEFAULT_SOURCE -Wall -Wextra -pedantic -pthread -O$(O) -g -Iinclude -MMD -MP
LDFLAGS += -pthread

SANITIZE = -fsanitize=address,undefined -fno-omit-frame-pointer

CLIENT_OBJS = client.o utils.o
SERVER_OBJS = server.o parse.o utils.o db_manager.o client_context.o \
              db_operations.o db_persistance.o db_index.o extensible_hash_table.o

all: client server

client: $(CLIENT_OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

server: $(SERVER_OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

debug: clean
	$(MAKE) all O=0 CFLAGS="$(SANITIZE)" LDFLAGS="$(SANITIZE)"

test: all
	cd .. && ./run_tests.py --no-build

fuzz: all
	cd .. && tests/fuzz.py --no-build

clean:
	rm -f client server *.o *.d cs165_unix_socket
	rm -rf *.dSYM .deps database

-include $(SERVER_OBJS:.o=.d) $(CLIENT_OBJS:.o=.d)

.PHONY: all debug test fuzz clean
