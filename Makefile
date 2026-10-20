CC ?= gcc
CFLAGS ?= -std=c99 -O3 -Wall -Wextra -pedantic
LDFLAGS ?=

SRC_DIR = src
INC_DIR = include
BUILD_DIR = build
BIN_DIR = bin
TEST_DIR = tests
BENCH_DIR = bench

TARGET = $(BIN_DIR)/append-only-btree-engine
TEST_TARGET = $(BIN_DIR)/run_tests
BENCH_TARGET = $(BIN_DIR)/run_benchmarks

SRCS := $(wildcard $(SRC_DIR)/*.c)
OBJS := $(patsubst $(SRC_DIR)/%.c, $(BUILD_DIR)/%.o, $(SRCS))

TEST_SRCS := $(wildcard $(TEST_DIR)/*.c)
BENCH_SRCS := $(wildcard $(BENCH_DIR)/*.c)

.PHONY: all build test benchmark clean directories

all: directories build

directories:
	@mkdir -p $(BUILD_DIR) $(BIN_DIR)

build: directories $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) $(OBJS) -o $@ $(LDFLAGS)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -I$(INC_DIR) -c $< -o $@

test: directories
	@if [ -d "$(TEST_DIR)" ] && [ -n "$(TEST_SRCS)" ]; then \
		$(CC) $(CFLAGS) -I$(INC_DIR) $(TEST_SRCS) $(filter-out $(BUILD_DIR)/main.o, $(OBJS)) -o $(TEST_TARGET) $(LDFLAGS); \
		./$(TEST_TARGET); \
	else \
		echo "No tests found in $(TEST_DIR). Skipping."; \
	fi

benchmark: directories
	@if [ -d "$(BENCH_DIR)" ] && [ -n "$(BENCH_SRCS)" ]; then \
		$(CC) $(CFLAGS) -I$(INC_DIR) $(BENCH_SRCS) $(filter-out $(BUILD_DIR)/main.o, $(OBJS)) -o $(BENCH_TARGET) $(LDFLAGS); \
		./$(BENCH_TARGET); \
	else \
		echo "No benchmarks found in $(BENCH_DIR). Skipping."; \
	fi

clean:
	@rm -rf $(BUILD_DIR) $(BIN_DIR)
	@find . -type f -name '*.o' -delete
	@find . -type f -name '*.d' -delete