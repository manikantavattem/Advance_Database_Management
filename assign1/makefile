CC = gcc
CFLAGS = --std=c11 -Wall -pedantic
TARGET = test_assign1_1
SRC_FILES = dberror.c storage_mgr.c test_assign1_1.c
OBJ_FILES = $(SRC_FILES:.c=.o)
OBJ_DIR = obj
BIN_DIR = bin

.PHONY: all clean

all: $(BIN_DIR)/$(TARGET)

$(BIN_DIR)/$(TARGET): $(addprefix $(OBJ_DIR)/,$(OBJ_FILES))
	@mkdir -p $(@D)
	$(CC) $(CFLAGS) -o $@ $^

$(OBJ_DIR)/%.o: %.c
	@mkdir -p $(@D)
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	rm -rf $(OBJ_DIR)/* $(BIN_DIR)/*

test: $(BIN_DIR)/$(TARGET)
	@echo "Running test cases..."
	@./$(BIN_DIR)/$(TARGET)
