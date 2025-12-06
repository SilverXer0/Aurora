CXX      = clang++

GRPC_CFLAGS := $(shell pkg-config --cflags protobuf grpc++)
GRPC_LIBS   := $(shell pkg-config --libs protobuf grpc++)

HOMEBREW_PREFIX := $(shell brew --prefix)

CXXFLAGS = -std=c++20 -O2 -Wall -Wextra \
			-Wno-unused-parameter \
           -I./src -I./generated \
           $(GRPC_CFLAGS) \
           -I$(HOMEBREW_PREFIX)/include

LDFLAGS  = -L$(HOMEBREW_PREFIX)/lib
LDLIBS   = $(GRPC_LIBS) \
           -lrocksdb -lpthread

SRC_DIR      = src
GEN_DIR      = generated
PROTO_DIR    = proto

SOURCES = \
  $(SRC_DIR)/main.cc \
  $(SRC_DIR)/telemetry_service.cc \
  $(GEN_DIR)/telemetry.pb.cc \
  $(GEN_DIR)/telemetry.grpc.pb.cc

OBJECTS = $(SOURCES:.cc=.o)

TARGET = aurora

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJECTS) $(LDFLAGS) $(LDLIBS)

%.o: %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(GEN_DIR)/telemetry.pb.cc $(GEN_DIR)/telemetry.grpc.pb.cc: $(PROTO_DIR)/telemetry.proto
	mkdir -p $(GEN_DIR)
	protoc -I=$(PROTO_DIR) \
	  --cpp_out=$(GEN_DIR) \
	  --grpc_out=$(GEN_DIR) \
	  --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` \
	  $(PROTO_DIR)/telemetry.proto

.PHONY: clean
clean:
	rm -f $(OBJECTS) $(TARGET)