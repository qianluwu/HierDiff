CXX=g++
CXXFLAGS= -I. -std=c++17 -Ofast -pthread
TARGET=main
SRC=main.cpp

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) $< -o $@

clean:
	rm -f $(TARGET)