all: example

example: example.cc
	$(CXX) -o $@ $(CXXFLAGS) --std=c++17 $< $$(pkg-config --cflags --libs arrow)

