# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.12

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/local/bin/cmake

# The command to remove a file.
RM = /usr/local/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/pcl_liwh/yangzheng/recv/faster/cc

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/pcl_liwh/yangzheng/recv/faster/cc/build

# Include any dependencies generated for this target.
include test/CMakeFiles/str_parallel_test.dir/depend.make

# Include the progress variables for this target.
include test/CMakeFiles/str_parallel_test.dir/progress.make

# Include the compile flags for this target's objects.
include test/CMakeFiles/str_parallel_test.dir/flags.make

test/CMakeFiles/str_parallel_test.dir/str_parallel_test.cc.o: test/CMakeFiles/str_parallel_test.dir/flags.make
test/CMakeFiles/str_parallel_test.dir/str_parallel_test.cc.o: ../test/str_parallel_test.cc
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/pcl_liwh/yangzheng/recv/faster/cc/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object test/CMakeFiles/str_parallel_test.dir/str_parallel_test.cc.o"
	cd /home/pcl_liwh/yangzheng/recv/faster/cc/build/test && /usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/str_parallel_test.dir/str_parallel_test.cc.o -c /home/pcl_liwh/yangzheng/recv/faster/cc/test/str_parallel_test.cc

test/CMakeFiles/str_parallel_test.dir/str_parallel_test.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/str_parallel_test.dir/str_parallel_test.cc.i"
	cd /home/pcl_liwh/yangzheng/recv/faster/cc/build/test && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/pcl_liwh/yangzheng/recv/faster/cc/test/str_parallel_test.cc > CMakeFiles/str_parallel_test.dir/str_parallel_test.cc.i

test/CMakeFiles/str_parallel_test.dir/str_parallel_test.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/str_parallel_test.dir/str_parallel_test.cc.s"
	cd /home/pcl_liwh/yangzheng/recv/faster/cc/build/test && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/pcl_liwh/yangzheng/recv/faster/cc/test/str_parallel_test.cc -o CMakeFiles/str_parallel_test.dir/str_parallel_test.cc.s

# Object files for target str_parallel_test
str_parallel_test_OBJECTS = \
"CMakeFiles/str_parallel_test.dir/str_parallel_test.cc.o"

# External object files for target str_parallel_test
str_parallel_test_EXTERNAL_OBJECTS =

str_parallel_test: test/CMakeFiles/str_parallel_test.dir/str_parallel_test.cc.o
str_parallel_test: test/CMakeFiles/str_parallel_test.dir/build.make
str_parallel_test: libfaster.a
str_parallel_test: lib/libgtest.a
str_parallel_test: test/CMakeFiles/str_parallel_test.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/pcl_liwh/yangzheng/recv/faster/cc/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable ../str_parallel_test"
	cd /home/pcl_liwh/yangzheng/recv/faster/cc/build/test && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/str_parallel_test.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
test/CMakeFiles/str_parallel_test.dir/build: str_parallel_test

.PHONY : test/CMakeFiles/str_parallel_test.dir/build

test/CMakeFiles/str_parallel_test.dir/clean:
	cd /home/pcl_liwh/yangzheng/recv/faster/cc/build/test && $(CMAKE_COMMAND) -P CMakeFiles/str_parallel_test.dir/cmake_clean.cmake
.PHONY : test/CMakeFiles/str_parallel_test.dir/clean

test/CMakeFiles/str_parallel_test.dir/depend:
	cd /home/pcl_liwh/yangzheng/recv/faster/cc/build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/pcl_liwh/yangzheng/recv/faster/cc /home/pcl_liwh/yangzheng/recv/faster/cc/test /home/pcl_liwh/yangzheng/recv/faster/cc/build /home/pcl_liwh/yangzheng/recv/faster/cc/build/test /home/pcl_liwh/yangzheng/recv/faster/cc/build/test/CMakeFiles/str_parallel_test.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : test/CMakeFiles/str_parallel_test.dir/depend

