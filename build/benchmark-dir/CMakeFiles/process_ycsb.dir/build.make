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
include benchmark-dir/CMakeFiles/process_ycsb.dir/depend.make

# Include the progress variables for this target.
include benchmark-dir/CMakeFiles/process_ycsb.dir/progress.make

# Include the compile flags for this target's objects.
include benchmark-dir/CMakeFiles/process_ycsb.dir/flags.make

benchmark-dir/CMakeFiles/process_ycsb.dir/process_ycsb.cc.o: benchmark-dir/CMakeFiles/process_ycsb.dir/flags.make
benchmark-dir/CMakeFiles/process_ycsb.dir/process_ycsb.cc.o: ../benchmark-dir/process_ycsb.cc
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/pcl_liwh/yangzheng/recv/faster/cc/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object benchmark-dir/CMakeFiles/process_ycsb.dir/process_ycsb.cc.o"
	cd /home/pcl_liwh/yangzheng/recv/faster/cc/build/benchmark-dir && /usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/process_ycsb.dir/process_ycsb.cc.o -c /home/pcl_liwh/yangzheng/recv/faster/cc/benchmark-dir/process_ycsb.cc

benchmark-dir/CMakeFiles/process_ycsb.dir/process_ycsb.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/process_ycsb.dir/process_ycsb.cc.i"
	cd /home/pcl_liwh/yangzheng/recv/faster/cc/build/benchmark-dir && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/pcl_liwh/yangzheng/recv/faster/cc/benchmark-dir/process_ycsb.cc > CMakeFiles/process_ycsb.dir/process_ycsb.cc.i

benchmark-dir/CMakeFiles/process_ycsb.dir/process_ycsb.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/process_ycsb.dir/process_ycsb.cc.s"
	cd /home/pcl_liwh/yangzheng/recv/faster/cc/build/benchmark-dir && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/pcl_liwh/yangzheng/recv/faster/cc/benchmark-dir/process_ycsb.cc -o CMakeFiles/process_ycsb.dir/process_ycsb.cc.s

# Object files for target process_ycsb
process_ycsb_OBJECTS = \
"CMakeFiles/process_ycsb.dir/process_ycsb.cc.o"

# External object files for target process_ycsb
process_ycsb_EXTERNAL_OBJECTS =

process_ycsb: benchmark-dir/CMakeFiles/process_ycsb.dir/process_ycsb.cc.o
process_ycsb: benchmark-dir/CMakeFiles/process_ycsb.dir/build.make
process_ycsb: benchmark-dir/CMakeFiles/process_ycsb.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/pcl_liwh/yangzheng/recv/faster/cc/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable ../process_ycsb"
	cd /home/pcl_liwh/yangzheng/recv/faster/cc/build/benchmark-dir && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/process_ycsb.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
benchmark-dir/CMakeFiles/process_ycsb.dir/build: process_ycsb

.PHONY : benchmark-dir/CMakeFiles/process_ycsb.dir/build

benchmark-dir/CMakeFiles/process_ycsb.dir/clean:
	cd /home/pcl_liwh/yangzheng/recv/faster/cc/build/benchmark-dir && $(CMAKE_COMMAND) -P CMakeFiles/process_ycsb.dir/cmake_clean.cmake
.PHONY : benchmark-dir/CMakeFiles/process_ycsb.dir/clean

benchmark-dir/CMakeFiles/process_ycsb.dir/depend:
	cd /home/pcl_liwh/yangzheng/recv/faster/cc/build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/pcl_liwh/yangzheng/recv/faster/cc /home/pcl_liwh/yangzheng/recv/faster/cc/benchmark-dir /home/pcl_liwh/yangzheng/recv/faster/cc/build /home/pcl_liwh/yangzheng/recv/faster/cc/build/benchmark-dir /home/pcl_liwh/yangzheng/recv/faster/cc/build/benchmark-dir/CMakeFiles/process_ycsb.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : benchmark-dir/CMakeFiles/process_ycsb.dir/depend
