link_libraries(pthread event event_core)

add_executable(multiport_network server.cpp kvobj.cpp assoc.cpp)

add_executable(simple_test simple_test.cpp kvobj.cpp)

target_link_libraries(multiport_network ${FASTER_BENCHMARK_LINK_LIBS} -lnuma)