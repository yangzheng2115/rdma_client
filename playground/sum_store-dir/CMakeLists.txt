set(SUM_STORE_HEADERS
        concurrent_recovery_test.h
        single_threaded_recovery_test.h
        sum_store.h
        )

add_executable(sum_store ${SUM_STORE_HEADERS} sum_store.cc)
add_executable(FH_MicroTest FH_MicroTest.cpp)
add_executable(FH_YCSBTest FH_YCSBTest.cpp)
add_executable(FH_ScanTest FH_ScanTest.cpp)
add_executable(YCSBTest YCSBTest.cpp)
add_executable(OneFileDCASTest OneFileDCASTest.cpp)
add_executable(EntryTest EntryTest.cpp)
add_executable(FH_NumaTest FH_NumaTest.cpp)
#add_executable(DummyTest DummyTest.cpp)
target_link_libraries(sum_store ${FASTER_BENCHMARK_LINK_LIBS})
target_link_libraries(FH_MicroTest ${FASTER_BENCHMARK_LINK_LIBS} -lnuma)
target_link_libraries(YCSBTest ${FASTER_BENCHMARK_LINK_LIBS})
#target_link_libraries(DummyTest ${FASTER_BENCHMARK_LINK_LIBS} -lnuma)
target_link_libraries(FH_YCSBTest ${FASTER_BENCHMARK_LINK_LIBS})
target_link_libraries(FH_NumaTest ${FASTER_BENCHMARK_LINK_LIBS} -lnuma)
target_link_libraries(FH_ScanTest ${FASTER_BENCHMARK_LINK_LIBS} -lnuma)
target_link_libraries(OneFileDCASTest ${FASTER_BENCHMARK_LINK_LIBS})
target_link_libraries(EntryTest ${FASTER_BENCHMARK_LINK_LIBS})

