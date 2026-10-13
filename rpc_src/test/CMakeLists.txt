find_package(GTest REQUIRED)
find_package(Python3 REQUIRED COMPONENTS Interpreter)
include(GoogleTest)

# 仅替换指定符号来注入故障或模拟发现，保留未替换的真实网络实现。
function(rpc_wrap_symbols target)
    foreach(symbol IN LISTS ARGN)
        target_link_options(${target} PRIVATE "LINKER:--wrap=${symbol}")
    endforeach()
endfunction()

# 每个测试用例单独注册到 CTest，可按名称筛选并设置独立超时。
function(rpc_add_gtest target)
    cmake_parse_arguments(TEST "" "TIMEOUT" "SOURCES;WRAP" ${ARGN})
    if(TEST_UNPARSED_ARGUMENTS OR NOT TEST_SOURCES)
        message(FATAL_ERROR "Invalid arguments for test target ${target}")
    endif()
    rpc_add_executable(${target} ${TEST_SOURCES})
    target_link_libraries(${target} PRIVATE GTest::gtest_main)
    rpc_wrap_symbols(${target} ${TEST_WRAP})
    if(TEST_TIMEOUT)
        gtest_discover_tests(${target} PROPERTIES TIMEOUT "${TEST_TIMEOUT}")
    else()
        gtest_discover_tests(${target})
    endif()
endfunction()

rpc_add_gtest(load_balancer_test SOURCES load_balancer_test.cc)
rpc_add_gtest(serializer_test SOURCES serializer_test.cc)
rpc_add_gtest(rpc_protocol_test SOURCES rpc_protocol_test.cc)
rpc_add_gtest(transport_controls_test SOURCES transport_controls_test.cc)
rpc_add_gtest(service_test SOURCES service_test.cc)
rpc_add_gtest(thread_pool_test SOURCES thread_pool_test.cc TIMEOUT 30)
rpc_add_gtest(aes_encrypt_test SOURCES aes_encrypt_test.cc)
rpc_add_gtest(rpc_client_config_test SOURCES rpc_client_config_test.cc)
rpc_add_gtest(server_config_test SOURCES server_config_test.cc)
rpc_add_gtest(utils_test SOURCES utils_test.cc)
rpc_add_gtest(zstd_compress_test SOURCES zstd_compress_test.cc)

add_test(NAME scripts_regression
    COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/scripts_test.py")

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    # 只替换 ZooKeeper C API；网络和并发代码仍是生产实现。
    set(RPC_DISCOVERY_WRAPS
        zookeeper_init zookeeper_close zoo_get_children zoo_get deallocate_String_vector)

    rpc_add_gtest(network_test SOURCES network_test.cc TIMEOUT 30
        WRAP ${RPC_DISCOVERY_WRAPS} poll accept epoll_wait)
    rpc_add_gtest(service_registry_test SOURCES service_registry_test.cc TIMEOUT 30
        WRAP zookeeper_init zookeeper_close zoo_exists zoo_wexists zoo_get
            zoo_create zoo_client_id zoo_acreate zoo_aexists zoo_awexists zoo_aget)
    rpc_add_gtest(zk_conn_handler_test SOURCES zk_conn_handler_test.cc
        WRAP ${RPC_DISCOVERY_WRAPS} zoo_exists zoo_create)

    rpc_add_executable(stress_test_client_probe
        "${PROJECT_SOURCE_DIR}/rpc_src/tools/stress_test_client.cc"
        stress_test_backend.cc)
    rpc_wrap_symbols(stress_test_client_probe ${RPC_DISCOVERY_WRAPS})
    add_test(NAME stress_client_regression
        COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/stress_client_test.py"
            --client "$<TARGET_FILE:stress_test_client_probe>")
    set_tests_properties(stress_client_regression PROPERTIES TIMEOUT 60)

    if(RPC_ENABLE_LIVE_TESTS)
        rpc_add_executable(live_integration_test live_integration_test.cc)
        add_test(NAME live_tcp_zookeeper
            COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/live_test.py"
                --driver "$<TARGET_FILE:live_integration_test>")
        set_tests_properties(live_tcp_zookeeper PROPERTIES TIMEOUT 120 SKIP_RETURN_CODE 77)
    endif()
endif()
