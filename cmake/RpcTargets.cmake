include_guard()

# 仅配置本项目的目标，不把警告选项传给依赖或使用者。
function(rpc_configure_target target)
    set_target_properties(${target} PROPERTIES
        CXX_STANDARD 17
        CXX_STANDARD_REQUIRED ON
        CXX_EXTENSIONS OFF
    )
    if(MSVC)
        target_compile_options(${target} PRIVATE /W4 /permissive-)
    elseif(CMAKE_CXX_COMPILER_ID MATCHES "^(GNU|Clang|AppleClang)$")
        target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic)
    endif()
endfunction()

function(rpc_add_executable target)
    add_executable(${target} ${ARGN})
    target_link_libraries(${target} PRIVATE cookrpc::rpc_common)
    rpc_configure_target(${target})
    # 保持脚本、文档和 sanitizer 驱动使用的可执行文件位置。
    set_target_properties(${target} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}"
    )
endfunction()
