#                  ReactivePlusPlus library
#
#          Copyright Aleksey Loginov 2023 - present.
# Distributed under the Boost Software License, Version 1.0.
#    (See accompanying file LICENSE_1_0.txt or copy at
#          https://www.boost.org/LICENSE_1_0.txt)
#
# Project home: https://github.com/AlexInLog/ReactivePlusPlus
#

add_subdirectory(utils)

macro(add_test_target target_name module files)
  set(TARGET ${target_name})

  add_executable(${TARGET} ${files})
  target_link_libraries(${TARGET} PRIVATE rpp_doctest_main trompeloeil::trompeloeil rpp_tests_utils ${module})
  set_target_properties(${TARGET} PROPERTIES FOLDER Tests/Suites/${module})

  add_test_with_coverage(${TARGET})

  if (${module} STREQUAL rppqt)
    rpp_add_qt_support_to_executable(${TARGET})
  endif()

  if (${module} STREQUAL rppgrpc)
    target_link_libraries(${TARGET} PRIVATE rppgrpc_tests_proto)
  endif()

  if (${module} STREQUAL rppasio)
    rpp_add_asio_support_to_executable(${TARGET})
  endif()

  target_compile_features(${TARGET} PRIVATE cxx_std_20)

  if(MSVC)
    target_compile_options(${TARGET} PRIVATE /W4 /WX /wd4702)
  else()
    target_compile_options(${TARGET} PRIVATE -Wall -Wextra -Wpedantic -Werror -Wconversion -Wno-gnu-zero-variadic-macro-arguments)
  endif()
endmacro()

macro(rpp_register_tests module)
  file(GLOB_RECURSE RPP_FILES "${module}/test_*.cpp")
  if (RPP_BUILD_TESTS_TOGETHER)
    add_test_target(test_${module} ${module} "${RPP_FILES}")
  else()
    foreach(SOURCE ${RPP_FILES})
      get_filename_component(BASE_NAME ${SOURCE} NAME_WE)
      add_test_target(${BASE_NAME} ${module} ${SOURCE})
    endforeach()
  endif()
endmacro()

rpp_register_tests(rpp)

if (RPP_BUILD_QT_CODE)
  rpp_register_tests(rppqt)
endif()

if (RPP_BUILD_GRPC_CODE)
  rpp_add_proto_target(rppgrpc_tests_proto rppgrpc/proto.proto)
  rpp_register_tests(rppgrpc)
endif()

if (RPP_BUILD_ASIO_CODE)
  rpp_register_tests(rppasio)
endif()
