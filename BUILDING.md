# Building with CMake

## Build

This project doesn't require any special command-line flags to build to keep
things simple.

Here are the steps for building in release mode with a single-configuration
generator, like the Unix Makefiles one:

```sh
cmake -S . -B build -D CMAKE_BUILD_TYPE=Release
cmake --build build
```

Here are the steps for building in release mode with a multi-configuration
generator, like the Visual Studio ones:

```sh
cmake -S . -B build
cmake --build build --config Release
```

But RPP is header-only library, so, without enabling any extra options is just compiles "nothing". So, you can use next options to enable/disable some parts of project:

- `RPP_BUILD_TESTS` - (ON/OFF) build unit tests (default OFF)
- `RPP_BUILD_EXAMPLES` - (ON/OFF) build examples of usage of RPP (default OFF)
- `RPP_BUILD_SFML_CODE` - (ON/OFF) build RPP code related to SFML or not (default OFF) - requires SFML to be installed
- `RPP_BUILD_QT_CODE` - (ON/OFF) build QT related code (examples/tests)(rppqt module doesn't require this one) (default OFF) - requires QT5/6 to be installed
- `RPP_BUILD_GRPC_CODE` - (ON/OFF) build gRPC related code (examples/tests)(rppgrpc module doesn't require this one) (default OFF) - requires gRPC++/protobuf to be installed
- `RPP_BUILD_ASIO_CODE` - (ON/OFF) build RPPASIO related code (examples/tests)(rppasio module doesn't require this one) (default OFF) - requires asio to be installed

By default, it provides rpp, rppqt, rppgrpc, rppasio INTERFACE modules.

## Install

This project doesn't require any special command-line flags to install to keep
things simple. As a prerequisite, the project has to be built with the above
commands already.

The below commands require at least CMake 3.15 to run, because that is the
version in which [Install a Project][2] was added.

Here is the command for installing the release mode artifacts with a
single-configuration generator, like the Unix Makefiles one:

```sh
cmake --install build
```

Here is the command for installing the release mode artifacts with a
multi-configuration generator, like the Visual Studio ones:

```sh
cmake --install build --config Release
```

Optionally you can download it via FetchContent

```cmake
Include(FetchContent)

FetchContent_Declare(
    RPP
    GIT_REPOSITORY https://github.com/AlexInLog/ReactivePlusPlus.git
    GIT_TAG        origin/v2
)

FetchContent_MakeAvailable(RPP)
```

### CMake package

This project exports a CMake package to be used with the [`find_package`][3]
command of CMake:

* Package name: `RPP`
* Target names:
 - `RPP::rpp` - main rpp INTERFACE target
 - `RPP::rppqt` - additional INTERFACE target to extend functionality for QT. rppqt doesn't include QT or even doesn't link with that itself.
 - `RPP::rppasio` - additional INTERFACE target to extend functionality for ASIO. rppasio doesn't include boost::asio or even doesn't link with that itself.
 - `RPP::rppgrpc` - additional INTERFACE target to extend functionality for gRPC. rppgrpc doesn't include gRPC or even doesn't link with that itself.

Example usage:

```cmake
find_package(RPP REQUIRED)
# Declare the imported target as a build requirement using PRIVATE, where
# project_target is a target created in the consuming project
target_link_libraries(project_target
    PRIVATE
        RPP::rpp
        RPP::rppqt
        RPP::rppasio
        RPP::rppgrpc
)
```

## Usage:

To use ReactivePlusPlus in your code just do
```cpp
#include <rpp/rpp.hpp>
```
to include whole library functionality and don't worry about includes. (same can be applied to rppgrpc or rppqt modules)
Also you can include only forwardings for everyting
```cpp
#include <rpp/fwd.hpp>
```
To avoid including "everyting" you can just include some iterested part of code (or forward it as well)
```cpp
#include <rpp/operators/map.hpp>
#include <rpp/observables/fwd.hpp>
```
**NOTE**: In case of partial including of operators you need to include headers for each used operators like
```cpp
#include <rpp/operators/map.hpp>
#include <rpp/operators/filter.hpp>

```
else it would not be possible to compile

[1]: https://cmake.org/download/
[2]: https://cmake.org/cmake/help/latest/manual/cmake.1.html#install-a-project
[3]: https://cmake.org/cmake/help/latest/command/find_package.html
