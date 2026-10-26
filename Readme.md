# ReactivePlusPlus

[![CI v2](https://github.com/AlexInLog/ReactivePlusPlus/actions/workflows/ci%20v2.yml/badge.svg?branch=v2)](https://github.com/AlexInLog/ReactivePlusPlus/actions/workflows/ci%20v2.yml)
[![codecov](https://codecov.io/gh/AlexInLog/ReactivePlusPlus/branch/v2/graph/badge.svg?token=INEHPRF18E)](https://app.codecov.io/gh/AlexInLog/ReactivePlusPlus/tree/v2)
[![Lines of Code](https://sonarcloud.io/api/project_badges/measure?project=AlexInLog_ReactivePlusPlus&metric=ncloc&branch=v2)](https://sonarcloud.io/summary/new_code?id=AlexInLog_ReactivePlusPlus&branch=v2)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=AlexInLog_ReactivePlusPlus&metric=sqale_rating&branch=v2)](https://sonarcloud.io/summary/new_code?id=AlexInLog_ReactivePlusPlus&branch=v2)
![GitHub commit activity (v2)](https://img.shields.io/github/commit-activity/m/AlexInLog/ReactivePlusPlus/v2)

[![User guide](https://img.shields.io/badge/User_guide-link-green)](https://AlexInLog.github.io/ReactivePlusPlus/v2/docs/html/index.html)
[![API Reference](https://img.shields.io/badge/API_Reference-link-green)](https://AlexInLog.github.io/ReactivePlusPlus/v2/docs/html/topics.html)
[![Benchmarks](https://img.shields.io/badge/Benchmarks-link-green)](https://AlexInLog.github.io/ReactivePlusPlus/v2/benchmark)

[![Join the chat in Discord: https://discord.gg/KWMR3RNkVz](https://img.shields.io/badge/Discord-Chat!-brightgreen.svg)](https://discord.gg/KWMR3RNkVz)
[![Conan Center](https://img.shields.io/conan/v/reactiveplusplus)](https://conan.io/center/recipes/reactiveplusplus)
![Vcpkg Version](https://img.shields.io/vcpkg/v/reactiveplusplus)


## What is that?

ReactivePlusPlus is reactive programming library for C++20 language inspired by "official implementation" ([RxCpp](https://github.com/ReactiveX/RxCpp)) and original idea ([ReactiveX](https://reactivex.io/)) that only depends on standard library and C++20 features (mostly on [concepts](https://en.cppreference.com/w/cpp/language/constraints)).


### But... what is that?

To put it even simpler: ReactivePlusPlus is library for building asynchronous event-driven streams of data with help of sequences of primitive operators in the declarative form. Like this:

```cpp
rpp::source::from_callable(&::getchar)
   | rpp::operators::repeat()
   | rpp::operators::take_while([](char v) { return v != '0'; })
   | rpp::operators::filter(std::not_fn(&::isdigit))
   | rpp::operators::map(&::toupper)
   | rpp::operators::subscribe([](char v) { std::cout << v; });
```
[Try it on godbolt!](https://godbolt.org/#g:!((g:!((g:!((h:codeEditor,i:(filename:'1',fontScale:14,fontUsePx:'0',j:1,lang:c%2B%2B,selection:(endColumn:65,endLineNumber:12,positionColumn:1,positionLineNumber:8,selectionStartColumn:65,selectionStartLineNumber:12,startColumn:1,startLineNumber:8),source:'%23include+%3Crpp/rpp.hpp%3E%0A%23include+%3Ciostream%3E%0A%23include+%3Cfunctional%3E%0A%0Aint+main()%0A%7B%0A++++rpp::source::from_callable(%26::getchar)%0A++++%7C+rpp::operators::repeat()%0A++++%7C+rpp::operators::take_while(%5B%5D(char+v)+%7B+return+v+!!%3D+!'0!'%3B+%7D)%0A++++%7C+rpp::operators::filter(std::not_fn(%26::isdigit))%0A++++%7C+rpp::operators::map(%26::toupper)%0A++++%7C+rpp::operators::subscribe(%5B%5D(char+v)+%7B+std::cout+%3C%3C+v%3B+%7D)%3B%0A++++return+0%3B%0A%7D'),l:'5',n:'1',o:'C%2B%2B+source+%231',t:'0')),k:60.849967804249836,l:'4',m:100,n:'0',o:'',s:0,t:'0'),(g:!((h:executor,i:(argsPanelShown:'1',compilationPanelShown:'0',compiler:g132,compilerName:'',compilerOutShown:'0',execArgs:'',execStdin:'He11lLo+%23@!!$+W%23oRl@123d+!!0001123W',fontScale:14,fontUsePx:'0',j:1,lang:c%2B%2B,libs:!((name:reactive_plus_plus,ver:v2)),options:'-std%3Dc%2B%2B20',overrides:!(),runtimeTools:!(),source:1,stdinPanelShown:'0',wrap:'1'),l:'5',n:'0',o:'Executor+x86-64+gcc+13.2+(C%2B%2B,+Editor+%231)',t:'0')),header:(),k:39.150032195750164,l:'4',n:'0',o:'',s:0,t:'0')),l:'2',n:'0',o:'',t:'0')),version:4)


<details><summary>Explanation</summary>
There we are creating observable (soure of emissions/values/data) to emit value via invoking of `getchar` function, `repeat`-ing it infinite amount of time till termination event happening. It emits values while symbol is not equal to `0`, taking only **not** digits, maping them to upper case and then just printing to console.
</details>


Also RPP supports out of box:
-  QT as rppqt module. Checkout [RPPQT reference](https://AlexInLog.github.io/ReactivePlusPlus/v2/docs/html/group__rppqt.html).
-  gRPC as rppgrpc module. Checkout [RPPgRPC reference](https://AlexInLog.github.io/ReactivePlusPlus/v2/docs/html/group__rppgrpc.html).
-  boost::asio as rppasio module. Checkout [RPPASIO reference](https://AlexInLog.github.io/ReactivePlusPlus/v2/docs/html/group__rppasio.html).

## What about existing Reactive Extension libraries for C++?

Reactive programming is a powerful paradigm for creating multi-threaded and real-time applications. Unfortunately, at the moment of creating ReactivePlusPlus, there is only one stable and fully-implemented library available - [RxCpp](https://github.com/ReactiveX/RxCpp).

While RxCpp is a great and powerful library, it has some disadvantages. It is written in C++11 and contains parts written in pre-C++11 style. This leads to a mess of old-style classes and wrappers. Additionally, the `rxcpp::observable` type has a long chain of template parameters, which can lead to slower IDE performance and larger binaries. Sometimes it has bad performance due to tremendous amount of usage of heap or just non-effective logic.

Another implementation, [another-rxcpp](https://github.com/CODIANZ/another-rxcpp), uses type erasure and `std::function`, but this approach results in larger observer/observable sizes and potentially causes performance issues.

### Why ReactivePlusPlus?

**ReactivePlusPlus** tries to solve all mentioned issues:
- **ReactivePlusPlus** written in **Modern C++ (C++20)** with concepts which makes code-base a lot more understandable and clean:
   - Concepts provide more clear errors and checks: you will understand that pass something incorrect before compilation in IDE or during compilation with understandable errors instead of _"invalid template class map_invalid_t"_
   - Everywhere while possible used deduction of template arguments, for example, type of values of observable by type of subscriber used in on_subscribe and etc
- **ReactivePlusPlus** keeps balance between performance and type-erasing mechanism
- **ReactivePlusPlus** is fast: every part of code written with performance in mind. Starting from tests over amount of copies/move and finishing to Continuous Benchmarking. Benchmarks prove that RPP faster than RxCPP in most cases: [Continuous benchmarking results and comparison with RxCpp](https://AlexInLog.github.io/ReactivePlusPlus/v2/benchmark)

Currently ReactivePlusPlus is still under development but it has a lot of implemented operators for now. List of implemented features can be found in [API Reference](https://AlexInLog.github.io/ReactivePlusPlus/v2/docs/html/topics.html) with very detailed documentation for each of them.

Main advantages of ReactivePlusPlus are that it is written in Modern C++ with Performance and Usage in mind. v2 is written to follow the [**"zero-overhead principle"**](https://en.cppreference.com/w/cpp/language/Zero-overhead_principle). As a result, it is fast, readable, easy to use, and well-documented. This is proven by [continuous benchmarking results of v2 and comparison with RxCpp](https://AlexInLog.github.io/ReactivePlusPlus/v2/benchmark).

## Usage

See the [BUILDING](BUILDING.md) document to know how to build/install RPP.
If you are going to know more details about developing for RPP check [HACKING](HACKING.md) document.

>[!IMPORTANT]
> ReactivePlusPlus is library for C++20. So, it works only on compilers that supports most C++20 features. List of minimal supported compilers:
> - (ubuntu) gcc-10
> - (ubuntu) clang-11
> - (windows) visual studio 2022
> - (macos) Apple Clang 14

## Documentation:

Check detailed [User Guide/Tutorial](https://AlexInLog.github.io/ReactivePlusPlus/v2/docs/html/index.html) (to learn more details about ReactivePlusPlus or reactive programming paradigm itself) and extensive [API Reference of RPP](https://AlexInLog.github.io/ReactivePlusPlus/v2/docs/html/topics.html) (to know how to apply ReactivePlusPlus properly).


## Useful links
- [User Guide](https://AlexInLog.github.io/ReactivePlusPlus/v2/docs/html/index.html)
- [API Reference](https://AlexInLog.github.io/ReactivePlusPlus/v2/docs/html/topics.html)
- [Examples](https://github.com/AlexInLog/ReactivePlusPlus/tree/v2/src/examples)
- [reactivex.io](https://reactivex.io) describes everything related to reactive programming.
- [rxmarbles](https://rxmarbles.com/) interactive diagrams of observables/operators
- [Continuous benchmarking results and comparison with RxCpp](https://AlexInLog.github.io/ReactivePlusPlus/v2/benchmark)
- [BUILDING](BUILDING.md)
- [CONTRIBUTING](CONTRIBUTING.md)

## Licensing

```text
Boost Software License - Version 1.0 - August 17th, 2003

Permission is hereby granted, free of charge, to any person or organization
obtaining a copy of the software and accompanying documentation covered by
this license (the "Software") to use, reproduce, display, distribute,
execute, and transmit the Software, and to prepare derivative works of the
Software, and to permit third-parties to whom the Software is furnished to
do so, all subject to the following:

The copyright notices in the Software and this entire statement, including
the above license grant, this restriction and the following disclaimer,
must be included in all copies of the Software, in whole or in part, and
all derivative works of the Software, unless such copies or derivative
works are solely in the form of machine-executable object code generated by
a source language processor.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE, TITLE AND NON-INFRINGEMENT. IN NO EVENT
SHALL THE COPYRIGHT HOLDERS OR ANYONE DISTRIBUTING THE SOFTWARE BE LIABLE
FOR ANY DAMAGES OR OTHER LIABILITY, WHETHER IN CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
DEALINGS IN THE SOFTWARE.
```

# Credits:
ReactivePlusPlus library uses:
- [PVS-Studio](https://pvs-studio.com/pvs-studio/?utm_source=website&utm_medium=github&utm_campaign=open_source) - static analyzer for C, C++, C#, and Java code.
- [doctest](https://github.com/doctest/doctest) for unit testing only, fetched automatically in case of `RPP_BUILD_TESTS` enabled
- [trompeloeil](https://github.com/rollbear/trompeloeil) for mocking in unit testing only, fetched automatically in case of `RPP_BUILD_TESTS` enabled
- [nanobench](https://github.com/martinus/nanobench) for benchmarking only, fetched automatically in case of `RPP_BUILD_BENCHMARKS` enabled
- [RxCpp](https://github.com/ReactiveX/RxCpp) only for comparison of performance between RPP and RxCpp in CI benchmarks. Used as cmake dependency under option
- [reactivex.io](https://reactivex.io) as source for insipration and definition of entities used in RPP. Some comments used in RPP source code taken from [reactivex.io](https://reactivex.io)
- [rxmarbles python](https://pypi.org/project/rxmarbles/) as generator of marbles graphs in doxygen documentation
- [cmake-init](https://github.com/friendlyanon/cmake-init) as generator for most part of initial CMakes
- [doxygen-awesome-css](https://jothepro.github.io/doxygen-awesome-css/) as CSS theme for doxygen documentation
