//                  ReactivePlusPlus library
//
//          Copyright Aleksey Loginov 2023 - present.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          https://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/AlexInLog/ReactivePlusPlus
//

#pragma once

/**
 * @defgroup observers Observers
 *
 * @details Observer subscribes on Observable and obtains values provided by Observable.
 *
 *  In fact observer is kind of wrapper over 3 core functions:
 * - `on_next(T)`     - callback with new emission provided by observable
 * - `on_error(err)`  - failure termination callback with reason of failure of observable (why observable can't continue processing)
 * - `on_completed()` - succeed termination callback - observable is done, no any future emissions from this
 *
 * Additionally in RPP observer handles @link disposables @endlink related logic:
 * - `set_upstream(disposable)` - observable could pass to observer it's own disposable to provide ability for observer to terminate observable's internal actions/state.
 * - `is_disposed()` - observable could check if observer is still interested in emissions (`false`) or done and no any futher calls would be success (`true`)
 *
 * @par Observer creation:
 * - **Observer creation inside subscribe:** <br>
 * RPP expects user to create observers only inside `subscribe` function of observables. Something like this:
 * @code{.cpp}
 * rpp::source::just(1).subscribe([](int){}, [](const std::exception_ptr&){}, [](){});
 * rpp::source::just(1) | rpp::operators::subscribe([](int){}, [](const std::exception_ptr&){}, [](){});
 * @endcode
 * Some of the callbacks (on_next/on_error/on_completed) can be omitted. Check @link rpp::operators::subscribe @endlink for more details.
 *
 * - **Advanced observer creation:** <br>
 * Technically it is possible to create custom observer via creating new class/struct which satisfies concept @link rpp::constraint::observer_strategy @endlink, but it is **highly not-recommended for most cases** <br>
 * Also *technically* you could create your observer via `make_lambda_observer` function, but it is not recommended too: it could disable some built-in optimizations and cause worse performance. <br>
 * Also it is **most probably** bad pattern and invalid usage of RX if you want to keep/store observers as member variables/fields. Most probably you are doing something wrong IF you are not implementing custom observable/operator.
 *
 * @see https://reactivex.io/documentation/observable.html
 * @ingroup rpp
 */

#include <rpp/observers/fwd.hpp>

#include <rpp/observers/dynamic_observer.hpp>
#include <rpp/observers/lambda_observer.hpp>
#include <rpp/observers/observer.hpp>
