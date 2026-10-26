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
 * @defgroup observables Observables
 * @brief Observable is the source of any Reactive Stream.
 * @details Observable is the source of any Reactive Stream. Observable provides ability to subscribe observer on stream of events. After subscription observable would emit values to observer.
 *
 * Reactive programming has an [Observable Contract](https://reactivex.io/documentation/contract.html). Please read it.
 *
 * This contract includes:
 *
 * > Observables must issue notifications to observers serially (not in parallel). They may issue these notifications from different threads, but there must be a formal happens-before relationship between the notifications.
 *
 * RPP follows this contract, meaning:
 *
 * 1. **All** RPP operators follow this contract.\n
 * All built-in RPP observables/operators emit emissions serially
 *
 * 2. User-provided callbacks can be non-thread-safe due to the thread-safety of the observable.\n
 * For example: internal logic of `take` operator doesn't use mutexes or atomics due to underlying observable **MUST** emit items serially
 *
 * 3. When implementing your own operator via `create`, **follow this contract**!
 *
 * 4. This is true **EXCEPT FOR** subjects if used manually. Use serialized_* instead if you can't guarantee serial emissions.
 *
 * For example:
 * @code{.cpp}
 * auto s1 = rpp::source::just(1) | rpp::operators::repeat() | rpp::operators::subscribe_on(rpp::schedulers::new_thread{});
 * auto s2 = rpp::source::just(2) | rpp::operators::repeat() | rpp::operators::subscribe_on(rpp::schedulers::new_thread{});
 * s1 | rpp::operators::merge_with(s2)
 *    | rpp::operators::map([](int v)
 *    {
 *        std::cout << "enter " << v << std::endl;
 *        std::this_thread::sleep_for(std::chrono::seconds{1});
 *        std::cout << "exit " << v << std::endl;
 *        return v;
 *    })
 *    | rpp::operators::as_blocking()
 *    | rpp::operators::subscribe([](int){});
 * @endcode
 *
 * This will never produce:
 * @code{.log}
 * enter 1
 * enter 2
 * exit 2
 * exit 1
 * @endcode
 *
 * Only serially:
 * @code{.log}
 * enter 1
 * exit 1
 * enter 1
 * exit 1
 * enter 2
 * exit 2
 * enter 2
 * exit 2
 * @endcode
 * @see https://reactivex.io/documentation/observable.html
 * @ingroup rpp
 */

#include <rpp/observables/fwd.hpp>

#include <rpp/observables/blocking_observable.hpp>
#include <rpp/observables/connectable_observable.hpp>
#include <rpp/observables/dynamic_connectable_observable.hpp>
#include <rpp/observables/dynamic_observable.hpp>
#include <rpp/observables/grouped_observable.hpp>
#include <rpp/observables/observable.hpp>
#include <rpp/observables/variant_observable.hpp>
