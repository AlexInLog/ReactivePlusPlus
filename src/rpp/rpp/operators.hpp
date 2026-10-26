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
 * @defgroup operators Operators
 * @brief Operators modify observables and extend them with custom logic.
 * @details Observables emit values based on underlying logic, such as iterating over a vector and etc. Operators allow you to enhance this stream, for example, by filtering values, transforming them, etc., resulting in a more suitable stream for specific cases.
 *
 * Example: Create an observable to read characters from console input, continue until '0' is encountered, filter out non-letter characters, and send the remaining letters as uppercase to the observer:
 *
 * @code{.cpp}
 * #include <rpp/rpp.hpp>
 * #include <iostream>
 *
 * int main()
 * {
 *   rpp::source::from_callable(&::getchar)
 *     | rpp::operators::repeat()
 *     | rpp::operators::take_while([](char v) { return v != '0'; })
 *     | rpp::operators::filter(std::not_fn(&::isdigit))
 *     | rpp::operators::map(&::toupper)
 *     | rpp::operators::subscribe([](char v) { std::cout << v; });
 *
 *   // input: 12345qwer5125ttqt0
 *   // output: QWERTTQT
 *
 *   return 0;
 * }
 * @endcode
 *
 * @par How operators work and how to create your own?
 * Example:
 *
 * @code{cpp}
 *  rpp::source::create<int>([](const auto& observer){
 *      observer.on_next(1);
 *      observer.on_completed();
 *  });
 * @endcode
 *
 * This example creates an observable of `int` using the `create` operator, which emits the value `1` and then completes.
 * The type of this observable is `rpp::observable<int, ...>`, where `...` is an implementation-defined type.
 * To convert `int` to `std::string`, you can use the `map` operator:
 *
 * @code{cpp}
 * rpp::source::create<int>([](const auto& observer){
 *   observer.on_next(1);
 *   observer.on_completed();
 * })
 * | rpp::operators::map([](int v){ return std::to_string(v); });
 * @endcode
 *
 * Now it is an `observable of strings` (`rpp::observable<std::string, ...>`). The `map` operator is a functor-adaptor that accepts an observable and returns another observable.
 * It transforms the original observable's type to the "final type" by invoking the passed function. In this case, the final type is `std::string`.
 * The `map` operator can be implemented in multiple ways:
 *
 * 1) call-based (function/functor or others) - operator accepts (old) observable and returns new (modified) observable
 * @code{cpp}
 * template<typename Fn>
 * struct map
 * {
 *   Fn fn{};
 *
 *   template<typename Type, typename Internal>
 *   auto operator()(const rpp::observable<Type, Internal>& observable) const {
 *     using FinalType = std::invoke_result_t<Fn, Type>;
 *     return rpp::source::create<FinalType>([observable, fn](const rpp::dynamic_observer<FinalType>& observer)
 *     {
 *       observable.subscribe([observer, fn](const auto& v) { observer.on_next(fn(v)); },
 *                            [observer](const std::exception_ptr& err) { observer.on_error(err); },
 *                            [observer]() { observer.on_completed(); });
 *     });
 *   }
 * }
 * @endcode
 * It is template for such an functor-adaptor. It is also fully valid example of call-based operator:
 * @code{cpp}
 * rpp::source::just(1)
 *   | [](const auto& observable) { return rpp::source::concat(observable, rpp::source::just(2)); };
 * @endcode
 * This converts the observable to a concatenation of the original observable and `just(2)`.
 *
 * 2) type-traits based - should satisfy @link rpp::constraint::operator_ @endlink concept.<br>
 * For example, you can implement such an operator like this:
 * @snippet readme.cpp simple_custom_map
 * But in this case you are missing disposables-related functionality.
 * So, it is better to implement it via providing custom observer's strategy with correct handling of disposables. Check real @link rpp::operators::map @endlink implementation for it =)
 *
 * @see https://reactivex.io/documentation/operators.html
 * @ingroup rpp
 */

#include <rpp/operators/fwd.hpp>

/**
 * @defgroup transforming_operators Transforming Operators
 * @brief Transforming operators are operators that transform items provided by observable
 * @see https://reactivex.io/documentation/operators.html#transforming
 * @ingroup operators
 */

#include <rpp/operators/buffer.hpp>
#include <rpp/operators/flat_map.hpp>
#include <rpp/operators/group_by.hpp>
#include <rpp/operators/map.hpp>
#include <rpp/operators/scan.hpp>
#include <rpp/operators/subscribe.hpp>
#include <rpp/operators/window.hpp>
#include <rpp/operators/window_toggle.hpp>

/**
 * @defgroup filtering_operators Filtering Operators
 * @brief Filtering operators are operators that emit only part of items that satisfies some condition
 * @see https://reactivex.io/documentation/operators.html#filtering
 * @ingroup operators
 */

#include <rpp/operators/debounce.hpp>
#include <rpp/operators/distinct.hpp>
#include <rpp/operators/distinct_until_changed.hpp>
#include <rpp/operators/element_at.hpp>
#include <rpp/operators/filter.hpp>
#include <rpp/operators/first.hpp>
#include <rpp/operators/last.hpp>
#include <rpp/operators/skip.hpp>
#include <rpp/operators/take.hpp>
#include <rpp/operators/take_last.hpp>
#include <rpp/operators/throttle.hpp>

/**
 * @defgroup conditional_operators Conditional Operators
 * @brief Conditional operators are operators that emit items based on some condition including condition of items from
 * other observables
 * @see https://reactivex.io/documentation/operators.html#conditional
 * @ingroup operators
 */

#include <rpp/operators/take_until.hpp>
#include <rpp/operators/take_while.hpp>

/**
 * @defgroup combining_operators Combining Operators
 * @brief Combining operators are operators that combines emissions of multiple observables into same observable by some rule
 * @see https://reactivex.io/documentation/operators.html#combining
 * @ingroup operators
 */

#include <rpp/operators/combine_latest.hpp>
#include <rpp/operators/merge.hpp>
#include <rpp/operators/start_with.hpp>
#include <rpp/operators/switch_on_next.hpp>
#include <rpp/operators/with_latest_from.hpp>
#include <rpp/operators/zip.hpp>

/**
 * @defgroup utility_operators Utility Operators
 * @brief Utility operators are operators that provide some extra functionality without changing of original values, but
 * changing of behaviour
 * @see https://reactivex.io/documentation/operators.html#utility
 * @ingroup operators
 */

#include <rpp/operators/as_blocking.hpp>
#include <rpp/operators/delay.hpp>
#include <rpp/operators/finally.hpp>
#include <rpp/operators/observe_on.hpp>
#include <rpp/operators/repeat.hpp>
#include <rpp/operators/repeat_when.hpp>
#include <rpp/operators/subscribe_on.hpp>
#include <rpp/operators/tap.hpp>
#include <rpp/operators/timeout.hpp>

/**
 * @defgroup connectable_operators Connectable Operators
 * @brief Connectable operators are operators that provide extra functionality for multicasting of controlling of subscription
 * @see https://reactivex.io/documentation/operators.html#connectable
 * @ingroup operators
 */

#include <rpp/operators/multicast.hpp>
#include <rpp/operators/publish.hpp>
#include <rpp/operators/ref_count.hpp>
#include <rpp/operators/share.hpp>

/**
 * @defgroup aggregate_operators Aggregate Operators
 * @brief Aggregate operators are operators that operate on the entire sequence of items emitted by an Observable
 * @see https://reactivex.io/documentation/operators.html#mathematical
 * @ingroup operators
 */

#include <rpp/operators/concat.hpp>
#include <rpp/operators/reduce.hpp>

/**
 * @defgroup error_handling_operators Error Handling Operators
 * @brief Operators that help to recover from error notifications from an Observable
 * @see https://reactivex.io/documentation/operators.html#error
 * @ingroup operators
 */

#include <rpp/operators/on_error_resume_next.hpp>
#include <rpp/operators/retry.hpp>
#include <rpp/operators/retry_when.hpp>
