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
 * @defgroup disposables Disposables
 *
 * @brief Disposable is handle/resource passed from observable to observer via the `set_upstream` method. Observer disposes this disposable when it wants to unsubscribe from observable.
 *
 * @details In reactive programming, a **disposable** is an object that represents a resource that needs to be released or disposed of when it is no longer needed.
 * This can include things like file handles, network connections, or any other resource that needs to be cleaned up after use.
 * The purpose of a disposable is to provide a way to manage resources in a safe and efficient manner.
 * By using disposables, you can ensure that resources are released in a timely manner, preventing memory leaks and other issues that can arise from resource leaks.
 *
 * There are 2 main purposes of disposables:
 * 1. **Upstream disposable** <br>
 * This is a disposable that the observable puts into the observer.
 * The upstream disposable keeps some state or callback that should be disposed of when the observer is disposed (== no longer wants to receive emissions, for example, was completed/errored or just unsubscribed)
 * This ensures that any resources used by the observable are properly cleaned up when the observer obtains on_error/on_completed or disposed in any other way.
 *
 * 2. **External disposable** <br>
 * This is a disposable that allows the observer to be disposed of from outside the observer itself.
 * This can be useful in situations where you need to cancel an ongoing operation or release resources before the observable has completed its work.
 * To achieve this in rpp you can pass disposable to `subscribe` method or use `subscribe_with_disposable` overload instead.
 *
 * @note In rpp all disposables should be created via @link rpp::disposable_wrapper_impl @endlink instead of manually.
 *
 * @warning From user of rpp library it is not really expected to handle disposables manually somehow **except** of case where user want to control lifetime of observable-observer connection manually.
 *
 * @ingroup rpp
 */

#include <rpp/disposables/fwd.hpp>

#include <rpp/disposables/callback_disposable.hpp>
#include <rpp/disposables/composite_disposable.hpp>
#include <rpp/disposables/disposable_wrapper.hpp>
#include <rpp/disposables/interface_composite_disposable.hpp>
#include <rpp/disposables/interface_disposable.hpp>
#include <rpp/disposables/refcount_disposable.hpp>
