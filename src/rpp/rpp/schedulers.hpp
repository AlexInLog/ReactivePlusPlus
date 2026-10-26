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
 * @defgroup schedulers Schedulers
 * @brief Scheduler is the way to introduce multi-threading in your application via RPP
 * @see https://reactivex.io/documentation/scheduler.html
 * @ingroup rpp
 */

#include <rpp/schedulers/computational.hpp>
#include <rpp/schedulers/current_thread.hpp>
#include <rpp/schedulers/immediate.hpp>
#include <rpp/schedulers/new_thread.hpp>
#include <rpp/schedulers/run_loop.hpp>
#include <rpp/schedulers/thread_pool.hpp>
