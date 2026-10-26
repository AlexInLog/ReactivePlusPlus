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

#include <rpp/schedulers/fwd.hpp>

#include <rpp/schedulers/thread_pool.hpp>

namespace rpp::schedulers
{
    /**
     * @brief Scheduler owning static thread pool of workers and using "some" thread from this pool on `create_worker` call
     * @warning Actually it is static variable to `thread_pool` scheduler
     * @note Expected to pass to this scheduler intensive CPU bound tasks with relatevely small duration of execution (to be sure that no any thread with tasks from some other operators would be blocked on that task)
     *
     * @par Examples
     * @snippet thread_pool.cpp computational
     *
     * @ingroup schedulers
     */
    class computational final
    {
    public:
        static auto create_worker()
        {
            static thread_pool s_tp{};
            return s_tp.create_worker();
        }
    };
} // namespace rpp::schedulers
