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

#include <rpp/disposables/fwd.hpp>

namespace rpp
{
    /**
     * @brief Interface of disposable
     *
     * @ingroup disposables
     */
    struct interface_disposable
    {
        virtual ~interface_disposable() noexcept = default;

        /**
         * @brief Check if this disposable is just disposed
         * @attention This function must be thread-safe
         */
        virtual bool is_disposed() const noexcept = 0;

        /**
         * @brief Dispose disposable and free any underlying resources and etc.
         * @attention This function must be thread-safe
         */
        void dispose() noexcept { dispose_impl(Mode::Disposing); }

        template<rpp::constraint::decayed_type TStrategy>
        friend class rpp::details::auto_dispose_wrapper;

    protected:
        enum class Mode : bool
        {
            Disposing  = 0, // someone called "dispose" method manually
            Destroying = 1  // called during destruction -> not needed to clear self in other disposables and etc + not allowed to call `shared_from_this`
        };

        virtual void dispose_impl(Mode mode) noexcept = 0;
    };
} // namespace rpp
