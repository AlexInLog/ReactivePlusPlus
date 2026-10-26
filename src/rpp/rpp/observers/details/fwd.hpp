//                  ReactivePlusPlus library
//
//          Copyright Aleksey Loginov 2022 - present.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          https://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/AlexInLog/ReactivePlusPlus
//

#pragma once

#include <rpp/disposables/fwd.hpp>

#include <rpp/disposables/details/container.hpp>

#include <type_traits>

namespace rpp::details::observers
{
    enum class disposables_mode : uint8_t
    {
        // Let observer deduce disposables mode
        Auto = 0,
        // No any disposables logic for observer expected
        None = 1,
        // Use external (passed to constructor) composite_disposable_wrapper as disposable
        External = 2,
        // Observer just controls is_disposed or not but upstreams handled via observer_strategy
        Boolean = 3
    };

    namespace constraint
    {
        template<typename T>
        concept disposables_strategy = requires(T& v, const T& const_v, const disposable_wrapper& d) {
            v.add(d);
            {
                const_v.is_disposed()
            } -> std::same_as<bool>;
            const_v.dispose();
        };
    } // namespace constraint

    template<typename DisposableContainer>
    class local_disposables_strategy;

    /**
     * @brief No any disposable logic at all. Used only inside proxy-forwarding operators where extra disposable logic not requires
     */
    struct none_disposables_strategy;
    /**
     * @brief Just control is_disposed or not via boolean and ignore upstreams at all
     */
    class boolean_disposables_strategy;

    /**
     * @brief Keep disposables inside dynamic_disposables_container container (based on std::vector)
     */
    using dynamic_disposables_strategy = local_disposables_strategy<disposables::dynamic_disposables_container>;

    /**
     * @brief Keep disposables inside static_disposables_container container (based on std::array)
     */
    template<size_t Count>
    using static_disposables_strategy = local_disposables_strategy<disposables::static_disposables_container<Count>>;

    using default_disposables_strategy = dynamic_disposables_strategy;

    namespace details
    {
        template<disposables_mode mode>
        consteval auto* deduce_optimal_disposables_strategy()
        {
            static_assert(mode == disposables_mode::Auto || mode == disposables_mode::None || mode == disposables_mode::External || mode == disposables_mode::Boolean);

#if defined(RPP_DISABLE_DISPOSABLES_OPTIMIZATION) and RPP_DISABLE_DISPOSABLES_OPTIMIZATION
            if constexpr (mode == disposables_mode::External)
                return static_cast<composite_disposable_wrapper*>(nullptr);
            else
                return static_cast<default_disposables_strategy*>(nullptr);
#else
            if constexpr (mode == disposables_mode::Auto)
                return static_cast<default_disposables_strategy*>(nullptr);
            else if constexpr (mode == disposables_mode::None)
                return static_cast<none_disposables_strategy*>(nullptr);
            else if constexpr (mode == disposables_mode::External)
                return static_cast<composite_disposable_wrapper*>(nullptr);
            else if constexpr (mode == disposables_mode::Boolean)
                return static_cast<boolean_disposables_strategy*>(nullptr);
            else
                return static_cast<void*>(nullptr);
#endif
        }
    } // namespace details

    template<rpp::details::observers::disposables_mode Mode>
    using deduce_optimal_disposables_strategy_t = std::remove_pointer_t<decltype(details::deduce_optimal_disposables_strategy<Mode>())>;
} // namespace rpp::details::observers
