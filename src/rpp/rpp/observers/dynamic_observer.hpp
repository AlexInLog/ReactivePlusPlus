//                   ReactivePlusPlus library
//
//           Copyright Aleksey Loginov 2023 - present.
//  Distributed under the Boost Software License, Version 1.0.
//     (See accompanying file LICENSE_1_0.txt or copy at
//           https://www.boost.org/LICENSE_1_0.txt)
//
//  Project home: https://github.com/AlexInLog/ReactivePlusPlus

#pragma once

#include <rpp/disposables/fwd.hpp>
#include <rpp/observers/fwd.hpp>

#include <rpp/observers/observer.hpp>

#include <memory>
#include <utility>

namespace rpp::details::observers
{
    template<typename Type>
    class observer_vtable
    {
    public:
        void set_upstream(const disposable_wrapper& d) noexcept { m_vtable.set_upstream_ptr(this, d); }
        bool is_disposed() const noexcept { return m_vtable.is_disposed_ptr(this); }

        void on_next(const Type& v) const noexcept { m_vtable.on_next_lvalue_ptr(this, v); }
        void on_next(Type&& v) const noexcept { m_vtable.on_next_rvalue_ptr(this, std::move(v)); }
        void on_error(const std::exception_ptr& err) const noexcept { m_vtable.on_error_ptr(this, err); }
        void on_completed() const noexcept { m_vtable.on_completed_ptr(this); }

    protected:
        struct vtable_t
        {
            void (*const on_next_lvalue_ptr)(const observer_vtable*, const Type&){};
            void (*const on_next_rvalue_ptr)(const observer_vtable*, Type&&){};
            void (*const on_error_ptr)(const observer_vtable*, const std::exception_ptr&){};
            void (*const on_completed_ptr)(const observer_vtable*){};

            void (*const set_upstream_ptr)(observer_vtable*, const disposable_wrapper&){};
            bool (*const is_disposed_ptr)(const observer_vtable*){};
        };

        observer_vtable(vtable_t&& vtable)
            : m_vtable{std::move(vtable)}
        {
        }

        const vtable_t m_vtable{};
    };

    template<rpp::constraint::observer TObs>
    class type_erased_observer : public observer_vtable<rpp::utils::extract_observer_type_t<TObs>>
    {
        using Type   = rpp::utils::extract_observer_type_t<TObs>;
        using Base   = observer_vtable<Type>;
        using Vtable = typename Base::vtable_t;

        static constexpr const TObs& cast(const Base* ptr)
        {
            return static_cast<const type_erased_observer*>(ptr)->m_observer;
        }

        static constexpr TObs& cast(Base* ptr)
        {
            return static_cast<type_erased_observer*>(ptr)->m_observer;
        }

    public:
        type_erased_observer(TObs&& observer)
            : Base{Vtable{
                  .on_next_lvalue_ptr = +[](const Base* b, const Type& v) { cast(b).on_next(v); },
                  .on_next_rvalue_ptr = +[](const Base* b, Type&& v) { cast(b).on_next(std::move(v)); },
                  .on_error_ptr       = +[](const Base* b, const std::exception_ptr& err) { cast(b).on_error(err); },
                  .on_completed_ptr   = +[](const Base* b) { cast(b).on_completed(); },
                  .set_upstream_ptr   = +[](Base* b, const rpp::disposable_wrapper& d) { cast(b).set_upstream(d); },
                  .is_disposed_ptr    = +[](const Base* b) { return cast(b).is_disposed(); }}}
            , m_observer{std::move(observer)}
        {
        }

    private:
        RPP_NO_UNIQUE_ADDRESS TObs m_observer;
    };

    template<rpp::constraint::decayed_type Type>
    class dynamic_strategy final
    {
    public:
        static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::None;

        template<rpp::constraint::observer_strategy<Type> Strategy>
            requires (!rpp::constraint::decayed_same_as<Strategy, dynamic_strategy<Type>>)
        explicit dynamic_strategy(observer<Type, Strategy>&& obs)
            : m_observer{std::make_shared<type_erased_observer<observer<Type, Strategy>>>(std::move(obs))}
        {
        }

        void set_upstream(const disposable_wrapper& d) noexcept { m_observer->set_upstream(d); }
        bool is_disposed() const noexcept { return m_observer->is_disposed(); }

        void on_next(const Type& v) const noexcept { m_observer->on_next(v); }
        void on_next(Type&& v) const noexcept { m_observer->on_next(std::move(v)); }
        void on_error(const std::exception_ptr& err) const noexcept { m_observer->on_error(err); }
        void on_completed() const noexcept { m_observer->on_completed(); }

    private:
        std::shared_ptr<observer_vtable<Type>> m_observer;
    };
} // namespace rpp::details::observers


namespace rpp
{
    /**
     * @brief Type-erased version of the `rpp::observer`. Any observer can be converted to dynamic_observer via `rpp::observer::as_dynamic` member function.
     * @details To provide type-erasure it uses `std::shared_ptr`. As a result it has worse performance, but it is **ONLY** way to copy observer.
     *
     * @tparam Type of value this observer can handle
     *
     * @ingroup observers
     */
    template<constraint::decayed_type Type>
    class dynamic_observer final : public observer<Type, details::observers::dynamic_strategy<Type>>
    {
        using base = observer<Type, details::observers::dynamic_strategy<Type>>;

    public:
        using base::base;

        dynamic_observer(base&& b)
            : base{std::move(b)}
        {
        }

        dynamic_observer(const base& b)
            : base{b}
        {
        }
    };
} // namespace rpp
