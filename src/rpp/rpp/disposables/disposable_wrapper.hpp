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

#include <rpp/defs.hpp>
#include <rpp/disposables/interface_disposable.hpp>
#include <rpp/utils/utils.hpp>

#include <memory>
#include <variant>

namespace rpp::details
{
    template<rpp::constraint::decayed_type TDisposable>
    class enable_wrapper_from_this;

    template<rpp::constraint::decayed_type TDisposable>
    class auto_dispose_wrapper final
    {
    public:
        static_assert(std::derived_from<TDisposable, interface_disposable>);

        template<typename... TArgs>
            requires (std::constructible_from<TDisposable, TArgs && ...> && !rpp::constraint::variadic_decayed_same_as<auto_dispose_wrapper, TArgs...>)
        explicit auto_dispose_wrapper(TArgs&&... args)
            : m_data{std::forward<TArgs>(args)...}
        {
        }

        auto_dispose_wrapper(const auto_dispose_wrapper&)     = delete;
        auto_dispose_wrapper(auto_dispose_wrapper&&) noexcept = delete;

        ~auto_dispose_wrapper() noexcept
        {
            static_cast<interface_disposable&>(m_data).dispose_impl(rpp::interface_disposable::Mode::Destroying);
        }

        TDisposable* get() { return &m_data; }

    private:
        RPP_NO_UNIQUE_ADDRESS TDisposable m_data;
    };

    class disposable_wrapper_base
    {
    public:
        bool operator==(const disposable_wrapper_base& other) const
        {
            return get().first == other.get().first;
        }

        bool is_disposed() const noexcept
        {
            if (const auto locked = get().first)
                return locked->is_disposed();
            return true;
        }

        void dispose() const noexcept
        {
            if (const auto locked = get().first)
                locked->dispose();
        }

    protected:
        explicit disposable_wrapper_base(std::shared_ptr<interface_disposable>&& disposable)
            : m_disposable{std::move(disposable)}
        {
        }

        explicit disposable_wrapper_base(std::weak_ptr<interface_disposable>&& disposable)
            : m_disposable{std::move(disposable)}
        {
        }

        disposable_wrapper_base() = default;

        std::pair<std::shared_ptr<interface_disposable>, bool> get() const noexcept
        {
            if (const auto ptr_ptr = std::get_if<std::shared_ptr<interface_disposable>>(&m_disposable))
                return {*ptr_ptr, true};

            if (const auto ptr_ptr = std::get_if<std::weak_ptr<interface_disposable>>(&m_disposable))
                return {ptr_ptr->lock(), false};

            return {nullptr, true};
        }

    private:
        std::variant<std::monostate, std::shared_ptr<interface_disposable>, std::weak_ptr<interface_disposable>> m_disposable;
    };

} // namespace rpp::details

namespace rpp
{
    /**
     * @brief Main RPP wrapper over @link disposables @endlink.
     * @details This wrapper invented to provide safe and easy-to-use access to disposables. It has next core points:
     * - disposable_wrapper is kind of smart_pointer (like std::shared_ptr) but for disposables. So, default constructed wrapper is empty wrapper.
     * - disposable_wrapper shares ownership like std::shared_ptr
     * - any disposable created via disposable_wrapper would have call `dispose()` during it's destruction (during destruction of last disposable_wrapper owning it)
     * - disposable_wrapper's methods is safe to use over empty/gone/disposed/weak disposables.
     * - as soon as disposable can be actually "any internal state" it provides access to "raw" shared_ptr and it can be nullptr in case of disposable empty/ptr gone.
     * - disposable_wrapper can be strong or weak (same as std::shared_ptr). weak disposable is important, for example, when it keeps observer and this observer should keep this disposable at the same time.
     * - disposable_wrapper has popluar methods to work with disposable: `dispose()`, `is_disposed()` and `add()`/`remove()`/`clear()` (for `interface_composite_disposable`).
     *
     * To construct wrapper you have to use `make` method:
     * @code{cpp}
     * auto d = rpp::disposable_wrapper::make<SomeSpecificDisposableType>(some_arguments, to_construct_it);
     * @endcode
     *
     * To achieve desired performance RPP is avoiding to returning disposable by default. So, it is why `subscribe` method is not returning anything by default. If you want to attach disposable to observer you can use overloading method accepting disposable as first argument like this:
     * @code{cpp}
     * auto d = rpp::composite_disposable_wrapper::make();
     * observable.subscribe(d, [](int v){});
     * @endcode

     * or use `subscribe_with_disposable` method instead
     * @code{cpp}
     * auto d = observable.subscribe_with_disposable([](int){});
     * @endcode
     *
     * @note rpp has 2 predefined disposable_wrappers for most popular cases:
     * - @link rpp::disposable_wrapper @endlink is wrapper for simple @link rpp::interface_disposable @endlink
     * - @link rpp::composite_disposable_wrapper @endlink is wrapper for @link rpp::composite_disposable @endlink
     *
     * @ingroup disposables
     */
    template<rpp::constraint::decayed_type TDisposable>
    class disposable_wrapper_impl final : public details::disposable_wrapper_base
    {
        using TDefaultMake = std::conditional_t<std::same_as<TDisposable, interface_composite_disposable>, composite_disposable, TDisposable>;

    public:
        template<constraint::decayed_type TTarget>
        friend class disposable_wrapper_impl;

        template<rpp::constraint::decayed_type TTarget>
        friend class details::enable_wrapper_from_this;

        bool operator==(const disposable_wrapper_impl&) const = default;

        /**
         * @brief Main way to create disposable_wrapper. Passed `TTarget` type can be any type derived from `TDisposable`.
         * @par Example:
         *
         * \code{cpp}
         * rpp::disposable_wrapper<rpp::interface_composite_disposable>::make<rpp::composite_disposable>();
         * \endcode
         */
        template<std::derived_from<TDisposable> TTarget = TDefaultMake, typename... TArgs>
            requires (std::constructible_from<TTarget, TArgs && ...>)
        [[nodiscard]] static disposable_wrapper_impl make(TArgs&&... args)
        {
            const auto ptr      = std::make_shared<details::auto_dispose_wrapper<TTarget>>(std::forward<TArgs>(args)...);
            auto       base_ptr = std::shared_ptr<TDisposable>{ptr, static_cast<TDisposable*>(ptr->get())};
            if constexpr (rpp::utils::is_base_of_v<TDisposable, rpp::details::enable_wrapper_from_this>)
            {
                base_ptr->set_weak_self(std::weak_ptr<interface_disposable>(base_ptr));
            }
            return disposable_wrapper_impl{std::static_pointer_cast<interface_disposable>(std::move(base_ptr))};
        }

        /**
         * @brief Creates disposable_wrapper which behaves like disposed disposable
         */
        [[nodiscard]] static disposable_wrapper_impl empty()
        {
            return disposable_wrapper_impl{};
        }

        template<rpp::constraint::is_nothrow_invocable Fn>
        disposable_wrapper add(Fn&& invocable)
            requires std::derived_from<TDisposable, interface_composite_disposable>
        {
            auto d = make_callback_disposable(std::forward<Fn>(invocable));
            add(d);
            return d;
        }

        void add(disposable_wrapper other) const
            requires std::derived_from<TDisposable, interface_composite_disposable>
        {
            if (const auto locked = lock())
                locked->add(std::move(other));
            else
                other.dispose();
        }

        void remove(const disposable_wrapper& other) const
            requires std::derived_from<TDisposable, interface_composite_disposable>
        {
            if (const auto locked = lock())
                locked->remove(other);
        }

        void clear() const
            requires std::derived_from<TDisposable, interface_composite_disposable>
        {
            if (const auto locked = lock())
                locked->clear();
        }

        [[nodiscard]] std::shared_ptr<TDisposable> lock() const noexcept
        {
            return std::static_pointer_cast<TDisposable>(get().first);
        }

        [[nodiscard]] disposable_wrapper_impl as_weak() const
        {
            auto [locked, is_shared] = get();
            if (is_shared)
                return disposable_wrapper_impl{std::weak_ptr<interface_disposable>{locked}};
            return *this;
        }

        template<constraint::decayed_type TTarget>
            requires rpp::constraint::static_pointer_convertible_to<TDisposable, TTarget>
        operator disposable_wrapper_impl<TTarget>() const
        {
            auto [locked, is_shared] = get();
            if (!locked)
                return rpp::disposable_wrapper_impl<TTarget>::empty();

            auto res = disposable_wrapper_impl<TTarget>{std::move(locked)};
            if (is_shared)
                return res;

            return res.as_weak();
        }

    private:
        using details::disposable_wrapper_base::disposable_wrapper_base;
    };
} // namespace rpp

namespace rpp::details
{
    template<rpp::constraint::decayed_type TStrategy>
    class enable_wrapper_from_this
    {
    public:
        template<rpp::constraint::decayed_type TSource>
        friend class rpp::disposable_wrapper_impl;

    protected:
        enable_wrapper_from_this() = default;

        void set_weak_self(std::weak_ptr<interface_disposable> weak)
        {
            m_weak = std::move(weak);
        }

    public:
        disposable_wrapper_impl<TStrategy> wrapper_from_this() const
        {
            return disposable_wrapper_impl<TStrategy>(std::static_pointer_cast<interface_disposable>(m_weak.lock()));
        }

    private:
        std::weak_ptr<interface_disposable> m_weak{};
    };
} // namespace rpp::details
