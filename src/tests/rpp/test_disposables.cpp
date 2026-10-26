//                   ReactivePlusPlus library
//
//           Copyright Aleksey Loginov 2023 - present.
//  Distributed under the Boost Software License, Version 1.0.
//     (See accompanying file LICENSE_1_0.txt or copy at
//           https://www.boost.org/LICENSE_1_0.txt)
//
//  Project home: https://github.com/AlexInLog/ReactivePlusPlus

#include <doctest/doctest.h>

#include <rpp/disposables/composite_disposable.hpp>
#include <rpp/disposables/disposable_wrapper.hpp>
#include <rpp/disposables/refcount_disposable.hpp>

namespace
{
    struct custom_disposable : public rpp::interface_disposable
    {
        custom_disposable() = default;

        bool is_disposed() const noexcept final { return dispose_count > 1; }

        void dispose_impl(rpp::interface_disposable::Mode) noexcept final { ++dispose_count; }

        size_t dispose_count{};
    };
} // namespace

TEST_CASE_TEMPLATE("disposable keeps state", TestType, rpp::details::disposables::dynamic_disposables_container, rpp::details::disposables::static_disposables_container<1>)
{
    auto d = rpp::composite_disposable_wrapper::make<rpp::composite_disposable_impl<TestType>>();

    CHECK(!d.is_disposed());


    SUBCASE("dispose marks disposable as disposed")
    {
        d.dispose();
        CHECK(d.is_disposed());
    }

    SUBCASE("dispose on copy of disposable marks both disposable as disposed")
    {
        auto copy = d; // NOLINT(performance-unnecessary-copy-initialization)
        copy.dispose();
        CHECK(copy.is_disposed());
        CHECK(d.is_disposed());
    }

    SUBCASE("add other disposable")
    {
        auto other = rpp::composite_disposable_wrapper::make();
        CHECK(!other.is_disposed());
        d.add(other);
        SUBCASE("calling dispose on original disposable forces both of them to be disposed")
        {
            d.dispose();
            CHECK(other.is_disposed());
            CHECK(d.is_disposed());
        }

        SUBCASE("calling clear on original disposable forces inner to be disposed")
        {
            d.clear();
            CHECK(other.is_disposed());
            CHECK(!d.is_disposed());

            other = rpp::composite_disposable_wrapper::make();
            CHECK(!other.is_disposed());
            d.add(other);
            CHECK(!other.is_disposed());

            d.clear();
            CHECK(other.is_disposed());

            CHECK(!d.is_disposed());
        }
        SUBCASE("calling clear on disposed disposable")
        {
            d.dispose();
            CHECK(other.is_disposed());
            CHECK(d.is_disposed());
            d.clear();
        }

        SUBCASE("calling remove + dispose on original disposable forces only original to be disposed")
        {
            d.remove(other);
            d.dispose();
            CHECK(!other.is_disposed());
            CHECK(d.is_disposed());
        }

        SUBCASE("calling dispose on other disposable forces only other to be disposed")
        {
            other.dispose();
            CHECK(other.is_disposed());
            CHECK(!d.is_disposed());
        }
    }

    SUBCASE("add disposed disposable")
    {
        auto other = rpp::composite_disposable_wrapper::make();
        other.dispose();
        d.add(other);
        CHECK(other.is_disposed());
        CHECK(!d.is_disposed());
    }

    SUBCASE("disposed disposable")
    {
        d.dispose();

        SUBCASE("adding non disposed disposable to empty forces it to be disposed")
        {
            auto other = rpp::composite_disposable_wrapper::make();
            CHECK(!other.is_disposed());
            d.add(other);
            CHECK(other.is_disposed());
        }
    }

    SUBCASE("empty disposable")
    {
        d = rpp::composite_disposable_wrapper::empty();
        CHECK(d.is_disposed());
        d.dispose();

        SUBCASE("adding non disposed disposable to empty forces it to be disposed")
        {
            auto other = rpp::composite_disposable_wrapper::make();
            CHECK(!other.is_disposed());
            d.add(other);
            CHECK(other.is_disposed());
        }
    }
    SUBCASE("disposable dispose on destruction")
    {
        {
            auto other = rpp::composite_disposable_wrapper::make();
            CHECK(!other.is_disposed());
            CHECK(!d.is_disposed());
            other.add(d);
            CHECK(!other.is_disposed());
            CHECK(!d.is_disposed());
        }
        CHECK(d.is_disposed());
    }

    SUBCASE("add callback_disposable")
    {
        size_t invoked_count{};
        d.add([&invoked_count]() noexcept {
            ++invoked_count;
        });
        CHECK(invoked_count == 0);
        d.dispose();
        CHECK(invoked_count == 1);
    }

    SUBCASE("add callback_disposable to disposed disposable")
    {
        d.dispose();

        size_t invoked_count{};
        d.add([&invoked_count]() noexcept {
            ++invoked_count;
        });
        CHECK(invoked_count == 1);
    }

    SUBCASE("add self")
    {
        d.add(d);
        CHECK(!d.is_disposed());
        SUBCASE("dispose self")
        {
            d.dispose();
            CHECK(d.is_disposed());
        }
    }

    SUBCASE("call dispose twice")
    {
        d.dispose();
        CHECK(d.is_disposed());

        d.dispose();
        CHECK(d.is_disposed());
    }
}

TEST_CASE("refcount disposable dispose underlying in case of reaching zero")
{
    auto refcount   = rpp::disposable_wrapper_impl<rpp::refcount_disposable>::make();
    auto refcounted = refcount.lock()->add_ref();
    auto underlying = rpp::disposable_wrapper_impl<custom_disposable>::make();
    refcount.add(underlying);

    CHECK(!underlying.is_disposed());
    CHECK(!refcounted.is_disposed());
    CHECK(!refcount.is_disposed());

    SUBCASE("disposing refcounted as is disposes underlying")
    {
        refcounted.dispose();

        CHECK(underlying.lock()->dispose_count == 1);
        CHECK(refcounted.is_disposed());
        CHECK(refcount.is_disposed());

        SUBCASE("additional disposing does nothing")
        {
            refcounted.dispose();
            CHECK(underlying.lock()->dispose_count == 1);
            CHECK(refcounted.is_disposed());
            CHECK(refcount.is_disposed());
        }
        SUBCASE("addref and disposing does nothing")
        {
            auto d = refcount.lock()->add_ref();
            CHECK(d.is_disposed());

            refcounted.dispose();
            CHECK(underlying.lock()->dispose_count == 1);
            CHECK(refcounted.is_disposed());
            CHECK(refcount.is_disposed());
        }
    }

    SUBCASE("disposing added to underlying not disposes refcount")
    {
        underlying.dispose();

        CHECK(underlying.lock()->dispose_count == 1);
        CHECK(!refcount.is_disposed());
        CHECK(!refcounted.is_disposed());
    }

    SUBCASE("add_ref prevents immediate disposing")
    {
        size_t                               count = 5;
        std::vector<rpp::disposable_wrapper> disposables{};
        for (size_t i = 0; i < count; ++i)
            disposables.push_back(refcount.lock()->add_ref());

        CHECK(!refcount.is_disposed());
        CHECK(!refcounted.is_disposed());

        for (size_t i = 0; i < 10 * count; ++i)
            refcounted.dispose();

        CHECK(refcounted.is_disposed());
        CHECK(!underlying.lock()->is_disposed());

        for (auto& d : disposables)
        {
            CHECK(!underlying.lock()->is_disposed());
            CHECK(!d.is_disposed());
            d.dispose();
            CHECK(d.is_disposed());
        }

        CHECK(underlying.lock()->dispose_count == 1);
    }
}

TEST_CASE("composite_disposable correctly handles exception")
{
    auto d  = rpp::composite_disposable_wrapper::make<rpp::composite_disposable_impl<rpp::details::disposables::static_disposables_container<1>>>();
    auto d1 = rpp::composite_disposable_wrapper::make();
    auto d2 = rpp::composite_disposable_wrapper::make();
    d.add(d1);
    CHECK_THROWS_AS(d.add(d2), rpp::utils::more_disposables_than_expected);
    CHECK(!d1.is_disposed());
    CHECK(!d2.is_disposed());

    d.dispose();
    CHECK(d1.is_disposed());
    CHECK(!d2.is_disposed());
}

TEST_CASE("static_disposables_container works as expected")
{
    rpp::details::disposables::static_disposables_container<2> container{};

    auto d1 = rpp::composite_disposable_wrapper::make();
    auto d2 = rpp::composite_disposable_wrapper::make();

    SUBCASE("dispose empty")
    {
        container.dispose();
    }

    container.push_back(d1);
    container.push_back(d2);

    SUBCASE("dispose with added disposable")
    {
        container.dispose();
        CHECK(d1.is_disposed());
        CHECK(d2.is_disposed());
    }

    SUBCASE("clear with added disposable")
    {
        container.clear();
        container.dispose();
        CHECK(!d1.is_disposed());
        CHECK(!d2.is_disposed());
        SUBCASE("add cleared and dispose")
        {
            container.push_back(d1);
            CHECK(!d1.is_disposed());
            container.dispose();
            CHECK(d1.is_disposed());
            CHECK(!d2.is_disposed());
        }
    }

    SUBCASE("remove with added disposable")
    {
        container.remove(d1);
        container.dispose();
        CHECK(!d1.is_disposed());
        CHECK(d2.is_disposed());
        SUBCASE("add removed and dispose")
        {
            container.push_back(d1);
            CHECK(!d1.is_disposed());
            container.dispose();
            CHECK(d1.is_disposed());
        }
    }

    SUBCASE("move container")
    {
        auto other = std::move(container);
        SUBCASE("dispose original")
        {
            container.dispose(); // NOLINT
            CHECK(!d1.is_disposed());
            CHECK(!d2.is_disposed());
        }

        SUBCASE("dispose copied")
        {
            other.dispose();
            CHECK(d1.is_disposed());
            CHECK(d2.is_disposed());
        }
        SUBCASE("move back")
        {
            container = std::move(other);
            SUBCASE("dispose copied")
            {
                other.dispose(); // NOLINT
                CHECK(!d1.is_disposed());
                CHECK(!d2.is_disposed());
            }

            SUBCASE("dispose original")
            {
                container.dispose();
                CHECK(d1.is_disposed());
                CHECK(d2.is_disposed());
            }
        }
    }
}
