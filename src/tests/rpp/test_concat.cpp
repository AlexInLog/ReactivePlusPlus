//                  ReactivePlusPlus library
//
//          Copyright Aleksey Loginov 2023 - present.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          https://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/AlexInLog/ReactivePlusPlus
//

#include <doctest/doctest.h>

#include <rpp/disposables/composite_disposable.hpp>
#include <rpp/disposables/disposable_wrapper.hpp>
#include <rpp/observables/dynamic_observable.hpp>
#include <rpp/operators/concat.hpp>
#include <rpp/schedulers/immediate.hpp>
#include <rpp/sources/concat.hpp>
#include <rpp/sources/create.hpp>
#include <rpp/sources/empty.hpp>
#include <rpp/sources/just.hpp>
#include <rpp/subjects/publish_subject.hpp>

#include "copy_count_tracker.hpp"
#include "disposable_observable.hpp"
#include "rpp_trompeloil.hpp"

#include <memory>
#include <optional>
#include <stdexcept>

struct my_container_with_error : std::vector<rpp::dynamic_observable<int>>
{
    using std::vector<rpp::dynamic_observable<int>>::vector;
    std::vector<rpp::dynamic_observable<int>>::const_iterator begin() const { throw std::runtime_error{""}; }
};

struct my_container_with_error_on_increment
{
public:
    class iterator
    {
    public:
        iterator(const my_container_with_error_on_increment* container)
            : m_container{container}
        {
        }

        using iterator_category = std::input_iterator_tag;
        using difference_type   = std::ptrdiff_t;
        using value_type        = rpp::dynamic_observable<int>;
        using pointer           = rpp::dynamic_observable<int>*;

        const value_type& operator*() const { return m_container->m_obs; }
        iterator&         operator++() { throw std::runtime_error{""}; }
        iterator          operator++(int) { throw std::runtime_error{""}; }

        bool operator==(const iterator&) const { return false; }
        bool operator!=(const iterator&) const { return true; }

    private:
        const my_container_with_error_on_increment* m_container;
    };

    iterator begin() const { return {this}; }
    iterator end() const { return {nullptr}; }

private:
    rpp::dynamic_observable<int> m_obs = rpp::source::just(1).as_dynamic();
};

TEST_CASE_TEMPLATE("concat", TestType, rpp::memory_model::use_stack, rpp::memory_model::use_shared)
{
    mock_observer<int>    mock{};
    trompeloeil::sequence s{};
    auto                  test = [&](const auto& make_concat) {
        SUBCASE("concat of solo observable")
        {
            REQUIRE_CALL(*mock, on_next_lvalue(1)).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_next_lvalue(2)).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_completed()).IN_SEQUENCE(s);

            auto observable = make_concat(rpp::source::just<TestType>(1, 2));
            observable.subscribe(mock);
        }
        SUBCASE("concat of multiple same observables")
        {
            REQUIRE_CALL(*mock, on_next_lvalue(1)).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_next_lvalue(2)).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_next_lvalue(1)).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_next_lvalue(2)).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_completed()).IN_SEQUENCE(s);

            auto observable = make_concat(rpp::source::just<TestType>(1, 2), rpp::source::just<TestType>(1, 2));
            observable.subscribe(mock);
        }
        SUBCASE("concat of multiple different observables")
        {
            REQUIRE_CALL(*mock, on_next_lvalue(1)).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_next_lvalue(2)).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_next_lvalue(1)).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_completed()).IN_SEQUENCE(s);

            auto observable = make_concat(rpp::source::just<TestType>(1, 2), rpp::source::just<TestType>(1));
            observable.subscribe(mock);
        }
        SUBCASE("concat stop if no completion")
        {
            REQUIRE_CALL(*mock, on_next_lvalue(1)).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_next_lvalue(2)).IN_SEQUENCE(s);

            std::optional<rpp::dynamic_observer<int>> observer{};
            auto                                      observable = make_concat(rpp::source::just<TestType>(1, 2), rpp::source::create<int>([&](auto&& obs) { observer.emplace(std::forward<decltype(obs)>(obs).as_dynamic()); }), rpp::source::just<TestType>(3));
            observable.subscribe(mock);

            REQUIRE(observer.has_value());

            SUBCASE("send completion later")
            {
                REQUIRE_CALL(*mock, on_next_lvalue(3)).IN_SEQUENCE(s);
                REQUIRE_CALL(*mock, on_completed()).IN_SEQUENCE(s);

                observer->on_completed();
            }
            SUBCASE("send emission later")
            {
                REQUIRE_CALL(*mock, on_next_rvalue(10)).IN_SEQUENCE(s);

                observer->on_next(10);
            }
            SUBCASE("send error later")
            {
                REQUIRE_CALL(*mock, on_error(trompeloeil::_)).IN_SEQUENCE(s);

                observer->on_error({});
            }
        }
        SUBCASE("concat stoped if disposed")
        {
            REQUIRE_CALL(*mock, on_next_lvalue(1)).IN_SEQUENCE(s);

            auto d = rpp::composite_disposable_wrapper::make();
            auto observable =
                make_concat(rpp::source::just<TestType>(1),
                            rpp::source::create<int>([&](auto&& obs) { d.dispose(); obs.on_completed(); }),
                            rpp::source::create<int>([&](auto&&) { FAIL("Shouldn't be called"); }),
                            rpp::source::just<TestType>(3));
            observable.subscribe(rpp::composite_disposable_wrapper{d}, mock);
        }

        SUBCASE("concat tracks actual upstream")
        {
            auto d  = rpp::composite_disposable_wrapper::make();
            auto d1 = rpp::composite_disposable_wrapper::make();

            auto observable = make_concat(rpp::source::create<int>([&](auto&& obs) {
                obs.set_upstream(rpp::disposable_wrapper{d1});

                CHECK(!d.is_disposed());
                CHECK(!d1.is_disposed());

                d.dispose();

                CHECK(d.is_disposed());
                CHECK(d1.is_disposed());
            }));
            observable.subscribe(rpp::composite_disposable_wrapper{d}, mock);
        }

        SUBCASE("concat tracks actual upstream for 2 upstreams")
        {
            auto d  = rpp::composite_disposable_wrapper::make();
            auto d1 = rpp::composite_disposable_wrapper::make();
            auto d2 = rpp::composite_disposable_wrapper::make();

            auto observable =
                make_concat(rpp::source::create<int>([&](auto&& obs) { obs.set_upstream(rpp::disposable_wrapper{d1}); obs.on_completed(); }),
                            rpp::source::create<int>([&](auto&& obs) {
                                obs.set_upstream(rpp::disposable_wrapper{d2});

                                CHECK(!d.is_disposed());
                                CHECK(d1.is_disposed());
                                CHECK(!d2.is_disposed());

                                d.dispose();

                                CHECK(d.is_disposed());
                                CHECK(d2.is_disposed());
                            }));

            observable.subscribe(rpp::composite_disposable_wrapper{d}, mock);
        }
    };

    SUBCASE("concat as source")
    {
        test([](auto&&... vals) {
            return rpp::source::concat<TestType>(std::forward<decltype(vals)>(vals)...);
        });
        SUBCASE("concat of array of different observables")
        {
            REQUIRE_CALL(*mock, on_next_lvalue(1)).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_next_lvalue(2)).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_next_lvalue(1)).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_next_lvalue(1)).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_completed()).IN_SEQUENCE(s);

            auto observable = rpp::source::concat<TestType>(std::array{rpp::source::just<TestType>(1, 2), rpp::source::just<TestType>(1, 1)});
            observable.subscribe(mock);
        }
        SUBCASE("container with error on begin")
        {
            REQUIRE_CALL(*mock, on_error(trompeloeil::_)).IN_SEQUENCE(s);

            rpp::source::concat<TestType>(my_container_with_error{}).subscribe(mock);
        }

        SUBCASE("container with error on increment")
        {
            REQUIRE_CALL(*mock, on_error(trompeloeil::_)).IN_SEQUENCE(s);

            rpp::source::concat<TestType>(my_container_with_error_on_increment{}).subscribe(mock);
        }
    }
    SUBCASE("concat as operator")
    {
        test([](auto&&... vals) {
            return rpp::source::just(std::forward<decltype(vals)>(vals).as_dynamic()...) | rpp::ops::concat();
        });
        SUBCASE("concat completes right")
        {
            rpp::subjects::publish_subject<rpp::dynamic_observable<int>> subj{};

            subj.get_observable() | rpp::ops::concat() | rpp::ops::subscribe(mock);
            SUBCASE("on_completed from base")
            {
                REQUIRE_CALL(*mock, on_completed()).IN_SEQUENCE(s);
                subj.get_observer().on_completed();
            }

            SUBCASE("on_completed from inner + then from base")
            {
                subj.get_observer().on_next(rpp::source::empty<int>());

                REQUIRE_CALL(*mock, on_completed()).IN_SEQUENCE(s);
                subj.get_observer().on_completed();
            }

            SUBCASE("on_completed from base + then from inner")
            {
                rpp::subjects::publish_subject<int> inner{};
                subj.get_observer().on_next(inner.get_observable());
                subj.get_observer().on_completed();

                REQUIRE_CALL(*mock, on_completed()).IN_SEQUENCE(s);
                inner.get_observer().on_completed();
            }
        }
    }
}

TEST_CASE("concat as operator async completiton")
{
    mock_observer<int>                  mock{};
    trompeloeil::sequence               s{};
    rpp::subjects::publish_subject<int> subj{};
    rpp::source::just(subj.get_observable().as_dynamic(), rpp::source::just(100).as_dynamic())
        | rpp::ops::concat()
        | rpp::ops::subscribe(mock);


    REQUIRE_CALL(*mock, on_next_lvalue(1)).IN_SEQUENCE(s);
    subj.get_observer().on_next(1);

    REQUIRE_CALL(*mock, on_next_lvalue(100)).IN_SEQUENCE(s);
    REQUIRE_CALL(*mock, on_completed()).IN_SEQUENCE(s);
    subj.get_observer().on_completed();
}

TEST_CASE("concat disposes on looping")
{
    mock_observer<int> mock{};
    REQUIRE_CALL(*mock, on_next_rvalue(1));
    REQUIRE_CALL(*mock, on_completed());

    rpp::source::concat(rpp::source::create<int>([](auto&& subscriber) {
        auto d = rpp::composite_disposable_wrapper::make();
        subscriber.set_upstream(d);
        subscriber.on_next(1);
        subscriber.on_completed();
        CHECK(d.is_disposed());
    })) | rpp::ops::subscribe(mock);
}

TEST_CASE("concat doesn't produce extra copies")
{
    copy_count_tracker tracker{};
    auto               source       = rpp::source::just(rpp::schedulers::immediate{}, tracker);
    auto               initial_copy = tracker.get_copy_count();
    auto               initial_move = tracker.get_move_count();

    SUBCASE("pass source via copy")
    {
        rpp::source::concat(source) | rpp::ops::subscribe([](const copy_count_tracker&) { });
        CHECK(tracker.get_copy_count() - initial_copy == 2); // 1 copy to observable + 1 copy to observer
        CHECK(tracker.get_move_count() - initial_move == 0);
    }
}

TEST_CASE("concat of iterable doesn't produce extra copies")
{
    copy_count_tracker tracker{};
    auto               source       = std::array{rpp::source::just(rpp::schedulers::immediate{}, tracker)};
    auto               initial_copy = tracker.get_copy_count();
    auto               initial_move = tracker.get_move_count();

    SUBCASE("pass source via copy")
    {
        rpp::source::concat(source) | rpp::ops::subscribe([](const copy_count_tracker&) { });
        CHECK(tracker.get_copy_count() - initial_copy == 2); // 1 copy to observable + 1 copy to observer
        CHECK(tracker.get_move_count() - initial_move == 0);
    }
}

TEST_CASE("concat satisfies disposable contracts")
{
    test_operator_over_observable_with_disposable<int>([](auto&& observable) { return rpp::source::concat(observable); });
}

TEST_CASE("concat as operator satisfies disposable contracts")
{
    test_operator_over_observable_with_disposable<int>([](auto&& observable) { return rpp::source::just(observable) | rpp::ops::concat(); });
    test_operator_over_observable_with_disposable<rpp::dynamic_observable<int>>([](auto&& observable) { return observable | rpp::ops::concat(); });
}
