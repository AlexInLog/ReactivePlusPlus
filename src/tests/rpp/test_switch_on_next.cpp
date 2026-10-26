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

#include <rpp/observables/dynamic_observable.hpp>
#include <rpp/operators/switch_on_next.hpp>
#include <rpp/sources/empty.hpp>
#include <rpp/sources/error.hpp>
#include <rpp/sources/just.hpp>
#include <rpp/sources/never.hpp>
#include <rpp/subjects/publish_subject.hpp>

#include "copy_count_tracker.hpp"
#include "disposable_observable.hpp"
#include "rpp_trompeloil.hpp"

TEST_CASE("switch_on_next switches observable after obtaining new one")
{
    mock_observer<int>    mock{};
    trompeloeil::sequence s{};

    SUBCASE("just observable of just observables")
    {
        auto observable = rpp::source::just(rpp::source::just(1), rpp::source::just(2), rpp::source::just(3));
        SUBCASE("subscribe on it via switch_on_next - obtains values as from concat")
        {
            REQUIRE_CALL(*mock, on_next_lvalue(1)).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_next_lvalue(2)).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_next_lvalue(3)).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_completed()).IN_SEQUENCE(s);
            observable | rpp::ops::switch_on_next() | rpp::ops::subscribe(mock);
        }
    }
    SUBCASE("just observable of just observables where second is error")
    {
        auto observable = rpp::source::just(rpp::source::just(1).as_dynamic(),
                                            rpp::source::error<int>(std::make_exception_ptr(std::runtime_error{""})).as_dynamic(),
                                            rpp::source::just(3).as_dynamic());
        SUBCASE("subscribe on it via switch_on_next - obtains values as from concat but stops on error")
        {
            REQUIRE_CALL(*mock, on_next_lvalue(1)).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_error(trompeloeil::_)).IN_SEQUENCE(s);

            observable | rpp::ops::switch_on_next() | rpp::ops::subscribe(mock);
        }
    }
    SUBCASE("just observable of just observables where second is completed")
    {
        auto observable = rpp::source::just(rpp::source::just(1).as_dynamic(),
                                            rpp::source::empty<int>().as_dynamic(),
                                            rpp::source::just(3).as_dynamic());
        SUBCASE("subscribe on it via switch_on_next - obtains values as from concat")
        {
            REQUIRE_CALL(*mock, on_next_lvalue(1)).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_next_lvalue(3)).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_completed()).IN_SEQUENCE(s);

            observable | rpp::ops::switch_on_next() | rpp::ops::subscribe(mock);
        }
    }
    SUBCASE("just observable of just observables where second is never")
    {
        auto observable = rpp::source::just(rpp::source::just(1).as_dynamic(),
                                            rpp::source::never<int>().as_dynamic(),
                                            rpp::source::just(3).as_dynamic());
        SUBCASE("subscribe on it via switch_on_next - obtains values as from concat")
        {
            REQUIRE_CALL(*mock, on_next_lvalue(1)).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_next_lvalue(3)).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_completed()).IN_SEQUENCE(s);
            observable | rpp::ops::switch_on_next() | rpp::ops::subscribe(mock);
        }
    }
    SUBCASE("just observable of just observables where last is never")
    {
        auto observable = rpp::source::just(rpp::source::just(1).as_dynamic(),
                                            rpp::source::just(3).as_dynamic(),
                                            rpp::source::never<int>().as_dynamic());
        SUBCASE("subscribe on it via switch_on_next - obtains values as from concat but no complete")
        {
            REQUIRE_CALL(*mock, on_next_lvalue(1)).IN_SEQUENCE(s);
            REQUIRE_CALL(*mock, on_next_lvalue(3)).IN_SEQUENCE(s);

            observable | rpp::ops::switch_on_next() | rpp::ops::subscribe(mock);
        }
    }
    SUBCASE("subject of just subjects")
    {
        auto subj_1           = rpp::subjects::publish_subject<int>();
        auto subj_2           = rpp::subjects::publish_subject<int>();
        auto subj_of_subjects = rpp::subjects::publish_subject<rpp::dynamic_observable<int>>();

        SUBCASE("subscribe on it via switch_on_next")
        {
            subj_of_subjects.get_observable() | rpp::ops::switch_on_next() | rpp::ops::subscribe(mock);
            SUBCASE("send first subject and send values for all subjects")
            {
                subj_1.get_observer().on_next(0);

                subj_of_subjects.get_observer().on_next(subj_1.get_observable().as_dynamic());

                SUBCASE("Only value from first subject obtained")
                {
                    REQUIRE_CALL(*mock, on_next_lvalue(1)).IN_SEQUENCE(s);
                    subj_1.get_observer().on_next(1);
                    subj_2.get_observer().on_next(2);
                }
                SUBCASE("send second subject and send values for all subjects")
                {
                    subj_of_subjects.get_observer().on_next(subj_2.get_observable().as_dynamic());

                    SUBCASE("Only value from second subject obtained")
                    {
                        subj_1.get_observer().on_next(1);

                        REQUIRE_CALL(*mock, on_next_lvalue(2)).IN_SEQUENCE(s);
                        subj_2.get_observer().on_next(2);
                    }
                }
                SUBCASE("original subject completes but provided send value")
                {
                    subj_of_subjects.get_observer().on_completed();

                    REQUIRE_CALL(*mock, on_next_lvalue(1)).IN_SEQUENCE(s);
                    subj_1.get_observer().on_next(1);
                    subj_2.get_observer().on_next(2);
                    SUBCASE("subject sends on_completed")
                    {
                        REQUIRE_CALL(*mock, on_completed()).IN_SEQUENCE(s);
                        subj_1.get_observer().on_completed();
                    }
                }
            }
        }
    }
    SUBCASE("switch_on_next completes right")
    {
        rpp::subjects::publish_subject<rpp::dynamic_observable<int>> subj{};

        subj.get_observable() | rpp::ops::switch_on_next() | rpp::ops::subscribe(mock);
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
            subj.get_observer().on_next(rpp::source::empty<int>());
            subj.get_observer().on_next(rpp::source::never<int>());

            rpp::subjects::publish_subject<int> inner{};
            subj.get_observer().on_next(inner.get_observable());
            subj.get_observer().on_completed();

            REQUIRE_CALL(*mock, on_completed()).IN_SEQUENCE(s);
            inner.get_observer().on_completed();
        }
    }
}

TEST_CASE("switch_on_next doesn't produce extra copies")
{
    SUBCASE("observable and subscriber")
    {
        copy_count_tracker verifier{};
        auto               obs = rpp::source::just(verifier.get_observable()) | rpp::ops::switch_on_next();
        SUBCASE("subscribe")
        {
            obs | rpp::ops::subscribe([](copy_count_tracker) { }); // NOLINT
            SUBCASE("no extra copies")
            {
                REQUIRE(verifier.get_copy_count() == 1); // 1 copy to final lambda
                REQUIRE(verifier.get_move_count() == 0);
            }
        }
    }
}

TEST_CASE("switch_on_next doesn't produce extra copies for move")
{
    SUBCASE("observable and subscriber")
    {
        copy_count_tracker verifier{};
        auto               obs = rpp::source::just(verifier.get_observable_for_move()) | rpp::ops::switch_on_next();
        SUBCASE("subscribe")
        {
            obs | rpp::ops::subscribe([](copy_count_tracker) { }); // NOLINT
            SUBCASE("no extra copies")
            {
                REQUIRE(verifier.get_copy_count() == 0);
                REQUIRE(verifier.get_move_count() == 1); // 1 move to final lambda
            }
        }
    }
}

TEST_CASE("switch_on_next handles race condition")
{
    SUBCASE("source observable in current thread pairs with error in other thread")
    {
        std::atomic_bool on_error_called{false};
        auto             subject = rpp::subjects::publish_subject<rpp::dynamic_observable<int>>{};
        SUBCASE("subscribe on it")
        {
            SUBCASE("on_error can't interleave with on_next")
            {
                std::thread th{};

                subject.get_observable()
                    | rpp::ops::switch_on_next()
                    | rpp::ops::subscribe([&](auto&&) {
                                       CHECK(!on_error_called);
                                       th = std::thread{[&]
                                       {
                                           subject.get_observer().on_error(std::exception_ptr{});
                                       }};
                                       std::this_thread::sleep_for(std::chrono::seconds{1});
                                       CHECK(!on_error_called); },
                                          [&](auto) { on_error_called = true; });

                subject.get_observer().on_next(rpp::source::just(1));

                th.join();
                CHECK(on_error_called);
            }
        }
    }
}
