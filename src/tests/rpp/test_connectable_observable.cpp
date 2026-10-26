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

#include <rpp/observables/connectable_observable.hpp>
#include <rpp/observables/dynamic_connectable_observable.hpp>
#include <rpp/observables/dynamic_observable.hpp>
#include <rpp/observers/mock_observer.hpp>
#include <rpp/operators/map.hpp>
#include <rpp/operators/multicast.hpp>
#include <rpp/operators/publish.hpp>
#include <rpp/operators/ref_count.hpp>
#include <rpp/operators/share.hpp>
#include <rpp/sources/create.hpp>
#include <rpp/sources/just.hpp>
#include <rpp/subjects/publish_subject.hpp>

TEST_CASE("connectable observable")
{
    auto mock = mock_observer_strategy<int>{};
    auto d    = rpp::composite_disposable_wrapper::make();

    SUBCASE("source and connectable observable from it")
    {
        auto source = rpp::source::just(1);
        auto test   = [&](auto&& connectable) {
            SUBCASE("subscribe on connectable")
            {
                connectable.subscribe(mock.get_observer(d));
                SUBCASE("nothing happens")
                {
                    CHECK(mock.get_total_on_next_count() == 0);
                    CHECK(mock.get_on_error_count() == 0);
                    CHECK(mock.get_on_completed_count() == 0);
                    CHECK(!d.is_disposed());
                }
                SUBCASE("call connect")
                {
                    auto sub_connectable = connectable.connect();
                    SUBCASE("observer obtains values")
                    {
                        CHECK(mock.get_received_values() == std::vector{1});
                        CHECK(mock.get_on_error_count() == 0);
                        CHECK(mock.get_on_completed_count() == 1);
                        CHECK(d.is_disposed());
                        CHECK(sub_connectable.is_disposed());
                    }
                }
                SUBCASE("call connect multiple times")
                {
                    auto sub_connectable = connectable.connect();
                    connectable.connect();
                    SUBCASE("observer obtains values")
                    {
                        CHECK(mock.get_received_values() == std::vector{1});
                        CHECK(mock.get_on_error_count() == 0);
                        CHECK(mock.get_on_completed_count() == 1);
                        CHECK(d.is_disposed());
                        CHECK(sub_connectable.is_disposed());
                    }
                }
            }
            SUBCASE("subscribe on connecatble via map and connect")
            {
                connectable | rpp::ops::map([](int val) { return val * 10; }) | rpp::ops::subscribe(mock.get_observer(d));
                auto sub_connectable = connectable.connect();
                SUBCASE("observer obtains modified values")
                {
                    CHECK(mock.get_received_values() == std::vector{10});
                    CHECK(mock.get_on_error_count() == 0);
                    CHECK(mock.get_on_completed_count() == 1);
                    CHECK(d.is_disposed());
                    CHECK(sub_connectable.is_disposed());
                }
            }
        };
        SUBCASE("connectable created manually")
        test(rpp::connectable_observable{source, rpp::subjects::publish_subject<int>{}});
        SUBCASE("dynamic_connectable created manually")
        test(rpp::dynamic_connectable_observable<rpp::subjects::publish_subject<int>>{source, rpp::subjects::publish_subject<int>{}});
        SUBCASE("dynamic_connectable coneverted")
        test(rpp::dynamic_connectable_observable<rpp::subjects::publish_subject<int>>{rpp::connectable_observable{source, rpp::subjects::publish_subject<int>{}}});
        SUBCASE("connectable created via multicast")
        test(source | rpp::ops::multicast(rpp::subjects::publish_subject<int>{}));
        SUBCASE("connectable created via templated multicast")
        test(source | rpp::ops::multicast<rpp::subjects::publish_subject>());
    }
    SUBCASE("subject as source and connectable observable from it")
    {
        auto source = rpp::subjects::publish_subject<int>();
        auto test   = [&](auto&& connectable) {
            SUBCASE("subscribe on connectable and connect")
            {
                connectable.subscribe(mock.get_observer(d));
                auto sub_connectable = connectable.connect();
                SUBCASE("call connect again and send value")
                {
                    auto new_sub_connectable = connectable.connect();
                    source.get_observer().on_next(1);

                    SUBCASE("observer obtains values only once")
                    {
                        CHECK(mock.get_received_values() == std::vector{1});
                        CHECK(mock.get_on_error_count() == 0);
                        CHECK(mock.get_on_completed_count() == 0);
                        CHECK(!d.is_disposed());
                        CHECK(!sub_connectable.is_disposed());
                    }
                }
                SUBCASE("unsubscribe connected subscription before any values from source")
                {
                    sub_connectable.dispose();
                    source.get_observer().on_next(1);
                    SUBCASE("subscriber obtains nothing")
                    {
                        CHECK(mock.get_total_on_next_count() == 0);
                        CHECK(mock.get_on_error_count() == 0);
                        CHECK(mock.get_on_completed_count() == 0);
                        CHECK(!d.is_disposed());
                        CHECK(sub_connectable.is_disposed());
                        CHECK(!source.get_disposable().is_disposed());
                    }
                    SUBCASE("connect again and send values")
                    {
                        auto new_sub_connectable = connectable.connect();
                        source.get_observer().on_next(1);

                        SUBCASE("subscriber obtains values")
                        {
                            CHECK(mock.get_total_on_next_count() == 1);
                            CHECK(mock.get_on_error_count() == 0);
                            CHECK(mock.get_on_completed_count() == 0);
                            CHECK(!d.is_disposed());
                            CHECK(sub_connectable.is_disposed());
                            CHECK(!source.get_disposable().is_disposed());
                        }
                    }
                }
                SUBCASE("obtain on_completed")
                {
                    source.get_observer().on_completed();
                    SUBCASE("subscribe obtains on_completed and unsubscribe initiated")
                    {
                        CHECK(mock.get_total_on_next_count() == 0);
                        CHECK(mock.get_on_error_count() == 0);
                        CHECK(mock.get_on_completed_count() == 1);
                        CHECK(d.is_disposed());
                        CHECK(sub_connectable.is_disposed());
                    }
                    SUBCASE("connect again and send values")
                    {
                        auto new_sub_connectable = connectable.connect();
                        source.get_observer().on_next(1);
                        SUBCASE("subscriber obtains nothing")
                        {
                            CHECK(mock.get_total_on_next_count() == 0);
                            CHECK(mock.get_on_error_count() == 0);
                            CHECK(mock.get_on_completed_count() == 1);
                            CHECK(d.is_disposed());
                            CHECK(sub_connectable.is_disposed());
                            CHECK(new_sub_connectable.is_disposed());
                        }
                    }
                }
            }
        };
        SUBCASE("connectable created manually")
        test(rpp::connectable_observable{source.get_observable(), rpp::subjects::publish_subject<int>{}});
        SUBCASE("dynamic_connectable created manually")
        test(rpp::dynamic_connectable_observable<rpp::subjects::publish_subject<int>>{source.get_observable(), rpp::subjects::publish_subject<int>{}});
        SUBCASE("connectable created via multicast")
        test(source.get_observable() | rpp::ops::multicast(rpp::subjects::publish_subject<int>{}));
        SUBCASE("connectable created via templated multicast")
        test(source.get_observable() | rpp::ops::multicast<rpp::subjects::publish_subject>());
    }
    SUBCASE("observable")
    {
        auto source = rpp::source::just(1);
        SUBCASE("call publish on it")
        {
            auto published = source | rpp::ops::publish();
            SUBCASE("published observable is same as Connectable with publish_subject")
            {
                static_assert(rpp::constraint::decayed_same_as<decltype(published), rpp::connectable_observable<decltype(source), rpp::subjects::publish_subject<int>>>);
            }
        }
        SUBCASE("call multicast on it with publish_subject")
        {
            auto published = source | rpp::ops::multicast(rpp::subjects::publish_subject<int>{});
            SUBCASE("published observable is same as Connectable with publish_subject")
            {
                static_assert(rpp::constraint::decayed_same_as<decltype(published), rpp::connectable_observable<decltype(source), rpp::subjects::publish_subject<int>>>);
            }
        }
        SUBCASE("call template multicast on it with publish_subject")
        {
            auto published = source | rpp::ops::multicast();
            SUBCASE("published observable is same as Connectable with publish_subject")
            {
                static_assert(rpp::constraint::decayed_same_as<decltype(published), rpp::connectable_observable<decltype(source), rpp::subjects::publish_subject<int>>>);
            }
        }
    }
}

TEST_CASE("ref_count")
{
    auto observer_1 = mock_observer_strategy<int>{};
    auto observer_2 = mock_observer_strategy<int>{};
    SUBCASE("connectable observable from just")
    {
        auto observable = rpp::source::just(1) | rpp::ops::multicast();

        SUBCASE("subscribe on it without ref_count")
        {
            observable.subscribe(observer_1);
            SUBCASE("nothing happens")
            {
                CHECK(observer_1.get_total_on_next_count() == 0);
                CHECK(observer_1.get_on_error_count() == 0);
                CHECK(observer_1.get_on_completed_count() == 0);
            }
            SUBCASE("subscribe on it another observer with ref_count")
            {
                observable.ref_count().subscribe(observer_2);
                SUBCASE("both observers obtain values")
                {
                    auto validate = [](auto observer) {
                        CHECK(observer.get_received_values() == std::vector{1});
                        CHECK(observer.get_total_on_next_count() == 1);
                        CHECK(observer.get_on_error_count() == 0);
                        CHECK(observer.get_on_completed_count() == 1);
                    };
                    validate(observer_1);
                    validate(observer_2);
                }
            }
        }
    }
    SUBCASE("connectable observable from subject")
    {
        auto subj       = rpp::subjects::publish_subject<int>{};
        auto observable = subj.get_observable() | rpp::ops::multicast();

        SUBCASE("subscribe on it without ref_count and with ref_count")
        {
            observable.subscribe(observer_1);
            auto sub = rpp::composite_disposable_wrapper::make();
            observable.ref_count().subscribe(rpp::composite_disposable_wrapper{sub}, observer_2);
            SUBCASE("send value")
            {
                subj.get_observer().on_next(1);
                SUBCASE("both observers obtain values")
                {
                    auto validate = [](auto observer) {
                        CHECK(observer.get_received_values() == std::vector{1});
                        CHECK(observer.get_total_on_next_count() == 1);
                        CHECK(observer.get_on_error_count() == 0);
                        CHECK(observer.get_on_completed_count() == 0);
                    };
                    validate(observer_1);
                    validate(observer_2);
                }
            }
            SUBCASE("unsubscribe observer with ref_count and send value")
            {
                sub.dispose();
                subj.get_observer().on_next(1);
                SUBCASE("no observers obtain values")
                {
                    auto validate = [](auto observer) {
                        CHECK(observer.get_total_on_next_count() == 0);
                        CHECK(observer.get_on_error_count() == 0);
                        CHECK(observer.get_on_completed_count() == 0);
                    };
                    validate(observer_1);
                    validate(observer_2);
                }
                SUBCASE("subscribe via ref_count again and send value")
                {
                    sub = rpp::composite_disposable_wrapper::make();
                    observable.ref_count().subscribe(rpp::composite_disposable_wrapper{sub}, observer_2);
                    subj.get_observer().on_next(1);
                    SUBCASE("both observers obtain values")
                    {
                        auto validate = [](auto observer) {
                            CHECK(observer.get_received_values() == std::vector{1});
                            CHECK(observer.get_total_on_next_count() == 1);
                            CHECK(observer.get_on_error_count() == 0);
                            CHECK(observer.get_on_completed_count() == 0);
                        };
                        validate(observer_1);
                        validate(observer_2);
                    }
                }
            }
        }
        SUBCASE("subscribe both with ref_count")
        {
            observable.ref_count().subscribe(observer_1);
            auto sub = rpp::composite_disposable_wrapper::make();
            observable.ref_count().subscribe(rpp::composite_disposable_wrapper{sub}, observer_2);
            SUBCASE("send value")
            {
                subj.get_observer().on_next(1);
                SUBCASE("both observers obtain values")
                {
                    auto validate = [](auto observer) {
                        CHECK(observer.get_received_values() == std::vector{1});
                        CHECK(observer.get_total_on_next_count() == 1);
                        CHECK(observer.get_on_error_count() == 0);
                        CHECK(observer.get_on_completed_count() == 0);
                    };
                    validate(observer_1);
                    validate(observer_2);
                }
            }
            SUBCASE("unsubscribe 1 observer with ref_count and send value")
            {
                sub.dispose();
                subj.get_observer().on_next(1);
                SUBCASE("first observer obtains values")
                {
                    CHECK(observer_1.get_received_values() == std::vector{1});
                    CHECK(observer_1.get_total_on_next_count() == 1);
                    CHECK(observer_1.get_on_error_count() == 0);
                    CHECK(observer_1.get_on_completed_count() == 0);

                    CHECK(observer_2.get_total_on_next_count() == 0);
                    CHECK(observer_2.get_on_error_count() == 0);
                    CHECK(observer_2.get_on_completed_count() == 0);
                }
            }
        }
    }
}

TEST_CASE("share")
{
    auto observer_1 = mock_observer_strategy<int>{};
    auto observer_2 = mock_observer_strategy<int>{};

    SUBCASE("share is multicast + ref_count")
    {
        auto source = rpp::source::just(1);
        static_assert(rpp::constraint::decayed_same_as<decltype(source | rpp::ops::share()),
                                                       decltype(source | rpp::ops::publish() | rpp::ops::ref_count())>);
    }

    SUBCASE("shared observable over subject")
    {
        size_t subscriptions_count{};
        auto   subj = rpp::subjects::publish_subject<int>{};

        auto observable = rpp::source::create<int>([&](auto&& obs) {
                              ++subscriptions_count;
                              subj.get_observable().subscribe(std::forward<decltype(obs)>(obs));
                          })
                        | rpp::ops::share();

        SUBCASE("original observable is not subscribed before any subscription")
        {
            CHECK(subscriptions_count == 0);
        }
        SUBCASE("subscribe both observers")
        {
            observable.subscribe(observer_1);
            auto sub = rpp::composite_disposable_wrapper::make();
            observable.subscribe(rpp::composite_disposable_wrapper{sub}, observer_2);

            SUBCASE("original observable is subscribed only once")
            {
                CHECK(subscriptions_count == 1);
            }
            SUBCASE("send value")
            {
                subj.get_observer().on_next(1);
                SUBCASE("both observers obtain values")
                {
                    auto validate = [](auto observer) {
                        CHECK(observer.get_received_values() == std::vector{1});
                        CHECK(observer.get_total_on_next_count() == 1);
                        CHECK(observer.get_on_error_count() == 0);
                        CHECK(observer.get_on_completed_count() == 0);
                    };
                    validate(observer_1);
                    validate(observer_2);
                }
            }
            SUBCASE("unsubscribe second observer and send value")
            {
                sub.dispose();
                subj.get_observer().on_next(1);
                SUBCASE("only first observer obtains values")
                {
                    CHECK(observer_1.get_received_values() == std::vector{1});
                    CHECK(observer_2.get_total_on_next_count() == 0);
                }
                SUBCASE("original observable is still subscribed")
                {
                    CHECK(subscriptions_count == 1);
                }
            }
        }
        SUBCASE("subscribe, unsubscribe everything and subscribe again")
        {
            auto sub = rpp::composite_disposable_wrapper::make();
            observable.subscribe(rpp::composite_disposable_wrapper{sub}, observer_1);
            sub.dispose();

            observable.subscribe(observer_2);
            SUBCASE("original observable is subscribed again")
            {
                CHECK(subscriptions_count == 2);
            }
            SUBCASE("send value")
            {
                subj.get_observer().on_next(1);
                SUBCASE("only second observer obtains values")
                {
                    CHECK(observer_1.get_total_on_next_count() == 0);
                    CHECK(observer_2.get_received_values() == std::vector{1});
                }
            }
        }
    }
}
