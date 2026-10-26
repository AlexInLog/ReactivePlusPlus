//                  ReactivePlusPlus library
//
//          Copyright Aleksey Loginov 2022 - present.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          https://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/AlexInLog/ReactivePlusPlus
//

#include <QObject>

struct test_q_object : public QObject
{
    Q_OBJECT

public:
    using QObject::QObject;

    void emit_single_value_signal(int v)
    {
        emit single_value_signal(v);
    }

    void emit_multiple_value_signal(int v, double d, const std::string& s)
    {
        emit multiple_value_signal(v, d, s);
    }

    void emit_no_value_signal()
    {
        emit no_value_signal();
    }

Q_SIGNALS:
    void single_value_signal(int v);
    void multiple_value_signal(int v, double d, const std::string& s);
    void no_value_signal();
};

#include <doctest/doctest.h>

#include <rpp/observers/mock_observer.hpp>

#include <rppqt/sources/from_signal.hpp>

#include "copy_count_tracker.hpp"
#include "test_from_signal.moc"

TEST_CASE("from_signal can see object value from object signal")
{
    SUBCASE("qobject with signal with 1 argument and observable from signal from this object")
    {
        mock_observer_strategy<int> mock_observer{};
        auto                        testobject = std::make_unique<test_q_object>();
        auto                        obs        = rppqt::source::from_signal(*testobject, &test_q_object::single_value_signal);
        SUBCASE("emit signal, subscribe on it and emit signal")
        {
            testobject->emit_single_value_signal(1);
            obs.subscribe(mock_observer);
            testobject->emit_single_value_signal(2);
            SUBCASE("subscriber sees only emission after subscribe")
            {
                CHECK(mock_observer.get_received_values() == std::vector<int>{2});
                CHECK(mock_observer.get_on_error_count() == 0);
                CHECK(mock_observer.get_on_completed_count() == 0);
            }
            SUBCASE("object destroyed")
            {
                testobject.reset();
                SUBCASE("subscriber sees completion")
                {
                    CHECK(mock_observer.get_received_values() == std::vector<int>{2});
                    CHECK(mock_observer.get_on_error_count() == 0);
                    CHECK(mock_observer.get_on_completed_count() == 1);
                }
            }
        }
        SUBCASE("object destroyed before subscription")
        {
            testobject.reset();
            SUBCASE("SUBCASE subscriber subscribed")
            {
                obs.subscribe(mock_observer);

                SUBCASE("subscriber sees only completion")
                {
                    CHECK(mock_observer.get_received_values() == std::vector<int>{});
                    CHECK(mock_observer.get_on_error_count() == 0);
                    CHECK(mock_observer.get_on_completed_count() == 1);
                }
            }
        }
    }
}

TEST_CASE("from_signal sends tuple if multiple values")
{
    SUBCASE("object with signal with multiple values and observable from this signal")
    {
        mock_observer_strategy<std::tuple<int, double, std::string>> mock_observer{};
        auto                                                         testobject = std::make_unique<test_q_object>();
        auto                                                         obs        = rppqt::source::from_signal(*testobject, &test_q_object::multiple_value_signal);
        SUBCASE("subscribe on it and emit signal")
        {
            obs.subscribe(mock_observer);
            testobject->emit_multiple_value_signal(1, 2, "31");
            SUBCASE("subscriber sees values")
            {
                CHECK(mock_observer.get_received_values() == std::vector<std::tuple<int, double, std::string>>{std::tuple{1, 2.0, "31"}});
                CHECK(mock_observer.get_on_error_count() == 0);
                CHECK(mock_observer.get_on_completed_count() == 0);
            }
        }
    }
}

TEST_CASE("from_signal sends special struct if no args in signal")
{
    SUBCASE("object with signal with zero values and observable from this signal")
    {
        mock_observer_strategy<rpp::utils::none> mock_observer{};
        auto                                     testobject = std::make_unique<test_q_object>();
        auto                                     obs        = rppqt::source::from_signal(*testobject, &test_q_object::no_value_signal);

        SUBCASE("subscribe on it and emit signal")
        {
            obs.subscribe(mock_observer);
            testobject->emit_no_value_signal();
            SUBCASE("subscriber sees values")
            {
                CHECK(mock_observer.get_received_values().size() == 1);
                CHECK(mock_observer.get_on_error_count() == 0);
                CHECK(mock_observer.get_on_completed_count() == 0);
            }
        }
    }
}
