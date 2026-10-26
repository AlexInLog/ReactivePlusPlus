#include <rpp/rpp.hpp>

#include <iostream>

/**
 * @example share.cpp
 **/
int main() // NOLINT(bugprone-exception-escape)
{
    {
        //! [share]
        auto subject = rpp::subjects::publish_subject<int>{};

        auto observable = rpp::source::create<int>([&](auto&& observer) {
                              std::cout << "SUBSCRIBE" << std::endl;
                              subject.get_observable().subscribe(std::forward<decltype(observer)>(observer));
                          })
                        | rpp::ops::share();

        std::cout << "before subscriptions" << std::endl;
        observable.subscribe([](int v) { std::cout << "#1 " << v << std::endl; });
        observable.subscribe([](int v) { std::cout << "#2 " << v << std::endl; });

        subject.get_observer().on_next(1);
        subject.get_observer().on_completed();
        // Output:
        // before subscriptions
        // SUBSCRIBE
        // #1 1
        // #2 1

        //! [share]
    }
    return 0;
}
