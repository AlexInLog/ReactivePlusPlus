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

#include <rpp/utils/constraints.hpp>

#include <grpcpp/support/status.h>

#include "rpp/subjects/publish_subject.hpp"

#include <deque>
#include <mutex>

namespace rppgrpc::details
{
    template<rpp::constraint::decayed_type TData>
    class base_writer
    {
    public:
        base_writer()
        {
            m_subject.get_observable().subscribe(typename details::base_writer<TData>::observer_strategy{*this});
        }

        virtual ~base_writer() noexcept = default;

        auto get_observer() const
        {
            return m_subject.get_observer();
        }

    protected:
        virtual void start_write(const TData& v)               = 0;
        virtual void finish_writes(const grpc::Status& status) = 0;

        void handle_on_done()
        {
            m_subject.get_disposable().dispose();
        }

        void handle_write_done()
        {
            std::lock_guard lock{m_write_mutex};
            m_write.pop_front();

            if (!m_write.empty())
            {
                start_write(m_write.front());
            }
            else if (m_finished)
            {
                finish_writes(grpc::Status::OK);
            }
        }

        struct observer_strategy
        {
            static constexpr auto preferred_disposables_mode = rpp::details::observers::disposables_mode::Auto;

            std::reference_wrapper<base_writer> owner{};

            template<rpp::constraint::decayed_same_as<TData> T>
            void on_next(T&& message) const
            {
                std::lock_guard lock{owner.get().m_write_mutex};
                owner.get().m_write.push_back(std::forward<T>(message));
                if (owner.get().m_write.size() == 1)
                    owner.get().start_write(owner.get().m_write.front());
            }

            void on_error(const std::exception_ptr&) const
            {
                std::lock_guard lock{owner.get().m_write_mutex};
                owner.get().m_finished = true;

                if (owner.get().m_write.size() == 0)
                    owner.get().finish_writes(grpc::Status{grpc::StatusCode::INTERNAL, "Internal error happens"});
            }
            void on_completed() const
            {
                std::lock_guard lock{owner.get().m_write_mutex};
                owner.get().m_finished = true;

                if (owner.get().m_write.size() == 0)
                    owner.get().finish_writes(grpc::Status::OK);
            }

            static constexpr bool is_disposed() { return false; }
            static constexpr void set_upstream(const rpp::disposable_wrapper&) { }
        };

    private:
        rpp::subjects::serialized_publish_subject<TData> m_subject{};

        std::mutex        m_write_mutex{};
        std::deque<TData> m_write{};
        bool              m_finished{};
    };

    template<rpp::constraint::decayed_type TData>
    class base_reader
    {
    public:
        base_reader()          = default;
        virtual ~base_reader() = default;

        auto get_observable()
        {
            return m_observer.get_observable();
        }

    protected:
        virtual void start_read(TData& data) = 0;

        void handle_read_done(bool initial = false)
        {
            if (!initial)
                m_observer.get_observer().on_next(m_data);
            start_read(m_data);
        }

        void handle_on_done(std::exception_ptr err)
        {
            if (err)
                m_observer.get_observer().on_error(err);
            else
                m_observer.get_observer().on_completed();
        }

    private:
        rpp::subjects::publish_subject<TData> m_observer;
        RPP_NO_UNIQUE_ADDRESS TData           m_data{};
    };

} // namespace rppgrpc::details
