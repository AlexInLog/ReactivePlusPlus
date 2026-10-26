//                   ReactivePlusPlus library
//
//           Copyright Aleksey Loginov 2023 - present.
//  Distributed under the Boost Software License, Version 1.0.
//     (See accompanying file LICENSE_1_0.txt or copy at
//           https://www.boost.org/LICENSE_1_0.txt)
//
//  Project home: https://github.com/AlexInLog/ReactivePlusPlus

#pragma once

#include <rpp/disposables/disposable_wrapper.hpp>
#include <rpp/utils/exceptions.hpp>

#include <algorithm>
#include <new>
#include <vector>

namespace rpp::details::disposables
{
    class dynamic_disposables_container
    {
    public:
        explicit dynamic_disposables_container() = default;

        dynamic_disposables_container(const dynamic_disposables_container&)           = delete;
        dynamic_disposables_container(dynamic_disposables_container&& other) noexcept = default;

        dynamic_disposables_container& operator=(const dynamic_disposables_container& other)     = delete;
        dynamic_disposables_container& operator=(dynamic_disposables_container&& other) noexcept = default;

        void push_back(const rpp::disposable_wrapper& d)
        {
            m_data.push_back(d);
        }

        void push_back(rpp::disposable_wrapper&& d)
        {
            m_data.push_back(std::move(d));
        }

        void remove(const rpp::disposable_wrapper& d)
        {
            m_data.erase(std::remove(m_data.begin(), m_data.end(), d), m_data.end());
        }

        void dispose() const
        {
            for (const auto& d : m_data)
            {
                d.dispose();
            }
        }

        void clear()
        {
            m_data.clear();
        }

    private:
        mutable std::vector<rpp::disposable_wrapper> m_data{};
    };

    template<size_t Count>
    class static_disposables_container
    {
    public:
        static_disposables_container()                                                     = default;
        static_disposables_container(const static_disposables_container&)                  = delete;
        static_disposables_container& operator=(const static_disposables_container& other) = delete;

        static_disposables_container& operator=(static_disposables_container&& other) noexcept
        {
            if (this == &other)
                return *this;

            m_size = other.m_size;
            for (size_t i = 0; i < m_size; ++i)
                std::construct_at(get(i), std::move(*other.get(i)));

            other.clear();
            return *this;
        }

        static_disposables_container(static_disposables_container&& other) noexcept
        {
            *this = std::move(other);
        }

        ~static_disposables_container() noexcept
        {
            clear();
        }

        void push_back(const rpp::disposable_wrapper& d)
        {
            if (m_size >= Count)
                throw rpp::utils::more_disposables_than_expected{"static_disposables_container obtained more disposables than expected"};
            std::construct_at(get(m_size++), d);
        }

        void push_back(rpp::disposable_wrapper&& d)
        {
            if (m_size >= Count)
                throw rpp::utils::more_disposables_than_expected{"static_disposables_container obtained more disposables than expected"};
            std::construct_at(get(m_size++), std::move(d));
        }

        void remove(const rpp::disposable_wrapper& d)
        {
            for (size_t i = 0; i < m_size;)
            {
                if (*get(i) != d)
                {
                    ++i;
                    continue;
                }

                for (size_t j = i + 1; j < m_size; ++j)
                    *get(j - 1) = std::move(*get(j));

                std::destroy_at(get(--m_size));
            }
        }

        void dispose() const
        {
            for (size_t i = 0; i < m_size; ++i)
            {
                get(i)->dispose();
            }
        }

        void clear()
        {
            for (size_t i = 0; i < m_size; ++i)
                std::destroy_at(get(i));
            m_size = 0;
        }

    private:
        const rpp::disposable_wrapper* get(size_t i) const
        {
            return std::launder(reinterpret_cast<const rpp::disposable_wrapper*>(&m_data[i * sizeof(rpp::disposable_wrapper)]));
        }
        rpp::disposable_wrapper* get(size_t i)
        {
            return std::launder(reinterpret_cast<rpp::disposable_wrapper*>(&m_data[i * sizeof(rpp::disposable_wrapper)]));
        }

    private:
        alignas(rpp::disposable_wrapper) std::byte m_data[sizeof(rpp::disposable_wrapper) * Count]{};
        size_t m_size{};
    };

    template<>
    class static_disposables_container<0>
    {
    public:
        [[noreturn]] static void push_back(const rpp::disposable_wrapper&)
        {
            throw rpp::utils::more_disposables_than_expected{"static_disposables_container<0> expected no disposables but received at least one"};
        }

        static void remove(const rpp::disposable_wrapper&) { }
        static void dispose() { }
        static void clear() { }
    };
} // namespace rpp::details::disposables
