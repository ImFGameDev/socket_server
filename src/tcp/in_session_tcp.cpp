#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>
#include <iostream>
#include <returnable/hash_events_getter.h>
#include <debug_system.h>

#include "../../include/sessions/tcp/in_session_tcp.h"
#include "commands.h"

namespace main_player::logic::connection
{
    using logger = main_player::core::debug::debug_system;
    using str = std::string;
    using u8 = std::uint8_t;
    using u32 = std::uint32_t;
    using s_t = std::size_t;

    const u32 _MAX_PACKET_SIZE = 10 * 1024 * 1024;

    const float _ping_pong = 30;

    //Public:
    in_session_tcp::in_session_tcp(boost::asio::ip::tcp::socket *socket): _is_closing(false), _data(nullptr),
                                                                          _time_wait_ping(0), _is_run(true)
    {
        _socket = new boost::asio::ip::tcp::socket(std::move(*socket));
        _data_tag = new char[4];
        _event = new main_player::core::actions::hash_events_getter<u8, const str &>();

        read_length();

        _event->add_listener(SESSION_PING, [this](const str &)
        {
            _time_wait_ping = 0;

            send(SESSION_PING, "");
        });
    }

    in_session_tcp::~in_session_tcp()
    {
        if (_socket)
        {
            try
            {
                boost::system::error_code ec;
                if (_socket->is_open())
                {
                    _socket->close(ec);

                    if (ec) std::cerr << "destruct close error: " << ec.message() << std::endl;
                }
            } catch (const std::exception &e)
            {
                std::cerr << "exception during close: " << e.what() << std::endl;
            }

            delete _socket;
        }

        delete _event;
        delete[] _data_tag;
        delete[] _data;
        _data = nullptr;
    }

    //Private:
    void in_session_tcp::read_length()
    {
        if (!_socket->is_open())
        {
            close();
            return;
        }

        async_read(*_socket, boost::asio::buffer(_data_tag, 4),
                   [this](boost::system::error_code ec, s_t length) -> void
                   {
                       try
                       {
                           if (ec)
                           {
                               if (ec != boost::asio::error::operation_aborted)
                                   logger::error("tcp_session",
                                                 "socket read length failed: " + ec.
                                                 message());

                               close();
                               return;
                           }

                           if (length != 4)
                           {
                               logger::error("tcp_session",
                                             "invalid length header size: " +
                                             std::to_string(length));
                               close();
                               return;
                           }

                           u32 packet_length = 0;
                           memcpy(&packet_length, _data_tag, 4);

                           if (packet_length == 0 || packet_length > _MAX_PACKET_SIZE)
                           {
                               logger::error("tcp_session",
                                             "invalid packet length: " + std::to_string(
                                                 packet_length));
                               close();
                               return;
                           }

                           read_data(packet_length);
                       } catch (const std::exception &e)
                       {
                           logger::error("tcp_session",
                                         "read_length exception: " + str(e.what()));
                           close();
                       }
                   });
    }

    void in_session_tcp::read_data(u32 length)
    {
        _data = new char[length];
        async_read(*_socket, boost::asio::buffer(_data, length),
                   [this, length](boost::system::error_code ec, s_t bytes_read) -> void
                   {
                       if (ec)
                       {
                           logger::error(
                               "tcp_session", "read data failed: " + ec.message());

                           delete[] _data;
                           _data = nullptr;

                           close();
                           return;
                       }

                       if (bytes_read != static_cast<s_t>(length))
                       {
                           str log = "size: " + std::to_string(length) + ", read size: " + std::to_string(bytes_read);

                           logger::error("tcp_session", "incomplete data read. " + log);

                           delete[] _data;
                           _data = nullptr;

                           close();
                           return;
                       }

                       u8 tag = _data[0];
                       int data_length = static_cast<int>(length - 1);

                       str json_data(_data + 1, data_length);

                       delete[] _data;
                       _data = nullptr;

                       logger::log("tcp_session", "read: " + std::to_string(tag));

                       _event->invoke(tag, json_data);


                       read_length();
                   });
    }

    void in_session_tcp::send_internal(const u8 &tag, const str &json,
                                       const std::function<void(boost::system::error_code, s_t)> &callback
    )
    {
        std::lock_guard<std::mutex> lock(_socket_mutex);
        if (!_socket->is_open())
        {
            if (callback) callback(boost::asio::error::not_connected, 0);

            close();
            return;
        }

        _write_queue.emplace_back(tag, json, callback);

        if (!_is_writing)
        {
            process_write_queue();
        }
    }

    void in_session_tcp::process_write_queue()
    {
        if (_write_queue.empty())
        {
            _is_writing = false;
            return;
        }

        _is_writing = true;

        auto [tag, json_str, callback] = std::move(_write_queue.front());
        _write_queue.erase(_write_queue.begin());

        u32 data_length = json_str.length();
        u32 total_length = data_length + 1;

        auto packet = new str();
        packet->reserve(4 + 1 + data_length);
        packet->append(reinterpret_cast<char *>(&total_length), 4);
        packet->push_back(static_cast<char>(tag));
        packet->append(json_str);

        logger::log("tcp_session", "send: " + std::to_string(tag));

        async_write(*_socket, boost::asio::buffer(*packet),
                    [this, callback, packet](boost::system::error_code ec, s_t bytes_transferred)
                    {
                        if (callback) callback(ec, bytes_transferred);
                        delete packet;

                        std::lock_guard<std::mutex> lock(_socket_mutex);
                        process_write_queue();
                    });
    }

    void in_session_tcp::close()
    {
        if (_is_closing.exchange(true)) return;

        boost::system::error_code ec_cancel;
        boost::system::error_code ec_close;
        std::lock_guard<std::mutex> lock(_socket_mutex);

        if (_socket && _socket->is_open())
        {
            _socket->cancel(ec_cancel);
            _socket->close(ec_close);

            if (ec_close) logger::error("tcp_session", "close error: " + ec_close.message());
        }

        if (_close_callback)
        {
            logger::log_green("tcp_session", "close callback");

            _close_callback();
        } else logger::error("tcp_session", "close callback null");
    }

    //Public:
    void in_session_tcp::set_listener_close(const std::function<void()> &callback)
    {
        _close_callback = callback;
    }

    void in_session_tcp::remove_listener(const u8 &tag)
    {
        _event->remove_listeners(tag);
    }

    void in_session_tcp::add_listener(const u8 &tag, std::function<void(const str &)> func)
    {
        _event->add_listener(tag, func);
    }

    void in_session_tcp::send(const u8 &tag, const str &json)
    {
        auto callback = [this](boost::system::error_code ec, s_t) -> void
        {
            if (ec)
            {
                logger::error("tcp_session",
                              "error writing to socket: " + ec.message());
                close();
            }
        };

        send_internal(tag, json, callback);
    }

    void in_session_tcp::send(const u8 &tag, const str &json, std::function<void(bool)> on_send)
    {
        auto cachedCallback = std::move(on_send);
        auto callbackWrite = [this, cachedCallback](boost::system::error_code ec, s_t) -> void
        {
            if (!ec)
            {
                if (cachedCallback) cachedCallback(true);
                return;
            }

            logger::error("tcp_session", "error writing to socket: " + ec.message());
            if (cachedCallback) cachedCallback(false);
            close();
        };

        send_internal(tag, json, callbackWrite);
    }

    bool in_session_tcp::is_closed()
    {
        return !_is_run;
    }

    void in_session_tcp::tick(const float &delta)
    {
        if (!_is_run) return;

        _time_wait_ping += delta;

        if (_time_wait_ping > _ping_pong)
        {
            _is_run = false;

            close();
        }
    }
}
