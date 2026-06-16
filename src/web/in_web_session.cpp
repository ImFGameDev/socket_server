#include <iostream>

#include "../../include/sessions/web/in_web_session.h"
#include <returnable/hash_events_getter.h>
#include <debug_system.h>

#include "commands.h"

namespace main_player::logic::connection
{
    using logger = main_player::core::debug::debug_system;
    using str = std::string;
    using u8 = std::uint8_t;
    using s_t = std::size_t;

    constexpr int _ping_pong_timeout = 30;

    void in_web_session::read_message()
    {
        if (!_ws->is_open() || _is_closing)
        {
            close();
            return;
        }

        auto buffer = std::make_shared<boost::beast::flat_buffer>();

        _ws->async_read(*buffer, [this, buffer](boost::beast::error_code ec, s_t bytes_transferred)
        {
            if (ec)
            {
                if (ec != boost::beast::websocket::error::closed)
                    logger::error("web_session", "WebSocket read failed: " + ec.message());

                close();
                return;
            }

            try
            {
                auto data = buffer->data();
                str message(static_cast<const char *>(data.data()), data.size());

                if (message.length() < 1)
                {
                    logger::error("web_session", "Empty WebSocket message");
                    close();
                    return;
                }

                auto tag = static_cast<u8>(message[0]);
                str json_data = message.substr(1);
                str log = "read: " + std::to_string(tag) + '/' + json_data;

                logger::log("web_session", log);

                _event->invoke(tag, json_data);

                read_message();
            } catch (const std::exception &e)
            {
                logger::error("web_session",
                              "Message processing error: " + str(e.what()));
                close();
            }
        });
    }

    void in_web_session::process_write_queue()
    {
        if (_write_queue.empty() || _is_closing || !_ws->is_open())
        {
            _is_writing = false;
            return;
        }

        _is_writing = true;

        auto [tag, json, callback] = _write_queue.front();

        _write_queue.pop_front();

        try
        {
            str message;

            message.reserve(1 + json.length());
            message.push_back(static_cast<char>(tag));
            message.append(json);

            logger::log("web_session", "send: " + std::to_string(tag) + '/' + json);

            _ws->async_write(boost::asio::buffer(message), [this, callback](boost::beast::error_code ec, s_t)
            {
                std::lock_guard<std::mutex> lock(_write_mutex);

                if (ec)
                {
                    logger::error("web_session",
                                  "WebSocket write error: " + ec.message());
                    if (callback) callback(false);
                    close();
                    return;
                }

                if (callback) callback(true);

                process_write_queue();
            });
        } catch (const std::exception &e)
        {
            logger::error("session", "Send preparation error: " + str(e.what()));

            if (callback) callback(false);
            close();
        }
    }

    void in_web_session::send_internal(const u8 &tag, const str &json, std::function<void(bool)> callback)
    {
        if (!_ws->is_open() || _is_closing)
        {
            logger::error("web_session", "Cannot send - WebSocket closed");
            if (callback) callback(false);
            return;
        }

        std::lock_guard<std::mutex> lock(_write_mutex);
        _write_queue.emplace_back(tag, json, callback);

        if (!_is_writing) process_write_queue();
    }

    void in_web_session::close()
    {
        if (_is_closing.exchange(true)) return;

        try
        {
            boost::beast::error_code ec;
            if (_ws && _ws->is_open())
            {
                _ws->close(boost::beast::websocket::close_code::normal, ec);
                if (ec)
                    logger::error("web_session",
                                  "WebSocket close error: " + ec.message());
            }
        }
        catch (const std::exception &e)
        {
            logger::error("web_session",
                          "Exception during close: " + str(e.what()));
        }

        std::function<void()> cb;

        {
            std::lock_guard<std::mutex> lock(_callback_mutex);
            cb = _close_callback;
        }

        if (cb)
        {
            logger::log_green("web_session", "close callback");
            cb();
        }
        else logger::error("web_session", "close callback null");
    }

    //Public:
    in_web_session::in_web_session(boost::asio::ip::tcp::socket *socket): _is_closing(false), _is_writing(false)
    {
        logger::log("web_session", "in_web_session()");

        _ws = new boost::beast::websocket::stream<boost::asio::ip::tcp::socket>(std::move(*socket));
        _event = new main_player::core::actions::hash_events_getter<u8, const str &>();
        _time_wait_ping = 0;
        _is_run = true;

        try
        {
            _ws->accept();
            _ws->binary(true);
            _ws->read_message_max(10 * 1024 * 1024);

            read_message();

            _event->add_listener(SESSION_PING, [this](const str &)
            {
                _time_wait_ping = 0;

                send(SESSION_PING, "");
            });
        } catch (const std::exception &e)
        {
            std::cerr << "WebSocket accept error: " << e.what() << std::endl;
            close();
        }
    }

    in_web_session::~in_web_session()
    {
        logger::log("web_session", "~in_web_session()");

        if (_ws)
        {
            try
            {
                boost::system::error_code ec_cancel;
                _ws->next_layer().cancel(ec_cancel);

                boost::beast::error_code ec;
                if (_ws->is_open())
                {
                    _ws->close(boost::beast::websocket::close_code::normal, ec);
                    if (ec) std::cerr << "WebSocket close error: " << ec.message() << std::endl;
                }
            } catch (const std::exception &e)
            {
                std::cerr << "Exception during WebSocket close: " << e.what() << std::endl;
            }

            delete _ws;
            _ws = nullptr;
        }

        delete _event;
    }

    void in_web_session::set_listener_close(const std::function<void()> &callback)
    {
        std::lock_guard<std::mutex> lock(_callback_mutex);
        _close_callback = callback;
    }

    void in_web_session::add_listener(const u8 &tag, std::function<void(const str &)> func)
    {
        _event->add_listener(tag, func);
    }

    void in_web_session::remove_listener(const u8 &tag)
    {
        _event->remove_listeners(tag);
    }

    void in_web_session::send(const u8 &tag, const str &json)
    {
        send_internal(tag, json, nullptr);
    }

    void in_web_session::send(const u8 &tag, const str &json, std::function<void(bool)> on_send)
    {
        send_internal(tag, json, on_send);
    }

    bool in_web_session::is_closed()
    {
        return !_is_run;
    }

    void in_web_session::tick(const float &delta)
    {
        if (!_is_run) return;

        _time_wait_ping += delta;

        if (_time_wait_ping > _ping_pong_timeout)
        {
            _is_run = false;

            close();
        }
    }
}
