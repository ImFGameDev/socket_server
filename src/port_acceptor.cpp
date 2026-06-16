#include <boost/beast/core/error.hpp>
#include <debug_system.h>

#include "port_acceptor.h"

namespace main_player::server::http
{
    void port_acceptor::do_accept()
    {
        auto socket = new boost::asio::ip::tcp::socket(*_io_context);

        _acceptor->async_accept(*socket, [this, socket](boost::beast::error_code ec)
        {
            if (!ec) _on_connect(std::move(socket));
            else main_player::core::debug::debug_system::error("port_acceptor", "accept error: " + ec.message());

            delete socket;
            do_accept();
        });
    }

    //Public:
    port_acceptor::port_acceptor(boost::asio::io_context *io_context, std::uint16_t port,
                                 const std::function<void(boost::asio::ip::tcp::socket *)> &on_connect
    ): _port(port), _on_connect(std::move(on_connect))
    {
        _io_context = io_context;
        _acceptor = new boost::asio::ip::tcp::acceptor(*io_context,
                                                       boost::asio::ip::tcp::endpoint(
                                                           boost::asio::ip::tcp::v4(), port));
    }

    port_acceptor::~port_acceptor()
    {
        if (_acceptor)
        {
            _acceptor->close();
            delete _acceptor;
        }
    }

    void port_acceptor::start()
    {
        do_accept();
    }
}
