#include <boost/beast/core/error.hpp>
#include <debug_system.h>

#include "port_acceptor.h"

namespace main_player::server::http
{
    using logger = main_player::core::debug::debug_system;
    using tcp = boost::asio::ip::tcp;

    void port_acceptor::do_accept()
    {
        auto socket = new tcp::socket(*_io_context);

        _acceptor->async_accept(*socket, [this, socket](boost::beast::error_code ec)
        {
            if (!ec) _on_connect(std::move(socket));
            else logger::error("port_acceptor", "accept error: " + ec.message());

            delete socket;
            do_accept();
        });
    }

    //Public:
    port_acceptor::port_acceptor(boost::asio::io_context *io_context, std::uint16_t port,
                                 const std::function<void(tcp::socket *)> &on_connect
    ): _port(port), _on_connect(std::move(on_connect))
    {
        _io_context = io_context;
        _acceptor = new tcp::acceptor(*io_context, tcp::endpoint(tcp::v4(), port));
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
