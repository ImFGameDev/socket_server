#ifndef SESSION_POST_H
#define SESSION_POST_H

#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http.hpp>
#include <debug_system.h>

namespace main_player::server::http
{
	class session_post
	{
	private:
		boost::asio::ip::tcp::socket* _socket;
		boost::beast::flat_buffer* _buffer;
		boost::beast::http::request<boost::beast::http::string_body>* _req;
		boost::beast::http::response<boost::beast::http::string_body>* _res;
		std::function<std::string(const std::string&)> _on_request;
		bool _is_reading;

		void cleanup();

		void read_request();

		void process_request();

		boost::beast::http::response<boost::beast::http::string_body> create_response(
			boost::beast::http::status status, const std::string& body);

		void send_response(boost::beast::http::response<boost::beast::http::string_body>&& response);

	public:
		session_post(boost::asio::ip::tcp::socket* socket, std::function<std::string(const std::string&)> on_request);

		~session_post();
	};
}

#endif
