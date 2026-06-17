#ifndef IN_SESSION_TCP_H
#define IN_SESSION_TCP_H

#include <boost/asio/ip/tcp.hpp>
#include <returnable/hash_events_getter.h>
#include <atomic>
#include <tuple>
#include <vector>
#include "../i_session.h"

namespace main_player::logic::connection
{
	class in_session_tcp : public i_session
	{
	private:
		main_player::core::actions::hash_events_getter<std::uint8_t, const std::string&>* _event;
		boost::asio::ip::tcp::socket* _socket;
		std::mutex _socket_mutex;
		std::atomic<bool> _is_run;
		std::atomic<bool> _is_closing;
		std::atomic<float> _time_wait_ping;
		char* _data_tag;
		char* _data;
		std::vector<std::tuple<std::uint8_t, std::string, std::function<void(boost::system::error_code, std::size_t)>>> _write_queue;
		bool _is_writing = false;

		std::function<void()> _close_callback;

		void read_length();

		void read_data(std::uint32_t length);

		void send_internal(const std::uint8_t& tag, const std::string& json, const std::function<void(boost::system::error_code, std::size_t)>& callback);

		void process_write_queue();

		void close();

	public:
		in_session_tcp(boost::asio::ip::tcp::socket* socket);

		~in_session_tcp() override;

		void send(const std::uint8_t& tag, const std::string& json) override;

		void send(const std::uint8_t& tag, const std::string& json, std::function<void(bool)> on_send) override;

		void set_listener_close(const std::function<void()>& callback) override;

		void add_listener(const std::uint8_t& tag, std::function<void(const std::string&)> func) override;

		void remove_listener(const std::uint8_t& tag) override;

		void tick(const float& delta) override;

		bool is_closed() override;
	};
}

#endif
