#ifndef IN_SESSION_WEB_H
#define IN_SESSION_WEB_H

#include <atomic>
#include <deque>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/websocket.hpp>
#include <sessions/i_session.h>
#include <returnable/hash_events_getter.h>

namespace main_player::logic::connection
{
	class in_web_session : public i_session
	{
	private:
		std::deque<std::tuple<std::uint8_t, std::string, std::function<void(bool)>>> _write_queue;
		main_player::core::actions::hash_events_getter<std::uint8_t, const std::string&>* _event;
		boost::beast::websocket::stream<boost::asio::ip::tcp::socket>* _ws;
		std::function<void()> _close_callback;
		std::mutex _callback_mutex;
		std::mutex _write_mutex;
		float _time_wait_ping;
		std::atomic<bool> _is_closing;
		bool _is_writing;
		std::atomic<bool> _is_run;

		void process_write_queue();

		void read_message();

		void send_internal(const std::uint8_t& tag, const std::string& json, std::function<void(bool)> callback);

		void close();

	public:
		in_web_session(const in_web_session&) = delete;
		in_web_session& operator=(const in_web_session&) = delete;
		in_web_session(in_web_session&&) = delete;
		in_web_session& operator=(in_web_session&&) = delete;

		explicit in_web_session(boost::asio::ip::tcp::socket* socket);

		~in_web_session() override;

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
