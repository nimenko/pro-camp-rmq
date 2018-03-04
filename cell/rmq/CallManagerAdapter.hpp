#pragma once
#include <map>
#include <string>
#include <chrono>
#include "../interfaces.hpp"

#include <amqpcpp.h>
#include <amqpcpp/libboostasio.h>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/lexical_cast.hpp>

namespace rmq
{

class CallManagerAdapter
{
public:
    explicit CallManagerAdapter(const std::string& address, ICallManager& impl, boost::asio::io_service& svc) :
        handler_(svc),
        connection_(&handler_, AMQP::Address(address)),
        channel_(&connection_),
        impl_(impl),
        svc_(svc)
    {

    }

    virtual void updateBalance(const std::string& phone, call_duration left)
    {
        svc_.post([this, phone, left]()
        {
            impl_.updateBalance(phone, left);
        });
    }

    virtual void pulse()
    {
        svc_.post([this]()
        {
            impl_.pulse();
        });
    }

    void activeRMQ()
    {
        channel_.declareExchange("caller", AMQP::topic).onSuccess([](){
            std::cout << "declared exchange " << std::endl;
        });

        channel_.declareQueue("caller").onSuccess([](const std::string &name, uint32_t messagecount, uint32_t consumercount) {
            std::cout << "declared queue " << name << std::endl;
        });

        channel_.bindQueue("caller", "caller", "caller" ).onSuccess([](){
            std::cout << "binded to queue " << std::endl;
        });

        channel_.consume("caller", AMQP::noack).onReceived(
                    [this](const AMQP::Message &message, uint64_t deliveryTag, bool redelivered) {
            svc_.post([this, &message]() {
                handleMessage_(std::string(message.body(), message.body() + message.bodySize()));
            });
        });
    }

private:
    void handleMessage_(const std::string& message)
    {
        //std::cout << "Got message: " << message << std::endl;
        std::vector<std::string> words;
        boost::split(words, message, boost::is_any_of(":"), boost::token_compress_on);

        if (words.empty()) return;
        if (words.front() == "pulse" && words.size() == 1)
        {
            pulse();
        }
        if (words.front() == "updateBalance" && words.size() == 3)
        {
            updateBalance(words[1], std::chrono::seconds(boost::lexical_cast<int>(words[2])));
        }
    }

    AMQP::LibBoostAsioHandler handler_;
    AMQP::TcpConnection connection_;
    AMQP::TcpChannel channel_;

    ICallManager& impl_;
    boost::asio::io_service& svc_;
};

}
