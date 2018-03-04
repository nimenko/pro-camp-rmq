#pragma once
#include <map>
#include <string>
#include <chrono>
#include "../interfaces.hpp"

#include <amqpcpp.h>
#include <amqpcpp/libboostasio.h>
#include <boost/lexical_cast.hpp>

namespace rmq
{

class CallManagerProxy : public ICallManager
{
public:
    explicit CallManagerProxy(const std::string& address, boost::asio::io_service& svc):
        handler_(svc),
        connection_(&handler_, AMQP::Address(address)),
        channel_(&connection_),
        svc_(svc)
    {

    }

    virtual void updateBalance(const std::string& phone,  call_duration left)
    {
        channel_.publish("caller", "caller", "updateBalance:" + phone + ":" +
                         boost::lexical_cast<std::string>(std::chrono::duration_cast<std::chrono::seconds>(left).count()));
    }

    virtual void pulse()
    {
        channel_.publish("caller", "caller", "pulse");
    }

private:
    virtual void handleIncommingCall(const std::string& phone)
    {

    }

    virtual void handleCallDisconnection(const std::string& phone)
    {

    }

    AMQP::LibBoostAsioHandler handler_;
    AMQP::TcpConnection connection_;
    AMQP::TcpChannel channel_;
    boost::asio::io_service& svc_;
};

}
