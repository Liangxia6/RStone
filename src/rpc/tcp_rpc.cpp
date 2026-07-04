#include "rstone/rpc/tcp_rpc.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <vector>

#include "rstone/common/error_code.h"
#include "rstone/rpc/rpc_codec.h"

namespace rstone {
namespace {

Status WriteAll(int fd, const void* data, std::size_t size) {
  const char* cursor = static_cast<const char*>(data);
  while (size > 0) {
    const auto written = ::send(fd, cursor, size, 0);
    if (written <= 0) {
      return Status::IoError("socket write failed");
    }
    cursor += written;
    size -= static_cast<std::size_t>(written);
  }
  return Status::Ok();
}

Status ReadAll(int fd, void* data, std::size_t size) {
  char* cursor = static_cast<char*>(data);
  while (size > 0) {
    const auto read = ::recv(fd, cursor, size, 0);
    if (read <= 0) {
      return Status::IoError("socket read failed");
    }
    cursor += read;
    size -= static_cast<std::size_t>(read);
  }
  return Status::Ok();
}

Status WriteFrame(int fd, const std::string& payload) {
  const std::uint32_t length = htonl(static_cast<std::uint32_t>(payload.size()));
  auto status = WriteAll(fd, &length, sizeof(length));
  if (!status.ok()) {
    return status;
  }
  return WriteAll(fd, payload.data(), payload.size());
}

Status ReadFrame(int fd, std::string* payload) {
  std::uint32_t network_length = 0;
  auto status = ReadAll(fd, &network_length, sizeof(network_length));
  if (!status.ok()) {
    return status;
  }
  const auto length = ntohl(network_length);
  if (length > 64 * 1024 * 1024) {
    return Status::InvalidArgument("rpc frame is too large");
  }
  std::vector<char> buffer(length);
  status = ReadAll(fd, buffer.data(), buffer.size());
  if (!status.ok()) {
    return status;
  }
  payload->assign(buffer.begin(), buffer.end());
  return Status::Ok();
}

RpcResponse ErrorResponse(const RpcRequest& request, const Status& status) {
  return MakeRpcError(request, std::string(ErrorCodeName(status.code())), status.message());
}

}  // namespace

TcpRpcServer::TcpRpcServer(std::shared_ptr<RpcServer> server) : server_(std::move(server)) {}

TcpRpcServer::~TcpRpcServer() {
  Stop();
}

Status TcpRpcServer::Start(const std::string& host, int port) {
  if (!server_) {
    return Status::InvalidArgument("rpc server must not be null");
  }
  listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd_ < 0) {
    return Status::IoError("failed to create socket");
  }

  int reuse = 1;
  ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(static_cast<std::uint16_t>(port));
  if (::inet_pton(AF_INET, host.c_str(), &address.sin_addr) != 1) {
    ::close(listen_fd_);
    listen_fd_ = -1;
    return Status::InvalidArgument("invalid tcp host: " + host);
  }

  if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
    ::close(listen_fd_);
    listen_fd_ = -1;
    return Status::IoError("failed to bind tcp rpc server");
  }

  socklen_t address_len = sizeof(address);
  if (::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&address), &address_len) == 0) {
    bound_port_ = ntohs(address.sin_port);
  } else {
    bound_port_ = port;
  }

  if (::listen(listen_fd_, 16) != 0) {
    ::close(listen_fd_);
    listen_fd_ = -1;
    return Status::IoError("failed to listen tcp rpc server");
  }

  running_ = true;
  accept_thread_ = std::thread([this] { AcceptLoop(); });
  return Status::Ok();
}

void TcpRpcServer::Stop() {
  if (!running_.exchange(false)) {
    return;
  }
  if (listen_fd_ >= 0) {
    ::shutdown(listen_fd_, SHUT_RDWR);
    ::close(listen_fd_);
    listen_fd_ = -1;
  }
  if (accept_thread_.joinable()) {
    accept_thread_.join();
  }
}

void TcpRpcServer::AcceptLoop() {
  while (running_) {
    const int client_fd = ::accept(listen_fd_, nullptr, nullptr);
    if (client_fd < 0) {
      continue;
    }
    std::thread([this, client_fd] { HandleClient(client_fd); }).detach();
  }
}

void TcpRpcServer::HandleClient(int client_fd) {
  std::string encoded_request;
  RpcRequest request;
  auto status = ReadFrame(client_fd, &encoded_request);
  RpcResponse response;
  if (status.ok()) {
    status = DecodeRpcRequest(encoded_request, &request);
  }
  if (status.ok()) {
    response = server_->Handle(request);
  } else {
    response = ErrorResponse(request, status);
  }
  (void)WriteFrame(client_fd, EncodeRpcResponse(response));
  ::close(client_fd);
}

TcpRpcClient::TcpRpcClient(std::string host, int port)
    : host_(std::move(host)), port_(port) {}

RpcResponse TcpRpcClient::Call(const RpcRequest& request) {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return MakeRpcError(request, std::string(ErrorCodeName(ErrorCode::kRpcError)),
                        "failed to create socket");
  }

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(static_cast<std::uint16_t>(port_));
  if (::inet_pton(AF_INET, host_.c_str(), &address.sin_addr) != 1) {
    ::close(fd);
    return MakeRpcError(request, std::string(ErrorCodeName(ErrorCode::kInvalidArgument)),
                        "invalid tcp host");
  }
  if (::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
    ::close(fd);
    return MakeRpcError(request, std::string(ErrorCodeName(ErrorCode::kRpcError)),
                        "failed to connect tcp rpc server");
  }

  auto status = WriteFrame(fd, EncodeRpcRequest(request));
  if (!status.ok()) {
    ::close(fd);
    return ErrorResponse(request, status);
  }

  std::string encoded_response;
  status = ReadFrame(fd, &encoded_response);
  ::close(fd);
  if (!status.ok()) {
    return ErrorResponse(request, status);
  }

  RpcResponse response;
  status = DecodeRpcResponse(encoded_response, &response);
  if (!status.ok()) {
    return ErrorResponse(request, status);
  }
  return response;
}

}  // namespace rstone
