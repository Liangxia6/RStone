#include "rstone/dashboard/dashboard_server.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <sstream>

#include "rstone/dashboard/status_json.h"

namespace rstone {
namespace {

std::int64_t NowMs() {
  const auto now = std::chrono::system_clock::now().time_since_epoch();
  return std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
}

Status WriteAll(int fd, const std::string& payload) {
  const char* cursor = payload.data();
  std::size_t remaining = payload.size();
  while (remaining > 0) {
    const auto written = ::send(fd, cursor, remaining, 0);
    if (written <= 0) {
      return Status::IoError("http socket write failed");
    }
    cursor += written;
    remaining -= static_cast<std::size_t>(written);
  }
  return Status::Ok();
}

std::string ReadHttpRequest(int fd) {
  std::string request;
  char buffer[4096];
  const auto read = ::recv(fd, buffer, sizeof(buffer), 0);
  if (read > 0) {
    request.assign(buffer, buffer + read);
  }
  return request;
}

std::string ExtractPath(const std::string& request) {
  std::istringstream input(request);
  std::string method;
  std::string path;
  input >> method >> path;
  if (method != "GET" || path.empty()) {
    return "";
  }
  const auto query = path.find('?');
  if (query != std::string::npos) {
    path = path.substr(0, query);
  }
  return path;
}

}  // namespace

DashboardServer::DashboardServer(RpcGatewayClient* gateway) : gateway_(gateway) {}

DashboardServer::~DashboardServer() {
  Stop();
}

Status DashboardServer::Start(const std::string& host, int port) {
  if (gateway_ == nullptr) {
    return Status::InvalidArgument("dashboard gateway client must not be null");
  }
  listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd_ < 0) {
    return Status::IoError("failed to create dashboard socket");
  }

  int reuse = 1;
  ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(static_cast<std::uint16_t>(port));
  if (::inet_pton(AF_INET, host.c_str(), &address.sin_addr) != 1) {
    ::close(listen_fd_);
    listen_fd_ = -1;
    return Status::InvalidArgument("invalid dashboard host: " + host);
  }
  if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
    ::close(listen_fd_);
    listen_fd_ = -1;
    return Status::IoError("failed to bind dashboard server");
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
    return Status::IoError("failed to listen dashboard server");
  }

  running_ = true;
  accept_thread_ = std::thread([this] { AcceptLoop(); });
  return Status::Ok();
}

void DashboardServer::Stop() {
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

void DashboardServer::AcceptLoop() {
  while (running_) {
    const int client_fd = ::accept(listen_fd_, nullptr, nullptr);
    if (client_fd < 0) {
      continue;
    }
    std::thread([this, client_fd] { HandleClient(client_fd); }).detach();
  }
}

void DashboardServer::HandleClient(int client_fd) {
  const auto request = ReadHttpRequest(client_fd);
  const auto response = HandleRequest(request);
  (void)WriteAll(client_fd, response);
  ::close(client_fd);
}

std::string DashboardServer::HandleRequest(const std::string& request) {
  const auto path = ExtractPath(request);
  if (path == "/" || path == "/index.html") {
    return ServeFile("web/index.html", "text/html; charset=utf-8");
  }
  if (path == "/app.js") {
    return ServeFile("web/app.js", "application/javascript; charset=utf-8");
  }
  if (path == "/style.css") {
    return ServeFile("web/style.css", "text/css; charset=utf-8");
  }
  if (path == "/api/health") {
    std::ostringstream body;
    body << "{\"ok\":true,\"service\":\"rstone-dashboard\",\"timestamp_ms\":"
         << NowMs() << "}";
    return HttpResponse(200, "OK", "application/json; charset=utf-8", body.str());
  }
  if (path == "/api/status") {
    return BuildStatusResponse();
  }
  return HttpResponse(404, "Not Found", "text/plain; charset=utf-8", "not found\n");
}

std::string DashboardServer::BuildStatusResponse() {
  const auto start = NowMs();
  FieldMap fields;
  const auto status = gateway_->GetStatus(&fields);
  const auto end = NowMs();
  const auto body = BuildDashboardStatusJson(fields, status.ok(), status.message(),
                                             end - start, end);
  return HttpResponse(status.ok() ? 200 : 503, status.ok() ? "OK" : "Service Unavailable",
                      "application/json; charset=utf-8", body);
}

std::string DashboardServer::ServeFile(const std::string& path,
                                       const std::string& content_type) {
  std::ifstream input(std::filesystem::path(path), std::ios::binary);
  if (!input) {
    return HttpResponse(404, "Not Found", "text/plain; charset=utf-8",
                        "missing dashboard asset: " + path + "\n");
  }
  std::string body((std::istreambuf_iterator<char>(input)),
                   std::istreambuf_iterator<char>());
  return HttpResponse(200, "OK", content_type, body);
}

std::string DashboardServer::HttpResponse(int status_code, const std::string& status_text,
                                          const std::string& content_type,
                                          const std::string& body) {
  std::ostringstream output;
  output << "HTTP/1.1 " << status_code << " " << status_text << "\r\n";
  output << "Content-Type: " << content_type << "\r\n";
  output << "Content-Length: " << body.size() << "\r\n";
  output << "Cache-Control: no-store\r\n";
  output << "Connection: close\r\n";
  output << "\r\n";
  output << body;
  return output.str();
}

}  // namespace rstone
