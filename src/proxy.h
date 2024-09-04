#pragma once

#include <memory>
#include "common.h"
#include "net/http_server.h"
#include "net/tcp_connection.h"

namespace pikiwidb {
  
class PProxy final {
public:
  PProxy() = default;
  ~PProxy() = default;

  PProxy& Instance();
  
  bool ParseArgs(int ac, char *av[]);
  const PString& GetConfigName() const { return cfg_file_; }
  
  bool Init();
  void Run();
  
  void Stop();
  
  void OnNewConnection(TcpConnection* obj);

public:
  PString cfg_file_;
  uint16_t port_{0};
  PString log_level_;
  
  PString master_;
  uint16_t master_port_{0};
  
  static const uint32_t kRunidSize;

private:
  uint32_t cmd_id_ = 0;
  
};
}