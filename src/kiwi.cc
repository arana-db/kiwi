// Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory

/*
  Stub main() routine for the kiwi executable.

  This does some essential startup tasks for kiwi, and then dispatches to the proper FooMain() routine for the
  incarnation.
 */

#include "kiwi.h"

#include <getopt.h>
#include <sys/fcntl.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <thread>

#include "praft/praft.h"
#include "pstd/log.h"
#include "pstd/pstd_util.h"

#include "client.h"
#include "config.h"
#include "helper.h"
#include "kiwi.h"
#include "kiwi_logo.h"
#include "slow_log.h"
#include "store.h"

// g_kiwi is a global abstraction of the server-side process
std::unique_ptr<KiwiDB> g_kiwi;

using namespace kiwi;

/*
 * set up a handler to be called if the kiwi crashes
 * with a fatal signal or exception.
 */
static void IntSigHandle(const int sig) {
  INFO("Catch Signal {}, cleanup...", sig);
  g_kiwi->Stop();
}

static void SignalSetup() {
  signal(SIGHUP, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, &IntSigHandle);
  signal(SIGQUIT, &IntSigHandle);
  signal(SIGTERM, &IntSigHandle);
}

const uint32_t KiwiDB::kRunidSize = 40;

static void Usage() {
  std::cerr << "kiwi is the kiwi server.\n";
  std::cerr << "\n";
  std::cerr << "Usage:\n";
  std::cerr << "  kiwi [/path/to/kiwi.conf] [options]\n";
  std::cerr << "\n";
  std::cerr << "Options:\n";
  std::cerr << "  -v, --version                   output version information, then exit\n";
  std::cerr << "  -h, --help                      output help message\n";
  std::cerr << "  -p PORT, --port PORT            Set the port listen on\n";
  std::cerr << "  -l LEVEL, --loglevel LEVEL      Set the log level\n";
  std::cerr << "  -s ADDRESS, --slaveof ADDRESS   Set the slave address\n";
  std::cerr << "  -c, --redis-compatible-mode     Enable Redis compatibility mode\n";
  std::cerr << "Examples:\n";
  std::cerr << "  kiwi /path/kiwi.conf\n";
  std::cerr << "  kiwi /path/kiwi.conf --loglevel verbose\n";
  std::cerr << "  kiwi --port 7777\n";
  std::cerr << "  kiwi --port 7777 --slaveof 127.0.0.1:8888\n";
}

// Handle the argc & argv
bool KiwiDB::ParseArgs(int argc, char* argv[]) {
  static struct option long_options[] = {
      {"version", no_argument, 0, 'v'},       {"help", no_argument, 0, 'h'},
      {"port", required_argument, 0, 'p'},    {"loglevel", required_argument, 0, 'l'},
      {"slaveof", required_argument, 0, 's'}, {"redis-compatible-mode", no_argument, 0, 'c'},
  };
  // kiwi [/path/to/kiwi.conf] [options]
  if (cfg_file_.empty() && argc > 1 && ::access(argv[1], R_OK) == 0) {
    cfg_file_ = argv[1];
    argc = argc - 1;
    argv = argv + 1;
  }
  while (1) {
    int this_option_optind = optind ? optind : 1;
    int option_index = 0;
    int c;
    c = getopt_long(argc, argv, "vhp:l:s:c", long_options, &option_index);
    if (c == -1) {
      break;
    }

    switch (c) {
      case 'v': {
        std::cerr << "kiwi Server version: " << Kkiwi_VERSION << " bits=" << (sizeof(void*) == 8 ? 64 : 32)
                  << std::endl;
        std::cerr << "kiwi Server Build Type: " << Kkiwi_BUILD_TYPE << std::endl;
#if defined(Kkiwi_BUILD_DATE)
        std::cerr << "kiwi Server Build Date: " << Kkiwi_BUILD_DATE << std::endl;
#endif
#if defined(Kkiwi_GIT_COMMIT_ID)
        std::cerr << "kiwi Server Build GIT SHA: " << Kkiwi_GIT_COMMIT_ID << std::endl;
#endif

        std::exit(0);
        break;
      }
      case 'h': {
        Usage();
        exit(0);
        break;
      }
      case 'p': {
        port_ = static_cast<uint16_t>(std::atoi(optarg));
        break;
      }
      case 'l': {
        log_level_ = std::string(optarg);
        break;
      }
      case 's': {
        unsigned int optarg_long = static_cast<unsigned int>(strlen(optarg));
        char* str = (char*)calloc(optarg_long, sizeof(char*));
        if (str) {
          if (sscanf(optarg, "%s:%d", str, &master_port_) != 2) {
            ERROR("Invalid slaveof format.");
            free(str);
            return false;
          }
          master_ = str;
          free(str);
        } else {
          ERROR("Memory alloc failed.");
        }
        break;
      }
      case 'c': {
        redis_compatible_mode = true;
        break;
      }
      case '?': {
        std::cerr << "Unknow option " << std::endl;
        return false;
        break;
      }
    }
  }
  return true;
}

void KiwiDB::OnNewConnection(uint64_t connId, std::shared_ptr<kiwi::PClient>& client, const net::SocketAddr& addr) {
  INFO("New connection from {}:{}", addr.GetIP(), addr.GetPort());
  client->SetSocketAddr(addr);
  client->OnConnect();
}

bool KiwiDB::Init() {
  char runid[kRunidSize + 1] = "";
  getRandomHexChars(runid, kRunidSize);
  g_config.Set("runid", {runid, kRunidSize}, true);

  if (port_ != 0) {
    g_config.Set("port", std::to_string(port_), true);
  }

  if (!log_level_.empty()) {
    g_config.Set("log-level", log_level_, true);
  }

  if (redis_compatible_mode) {
    g_config.Set("redis_compatible_mode", std::to_string(redis_compatible_mode), true);
  }

  auto num = g_config.worker_threads_num.load() + g_config.slave_threads_num.load();

  // now we only use fast cmd thread pool
  auto status = cmd_threads_.Init(g_config.fast_cmd_threads_num.load(), 0, "kiwi-cmd");
  if (!status.ok()) {
    ERROR("init cmd thread pool failed: {}", status.ToString());
    return false;
  }

  PSTORE.Init(g_config.databases.load(std::memory_order_relaxed));

  PSlowLog::Instance().SetThreshold(g_config.slow_log_time.load());
  PSlowLog::Instance().SetLogLimit(static_cast<std::size_t>(g_config.slow_log_max_len.load()));

  // master ip
  if (!g_config.master_ip.empty()) {
    PREPL.SetMasterAddr(g_config.master_ip.ToString().c_str(), g_config.master_port.load());
  }

  event_server_ =std::make_unique<net::EventServer<std::shared_ptr<PClient>>>(num);

  event_server_->SetRwSeparation(true);

  net::SocketAddr addr(g_config.ip.ToString(), g_config.port.load());
  INFO("Add listen addr:{}, port:{}", g_config.ip.ToString(), g_config.port.load());
  event_server_->AddListenAddr(addr);

  event_server_->SetOnInit([](std::shared_ptr<PClient>* client) { *client = std::make_shared<PClient>(); });

  event_server_->SetOnCreate([](uint64_t connID, std::shared_ptr<PClient>& client, const net::SocketAddr& addr) {
    client->SetSocketAddr(addr);
    client->OnConnect();
    INFO("New connection from fd:{} IP:{} port:{}", connID, addr.GetIP(), addr.GetPort());
  });

  event_server_->SetOnMessage([](std::string&& msg, std::shared_ptr<PClient>& t) {
    t->handlePacket(msg.c_str(), static_cast<int>(msg.size()));
  });

  event_server_->SetOnClose([](std::shared_ptr<PClient>& client, std::string&& msg) {
    INFO("Close connection id:{} msg:{}", client->GetConnId(), msg);
    client->OnClose();
  });

  event_server_->InitTimer(10);

  auto timerTask = std::make_shared<net::CommonTimerTask>(1000);
  timerTask->SetCallback([]() { PREPL.Cron(); });
  event_server_->AddTimerTask(timerTask);

  time(&start_time_s_);

  return true;
}

void KiwiDB::Run() {
  auto [ret, err] = event_server_->StartServer();
  if (!ret) {
    ERROR("start server failed: {}", err);
    return;
  }

  cmd_threads_.Start();
  event_server_->Wait();
  INFO("server exit running");
}

void KiwiDB::Stop() {
  kiwi::PRAFT.ShutDown();
  kiwi::PRAFT.Join();
  kiwi::PRAFT.Clear();
  cmd_threads_.Stop();
  event_server_->StopServer();
}

void KiwiDB::TCPConnect(
    const net::SocketAddr& addr,
    const std::function<void(uint64_t, std::shared_ptr<kiwi::PClient>&, const net::SocketAddr&)>& onConnect,
    const std::function<void(std::string)>& cb) {
  INFO("Connect to {}:{}", addr.GetIP(), addr.GetPort());
  event_server_->TCPConnect(addr, onConnect, cb);
}

static void InitLogs() {
  logger::Init("logs/kiwi_server.log");

#if BUILD_DEBUG
  spdlog::set_level(spdlog::level::debug);
#else
  spdlog::set_level(spdlog::level::info);
#endif
}

static int InitLimit() {
  rlimit limit;
  rlim_t maxfiles = g_config.max_clients;
  if (getrlimit(RLIMIT_NOFILE, &limit) == -1) {
    WARN("getrlimit error: {}", strerror(errno));
  } else if (limit.rlim_cur < maxfiles) {
    rlim_t old_limit = limit.rlim_cur;
    limit.rlim_cur = maxfiles;
    limit.rlim_max = maxfiles;
    if (setrlimit(RLIMIT_NOFILE, &limit) != -1) {
      WARN("your 'limit -n' of {} is not enough for kiwi to start. kiwi has successfully reconfig it to {}", old_limit,
           limit.rlim_cur);
    } else {
      ERROR(
          "your 'limit -n ' of {} is not enough for kiwi to start."
          " kiwi can not reconfig it({}), do it by yourself",
          old_limit, strerror(errno));
      return -1;
    }
  }

  return 0;
}

static void daemonize() {
  if (fork()) {
    // parent exits
    exit(0);
  }
  // create a new session
  setsid();
}

static void closeStd() {
  int fd;
  fd = open("/dev/null", O_RDWR, 0);
  if (fd != -1) {
    dup2(fd, STDIN_FILENO);
    dup2(fd, STDOUT_FILENO);
    dup2(fd, STDERR_FILENO);
    close(fd);
  }
}

// Any kiwi server process begins execution here.
int main(int argc, char* argv[]) {
  g_kiwi = std::make_unique<KiwiDB>();
  if (!g_kiwi->ParseArgs(argc, argv)) {
    Usage();
    return -1;
  }

  if (!g_kiwi->GetConfigName().empty()) {
    if (!g_config.LoadFromFile(g_kiwi->GetConfigName())) {
      std::cerr << "Load config file [" << g_kiwi->GetConfigName() << "] failed!\n";
      return -1;
    }
  }

  if (g_config.daemonize.load()) {
    daemonize();
  }

  pstd::InitRandom();
  SignalSetup();
  InitLogs();
  InitLimit();

  if (g_config.daemonize.load()) {
    closeStd();
  }

  if (g_kiwi->Init()) {
    // output logo to console
    char logo[1024] = "";
    snprintf(logo, sizeof logo - 1, kiwiLogo, Kkiwi_VERSION, static_cast<int>(sizeof(void*)) * 8,
             static_cast<int>(g_config.port));
    std::cout << logo;
    g_kiwi->Run();
  }

  // When kiwi exit, flush log
  spdlog::get(logger::Logger::Instance().Name())->flush();
  return 0;
}
