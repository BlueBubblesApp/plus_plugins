// clang-format off
#include "include/connectivity_plus/network_manager.h"
// clang-format on
#include "include/connectivity_plus/connectivity_plus_windows_plugin.h"

#include <flutter/event_channel.h>
#include <flutter/event_sink.h>
#include <flutter/event_stream_handler.h>
#include <flutter/method_channel.h>
#include <flutter/plugin_registrar_windows.h>
#include <flutter/standard_method_codec.h>

#include <algorithm>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <thread>
#include <utility>
#include <vector>

namespace {

typedef flutter::EventChannel<flutter::EncodableValue> FlEventChannel;
typedef flutter::EventSink<flutter::EncodableValue> FlEventSink;
typedef flutter::MethodCall<flutter::EncodableValue> FlMethodCall;
typedef flutter::MethodResult<flutter::EncodableValue> FlMethodResult;
typedef flutter::MethodChannel<flutter::EncodableValue> FlMethodChannel;
typedef flutter::StreamHandler<flutter::EncodableValue> FlStreamHandler;
typedef flutter::StreamHandlerError<flutter::EncodableValue>
    FlStreamHandlerError;

// Runs connectivity queries off the platform thread. GetConnectivityTypes()
// makes cross-process Network List Manager calls that can take seconds while
// the network is changing (resume from sleep, VPN up/down), and the platform
// thread also pumps the window's input, so querying there freezes typing.
// Results are delivered back on the platform thread.
class ConnectivityQuery {
public:
  typedef std::function<void(const std::set<ConnectivityType> &)> Callback;

  ConnectivityQuery();
  ~ConnectivityQuery();

  // Platform thread only. The callback receives the result of a query that
  // started after this call, so a burst of requests collapses into a few
  // queries without ever reporting state older than the request.
  void Request(Callback callback);

private:
  static constexpr UINT kResultMessage = WM_APP + 1;

  // Shared with the detached worker, which may outlive this object if a
  // query is still blocked when the plugin is destroyed.
  struct Shared {
    std::mutex mutex;
    std::condition_variable cv;
    uint64_t requested = 0; // newest generation asked for
    uint64_t done = 0;      // generation that produced `result`
    std::set<ConnectivityType> result;
    bool stop = false;
    HWND window = nullptr;
  };

  static void Run(std::shared_ptr<Shared> shared);
  static LRESULT CALLBACK WndProc(HWND hwnd, UINT message, WPARAM wparam,
                                  LPARAM lparam);
  void Deliver();

  std::shared_ptr<Shared> shared_ = std::make_shared<Shared>();
  std::vector<std::pair<uint64_t, Callback>> waiting_;
};

ConnectivityQuery::ConnectivityQuery() {
  static const wchar_t kClassName[] = L"ConnectivityPlusQueryWindow";
  static bool registered = false;
  if (!registered) {
    WNDCLASSEXW wc = {sizeof(wc)};
    wc.lpfnWndProc = WndProc;
    wc.hInstance = GetModuleHandle(nullptr);
    wc.lpszClassName = kClassName;
    registered = RegisterClassExW(&wc) != 0;
  }
  shared_->window = CreateWindowExW(0, kClassName, L"", 0, 0, 0, 0, 0,
                                    HWND_MESSAGE, nullptr,
                                    GetModuleHandle(nullptr), nullptr);
  SetWindowLongPtr(shared_->window, GWLP_USERDATA,
                   reinterpret_cast<LONG_PTR>(this));
  std::thread(&ConnectivityQuery::Run, shared_).detach();
}

ConnectivityQuery::~ConnectivityQuery() {
  {
    std::lock_guard<std::mutex> lock(shared_->mutex);
    shared_->stop = true;
  }
  shared_->cv.notify_one();
  // The worker only posts while holding the lock and !stop, so nothing
  // reaches the window after this.
  DestroyWindow(shared_->window);
}

void ConnectivityQuery::Request(Callback callback) {
  std::lock_guard<std::mutex> lock(shared_->mutex);
  waiting_.emplace_back(++shared_->requested, std::move(callback));
  shared_->cv.notify_one();
}

void ConnectivityQuery::Run(std::shared_ptr<Shared> shared) {
  // Its own instance: COM objects are bound to the apartment that made them.
  NetworkManager manager;
  const bool ready = manager.Init();

  std::unique_lock<std::mutex> lock(shared->mutex);
  uint64_t started = 0;
  while (true) {
    shared->cv.wait(lock, [&] {
      return shared->stop || shared->requested > started;
    });
    if (shared->stop) {
      break;
    }
    started = shared->requested;

    lock.unlock();
    std::set<ConnectivityType> types =
        ready ? manager.GetConnectivityTypes()
              : std::set<ConnectivityType>{ConnectivityType::None};
    lock.lock();

    if (shared->stop) {
      break;
    }
    shared->result = std::move(types);
    shared->done = started;
    PostMessage(shared->window, kResultMessage, 0, 0);
  }
}

LRESULT CALLBACK ConnectivityQuery::WndProc(HWND hwnd, UINT message,
                                            WPARAM wparam, LPARAM lparam) {
  if (message == kResultMessage) {
    auto *self = reinterpret_cast<ConnectivityQuery *>(
        GetWindowLongPtr(hwnd, GWLP_USERDATA));
    if (self) {
      self->Deliver();
    }
    return 0;
  }
  return DefWindowProc(hwnd, message, wparam, lparam);
}

void ConnectivityQuery::Deliver() {
  uint64_t done;
  std::set<ConnectivityType> types;
  std::vector<Callback> ready;
  {
    std::lock_guard<std::mutex> lock(shared_->mutex);
    done = shared_->done;
    types = shared_->result;
    for (auto it = waiting_.begin(); it != waiting_.end();) {
      if (it->first <= done) {
        ready.push_back(std::move(it->second));
        it = waiting_.erase(it);
      } else {
        ++it;
      }
    }
  }
  // Outside the lock: a callback may Request() again.
  for (auto &callback : ready) {
    callback(types);
  }
}

class ConnectivityPlusWindowsPlugin : public flutter::Plugin {
public:
  ConnectivityPlusWindowsPlugin();
  virtual ~ConnectivityPlusWindowsPlugin();

  std::shared_ptr<NetworkManager> GetManager() const;
  std::shared_ptr<ConnectivityQuery> GetQuery() const;

  static void RegisterWithRegistrar(flutter::PluginRegistrarWindows *registrar);

private:
  void HandleMethodCall(const FlMethodCall &method_call,
                        std::unique_ptr<FlMethodResult> result);

  std::shared_ptr<NetworkManager> manager;
  std::shared_ptr<ConnectivityQuery> query;
};

class ConnectivityStreamHandler : public FlStreamHandler {
public:
  ConnectivityStreamHandler(std::shared_ptr<NetworkManager> manager,
                            std::shared_ptr<ConnectivityQuery> query);
  virtual ~ConnectivityStreamHandler();

protected:
  void AddConnectivityEvent();

  std::unique_ptr<FlStreamHandlerError>
  OnListenInternal(const flutter::EncodableValue *arguments,
                   std::unique_ptr<FlEventSink> &&sink) override;

  std::unique_ptr<FlStreamHandlerError>
  OnCancelInternal(const flutter::EncodableValue *arguments) override;

private:
  bool TryStartListen();
  void ScheduleStartListenRetry();
  void CancelStartListenRetry();
  static void CALLBACK RetryTimerProc(HWND hwnd, UINT message, UINT_PTR id,
                                      DWORD time);

  // Thread-affine (platform thread), like the stream handler itself.
  static std::map<UINT_PTR, ConnectivityStreamHandler *> retry_handlers_;

  std::shared_ptr<NetworkManager> manager;
  std::shared_ptr<ConnectivityQuery> query;
  // Lets pending query callbacks detect that this handler is gone.
  std::shared_ptr<bool> alive_ = std::make_shared<bool>(true);
  std::unique_ptr<FlEventSink> sink;
  UINT_PTR retry_timer_id_ = 0;
  int retry_attempts_ = 0;
};

std::map<UINT_PTR, ConnectivityStreamHandler *>
    ConnectivityStreamHandler::retry_handlers_;

ConnectivityPlusWindowsPlugin::ConnectivityPlusWindowsPlugin() {
  manager = std::make_shared<NetworkManager>();
  manager->Init();
  query = std::make_shared<ConnectivityQuery>();
}

ConnectivityPlusWindowsPlugin::~ConnectivityPlusWindowsPlugin() {
  manager->Cleanup();
}

std::shared_ptr<NetworkManager>
ConnectivityPlusWindowsPlugin::GetManager() const {
  return manager;
}

std::shared_ptr<ConnectivityQuery>
ConnectivityPlusWindowsPlugin::GetQuery() const {
  return query;
}

void ConnectivityPlusWindowsPlugin::RegisterWithRegistrar(
    flutter::PluginRegistrarWindows *registrar) {
  auto plugin = std::make_unique<ConnectivityPlusWindowsPlugin>();

  auto methodChannel =
      std::make_unique<flutter::MethodChannel<flutter::EncodableValue>>(
          registrar->messenger(), "dev.fluttercommunity.plus/connectivity",
          &flutter::StandardMethodCodec::GetInstance());

  methodChannel->SetMethodCallHandler(
      [plugin_pointer = plugin.get()](const auto &call, auto result) {
        plugin_pointer->HandleMethodCall(call, std::move(result));
      });

  auto eventChannel = std::make_unique<FlEventChannel>(
      registrar->messenger(), "dev.fluttercommunity.plus/connectivity_status",
      &flutter::StandardMethodCodec::GetInstance());

  eventChannel->SetStreamHandler(
      std::make_unique<ConnectivityStreamHandler>(plugin->GetManager(),
                                                  plugin->GetQuery()));

  registrar->AddPlugin(std::move(plugin));
}

static std::string ConnectivityToString(ConnectivityType connectivityType) {
  switch (connectivityType) {
  case ConnectivityType::WiFi:
    return "wifi";
  case ConnectivityType::Ethernet:
    return "ethernet";
  case ConnectivityType::VPN:
    return "vpn";
  case ConnectivityType::Other:
    return "other";
  case ConnectivityType::None:
  default:
    return "none";
  }
}

static flutter::EncodableList
EncodeConnectivityTypes(std::set<ConnectivityType> connectivityTypes) {
  flutter::EncodableList encodedList;

  if (connectivityTypes.empty()) {
    encodedList.push_back(
        flutter::EncodableValue(ConnectivityToString(ConnectivityType::None)));
    return encodedList;
  }

  for (const auto &type : connectivityTypes) {
    std::string connectivityString = ConnectivityToString(type);
    encodedList.push_back(flutter::EncodableValue(connectivityString));
  }

  return encodedList;
}

void ConnectivityPlusWindowsPlugin::HandleMethodCall(
    const flutter::MethodCall<flutter::EncodableValue> &method_call,
    std::unique_ptr<flutter::MethodResult<flutter::EncodableValue>> result) {
  if (method_call.method_name().compare("check") == 0) {
    std::shared_ptr<FlMethodResult> pending = std::move(result);
    query->Request([pending](const std::set<ConnectivityType> &types) {
      pending->Success(EncodeConnectivityTypes(types));
    });
  } else {
    result->NotImplemented();
  }
}

ConnectivityStreamHandler::ConnectivityStreamHandler(
    std::shared_ptr<NetworkManager> manager,
    std::shared_ptr<ConnectivityQuery> query)
    : manager(manager), query(query) {}

ConnectivityStreamHandler::~ConnectivityStreamHandler() {
  CancelStartListenRetry();
}

void ConnectivityStreamHandler::AddConnectivityEvent() {
  std::weak_ptr<bool> alive = alive_;
  query->Request([this, alive](const std::set<ConnectivityType> &types) {
    if (alive.expired() || !sink) {
      return; // Handler destroyed or stream cancelled meanwhile.
    }
    sink->Success(EncodeConnectivityTypes(types));
  });
}

bool ConnectivityStreamHandler::TryStartListen() {
  auto callback =
      std::bind(&ConnectivityStreamHandler::AddConnectivityEvent, this);
  return manager->StartListen(callback);
}

void ConnectivityStreamHandler::ScheduleStartListenRetry() {
  // 100ms, 200ms, 400ms, ... — enough to escape whatever input-synchronous
  // window the platform thread was inside.
  UINT delay = 100u << (std::min)(retry_attempts_, 4);
  retry_timer_id_ = SetTimer(nullptr, 0, delay, RetryTimerProc);
  if (retry_timer_id_ != 0) {
    retry_handlers_[retry_timer_id_] = this;
  }
}

void ConnectivityStreamHandler::CancelStartListenRetry() {
  if (retry_timer_id_ != 0) {
    KillTimer(nullptr, retry_timer_id_);
    retry_handlers_.erase(retry_timer_id_);
    retry_timer_id_ = 0;
  }
}

void CALLBACK ConnectivityStreamHandler::RetryTimerProc(HWND hwnd,
                                                        UINT message,
                                                        UINT_PTR id,
                                                        DWORD time) {
  KillTimer(hwnd, id);
  auto it = retry_handlers_.find(id);
  if (it == retry_handlers_.end()) {
    return;
  }
  ConnectivityStreamHandler *self = it->second;
  retry_handlers_.erase(it);
  self->retry_timer_id_ = 0;

  if (!self->sink) {
    return; // Cancelled while the retry was pending.
  }

  if (self->TryStartListen()) {
    self->AddConnectivityEvent();
    return;
  }

  if (self->manager->GetError() == RPC_E_CANTCALLOUT_ININPUTSYNCCALL &&
      ++self->retry_attempts_ < 5) {
    self->ScheduleStartListenRetry();
    return;
  }

  // Persistent failure: report through the sink, where Dart-side stream
  // onError handlers can observe it (unlike an OnListen error, which the
  // framework can only surface as an uncatchable FlutterError).
  self->sink->Error(std::to_string(self->manager->GetError()),
                    "NetworkManager::StartListen", nullptr);
}

std::unique_ptr<FlStreamHandlerError>
ConnectivityStreamHandler::OnListenInternal(
    const flutter::EncodableValue *arguments,
    std::unique_ptr<FlEventSink> &&events) {
  sink = std::move(events);

  if (!TryStartListen()) {
    if (manager->GetError() == RPC_E_CANTCALLOUT_ININPUTSYNCCALL) {
      // The platform thread is inside an input-synchronous call (e.g. a
      // SendMessage-driven window callback), where COM rejects the outgoing
      // Advise. The subscription itself is fine — retry once the message
      // loop spins, and still emit the initial snapshot below.
      retry_attempts_ = 0;
      ScheduleStartListenRetry();
    } else {
      return std::make_unique<FlStreamHandlerError>(
          std::to_string(manager->GetError()), "NetworkManager::StartListen",
          nullptr);
    }
  }

  AddConnectivityEvent();
  return nullptr;
}

std::unique_ptr<FlStreamHandlerError>
ConnectivityStreamHandler::OnCancelInternal(
    const flutter::EncodableValue *arguments) {
  CancelStartListenRetry();
  manager->StopListen();
  sink.reset();
  return nullptr;
}

} // namespace

void ConnectivityPlusWindowsPluginRegisterWithRegistrar(
    FlutterDesktopPluginRegistrarRef registrar) {
  ConnectivityPlusWindowsPlugin::RegisterWithRegistrar(
      flutter::PluginRegistrarManager::GetInstance()
          ->GetRegistrar<flutter::PluginRegistrarWindows>(registrar));
}
