// Process supervisor — the OS-process-management layer sitting on top of
// the multicast split. Launches engine_subscriber and feed_publisher as
// real child processes (fork/exec, not threads), and watches them with
// waitpid(). If the long-running subscriber crashes, it gets restarted
// with backoff, up to a limit. If the publisher (which is meant to be
// finite — it sends its feed, then exits) exits normally, that's success,
// not failure.

#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

namespace {
    constexpr int kMaxRestarts = 5;

    pid_t spawn(const std::string& path, const std::vector<std::string>& args){
        pid_t pid = fork();
        if(pid < 0) {
            perror("fork");
            std::exit(1);
        }

        if (pid==0) {
            // Child: replace this process image with the target binary.
        std::vector<char*> argv;
        argv.push_back(const_cast<char*>(path.c_str()));
        for (auto& a : args) argv.push_back(const_cast<char*>(a.c_str()));
        argv.push_back(nullptr);
        execv(path.c_str(), argv.data());
        perror("execv");  // only reached if execv itself failed
        _exit(127);
        }
        return pid; // parent : pid of the new child
    }
}

int main(int argc, char** argv){
    setvbuf(stdout,nullptr,_IOLBF,0); // flush perline even when redirected to a file
    std::string build_dir = (argc > 1) ? argv[1] : "./build";
    std::string subscriber_path = build_dir + "/engine_subscriber";
    std::string publisher_path = build_dir + "/feed_publisher";

    pid_t subscriber_pid = spawn(subscriber_path,{});
    std::printf("supervisor: launched engine_subscriber (pid %d)\n", subscriber_pid);
    // Give the subscriber a moment to bind and join the multicast group
    // before the publisher starts sending — otherwise early packets
    // are lost before anyone's listening.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    pid_t publisher_pid = spawn(publisher_path,{"--count","200000"});
    std::printf("supervisor : launched feed_publisher (pid%d)\n", publisher_pid);
  

    int subscriber_restarts = 0;
    bool publisher_done = false;

    while(true){
        int status = 0;
        pid_t exited = waitpid(-1, &status, 0); //blocks until any child changes state;
        if (exited < 0) {
            perror("waitpid");
            break;
        }
    
    bool crashed = WIFSIGNALED(status);
    int code = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
    
    if (exited == publisher_pid) {
            publisher_done = true;
            if (crashed) {
                std::printf("supervisor: feed_publisher CRASHED (signal %d)\n", WTERMSIG(status));
            } else {
                std::printf("supervisor: feed_publisher exited normally (code %d)\n", code);
            }
        } else if (exited == subscriber_pid) {
            if (crashed) {
                std::printf("supervisor: engine_subscriber CRASHED (signal %d)\n",
                            WTERMSIG(status));
            } else if (code != 0) {
                std::printf("supervisor: engine_subscriber exited with error (code %d)\n", code);
            } else {
                // Clean exit (code 0) means it saw the end-of-stream marker
                // and finished on its own — that's success, not a failure
                // to recover from. Restarting here would be wrong: a fresh
                // subscriber joining now has missed the one end marker
                // already sent, and would block in recv() forever waiting
                // for a packet that's never coming again.
                std::printf("supervisor: engine_subscriber finished cleanly — done\n");
                break;
            }

            if (subscriber_restarts >= kMaxRestarts) {
                std::printf("supervisor: engine_subscriber failed %d times — giving up\n",
                            subscriber_restarts);
                break;
            }

            subscriber_restarts++;
            std::printf("supervisor: restarting engine_subscriber (attempt %d/%d)\n",
                        subscriber_restarts, kMaxRestarts);
            std::this_thread::sleep_for(
                std::chrono::milliseconds(500 * subscriber_restarts));  // linear backoff
            subscriber_pid = spawn(subscriber_path, {});
        }
    }

    std::printf("supervisor: shutting down\n");
    return 0;
}
   





