/*!  \file redis_wrapper.cpp
\brief redis op wrapper class impl for simple cluster model
\author zhaohongchao(zadezhao@tencent.com)
\date 2015/2/2 14:03:57
\version 1.0.0.0
\since 1.0.0.0
&copy; All rights reserved, Tencent
*/

#include "redis_wrapper.h"

#include <ComLogger.h>

extern "C" {
#include <net.h> // for redisContextSetTimeout
}

#include <stdarg.h>
#include <unistd.h>
#include <algorithm>
#include <boost/thread.hpp>

//////////////////////////////////////////////////////////////////////////
redis_cfg::redis_cfg() {
    memset(this, 0, sizeof(*this));
}
//////////////////////////////////////////////////////////////////////////
redis_request::redis_request(int retry_count)
    : retry_count(retry_count) {
    if (this->retry_count > MAX_RETRY_COUNT) {
        this->retry_count = MAX_RETRY_COUNT;
        WRITE_LOG(LOG_WARNING, "redis retry count %d  is too big,adjust to %d",
                  retry_count, this->retry_count);
    }
}

redis_request::~redis_request() {
    for (redisreqvec_t::iterator itr = reqs.begin(), end = reqs.end(); itr != end; ++ itr) {
        if (itr->first) {
            ::free(itr->first);
        }
    }
}

int redis_request::append_command(const char *format, ...) {
    va_list ap;
    va_start(ap, format);
    int ret = append_vcommand(format, ap);
    va_end(ap);
    return ret;
}

int redis_request::append_vcommand(const char *format, va_list ap) {
    char *cmd = NULL;
    int len = redisvFormatCommand(&cmd, format, ap);

    if (len == -1) {
        WRITE_LOG(LOG_FATAL, "outof memory");
        return -1;
    }

    reqs.push_back(std::make_pair(cmd, static_cast<size_t>(len)));
    WRITE_LOG(LOG_DEBUG, "append command '%s' len %d OK", cmd, len);
    return 0;
}
//////////////////////////////////////////////////////////////////////////
namespace {
void reset_replys(replyvec_t &replys) {
    for (replyvec_t::iterator itr = replys.begin(), end = replys.end(); itr != end; ++itr) {
        if (*itr != NULL) {
            freeReplyObject(*itr);
        }
    }

    WRITE_LOG(LOG_DEBUG, "clear %zd replys", replys.size());
    replys.clear();
}
}

redis_response::redis_response() {
}


redis_response::~redis_response() {
    reset_replys(replys);
}


//////////////////////////////////////////////////////////////////////////
namespace {
/*! parse redis config string
\param redis   redis config object
\param str   redis config string, such as "1.2.3.4:6379"
\return >=0 for ok ; others for fail
*/
int redis_cfg_parse_(redis_cfg *redis, const char *str) {
    if (sscanf(str, "%[^:]:%d", redis->host, &redis->port) != 2) {
        WRITE_LOG(LOG_ERROR, "redis '%s' parse error", str);
        return -1;
    }

    WRITE_LOG(LOG_DEBUG, "parse redis '%s:%d' ok", redis->host, redis->port);
    return 0;
}

/*! redis close connection
\param redis   redis config object
\return 0
*/
int redis_close_(redis_cfg *redis) {
    WRITE_LOG(LOG_ERROR, "redis '%s:%d' is wrong for '%s' & closed",
              redis->host, redis->port, redis->ctx->errstr);
    redisFree(redis->ctx);
    redis->ctx = NULL;
    //record redis close time for reconnect next time
    time(&redis->err_time);
    return 0;
}

/*! redis connect
\param redis  redis config object
\param tv   redis op timeout , maybe NULL
\return >=0 for ok ; others for fail
*/
int redis_connect_(redis_cfg *redis, const timeval *tv) {
    if (tv != NULL) {
        redis->ctx = redisConnectWithTimeout(redis->host, redis->port, *tv);
    } else {
        redis->ctx = redisConnect(redis->host, redis->port);
    }

    assert(NULL != redis->ctx);

    if (redis->ctx->err) {
        redis_close_(redis);
        return -1;
    }

    if (tv != NULL) {
        if (REDIS_OK != redisContextSetTimeout(redis->ctx, *tv)) {
            redis_close_(redis);
            return -1;
        }
    }

    WRITE_LOG(LOG_INFO, "redis '%s:%d' connect ok", redis->host, redis->port);
    return 0;
}

/*! redis check and reconnect when it has been closed
\param redis   redis config object
\param interval   check interval
\return >=0 for ok ; others for fail
*/
int redis_check_connect_(redis_cfg *redis, int interval) {
    int ret = 0;

    if (NULL == redis->ctx) {
        time_t now = time(NULL);

        if (now >= redis->err_time + interval) {
            ret = (redis->timeout.tv_sec == 0 && redis->timeout.tv_usec == 0) ?
                  redis_connect_(redis, NULL) : redis_connect_(redis, &redis->timeout);

            if (ret < 0) {
                redis->err_time = now;
                WRITE_LOG(LOG_WARNING, "update err_time for next reconnect");
            }
        } else {
            WRITE_LOG(LOG_DEBUG, "redis '%s:%d' is not connected for time too short",
                      redis->host, redis->port);
            ret = -1;
        }
    }

    return ret;
}

//! redis check reply function type
typedef bool redis_checkfn_t(const redis_cfg *, const redisReply *);

/*! redis response check for normal op
\param redis   redis_cfg object
\param reply   redis reply object
\return true
*/
bool empty_check(const redis_cfg *redis, const redisReply *reply)  {
    if (reply->type == REDIS_REPLY_ERROR) {
        // some commands such as "SHUTDOWN" response type is error but that is ok
        WRITE_LOG(LOG_WARNING, "redis '%s:%d' empty_check error '%s'",
                  redis->host, redis->port, reply->str);
    } else {
        WRITE_LOG(LOG_DEBUG, "redis '%s:%d' empty_check ok '%s'",
                  redis->host, redis->port, redis_reply_type_string(reply->type));

    }

    return true;
}

/*! redis response check for read op
\param redis   redis_cfg object
\param reply   redis reply object
\return true for ok; false for fail
*/
bool read_check(const redis_cfg *redis, const redisReply *reply) {
    if ((reply->type == REDIS_REPLY_STRING || reply->type == REDIS_REPLY_ARRAY
         || reply->type == REDIS_REPLY_INTEGER)) {
        WRITE_LOG(LOG_DEBUG, "redis '%s:%d' read_check ok '%s'",
                  redis->host, redis->port, redis_reply_type_string(reply->type));
        return true;
    }

    WRITE_LOG(LOG_ERROR, "redis '%s:%d' read_check error '%s',type %s,len %d",
              redis->host, redis->port, reply->str,
              redis_reply_type_string(reply->type), reply->len);
    return false;
}

/*! redis response check for write op
\param redis   redis_cfg object
\param reply   redis reply object
\return true for ok; false for fail
*/
bool write_check(const redis_cfg *redis, const redisReply *reply)  {
    if (reply->type == REDIS_REPLY_ERROR) {
        WRITE_LOG(LOG_ERROR, "redis '%s:%d' write_check error '%s'",
                  redis->host, redis->port, reply->str);
        return false;
    }

    WRITE_LOG(LOG_DEBUG, "redis '%s:%d' write_check ok '%s'",
              redis->host, redis->port, redis_reply_type_string(reply->type));
    return true;
}

/*! redis operation
\param redis   redis config object
\param req   redis request
\param res   redis response
\param check redis reply check funtion
\return >=0 for ok ; others for fail
*/
int redis_op_(redis_cfg *redis, const redis_request &req, replyvec_t &res, redis_checkfn_t check) {
    for (redisreqvec_t::const_iterator itr = req.reqs.begin(), end = req.reqs.end(); itr != end; ++ itr) {
        if (REDIS_OK != redisAppendFormattedCommand(redis->ctx, itr->first, itr->second)) {
            WRITE_LOG(LOG_FATAL, "redisAppendFormattedCommand error");//outof memory
            return -1;
        }
    }

    int ret = 0;

    for (size_t i = 0, size = req.reqs.size(); i != size; ++i) {
        void *reply = NULL;
        redisGetReply(redis->ctx, &reply);
        res.push_back(reinterpret_cast<redisReply *>(reply));

        if (NULL == reply) { //redis is error and should be closed
            redis_close_(redis);
            ret = -1;
            break;
        } else if (!check(redis, res.back())) {
            ret = -1;
        }
    }

    WRITE_LOG(LOG_DEBUG, "redis '%s:%d' op %d", redis->host, redis->port, ret);
    return ret;
}

/*! redis operation for one node in cluster
\param redis   redis cluster object
\param redis_idx   which redis in cluster
\param req   redis request
\param res   redis response
\param check   redis reply check function
\return >=0 for ok ; others for fail
*/
int redis_op_poll_(redis_connection &redis, size_t &redis_idx, const redis_request &req, redis_response &res, redis_checkfn_t check) {
    if (redis.redis.empty()) {
        WRITE_LOG(LOG_ERROR, "empty redis");
        return -1;
    }

    const size_t SIZE = redis.redis.size();
    assert(redis_idx < SIZE);

    for (size_t i = 0; i != SIZE; ++i) {
        redis_cfg *redisc = &redis.redis[redis_idx];

        if (++redis_idx == SIZE) {
            redis_idx = 0;
        }

        if (redis_check_connect_(redisc, redis.reconnect_interval) == 0) {
            if (redis_op_(redisc, req, res.replys, check) >= 0) {
                return 0;
            } else {
                reset_replys(res.replys);// clear partial results
            }
        }
    }

    WRITE_LOG(LOG_ERROR, "redis_op_poll error");
    return -1;
}

/*! thread sleep for milliseconds
\param msecs   milliseconds
*/
void thread_sleep(int msecs) {
    struct timeval delay;
    delay.tv_sec = 0;
    delay.tv_usec = msecs * 1000;
    select(0, NULL, NULL, NULL, &delay);
}

/*! redis operation callback for thread call
\param redis   redis cluster obecjt
\param idx   which redis in cluster
\param req   redis request
\param res   redis response
\param err_count plus one on error
\param check  check redis reply object
\return >=0 for ok ; others for fail
*/
int redis_op_callback_(redis_connection *redis, size_t idx, const redis_request *req, replyvec_t *res, int *err_count, redis_checkfn_t check) {
    redis_cfg *cfg = &redis->redis[idx];
    timeval * tv = (cfg->timeout.tv_sec == 0 && cfg->timeout.tv_usec == 0) ? NULL : &cfg->timeout;
    
    for (int i = 0; i != req->retry_count; ++i) {
        if (cfg->ctx == NULL){
            redis_connect_(cfg, tv);
        }
        
        if (cfg->ctx != NULL) {
            if (redis_op_(cfg, *req, *res, check) == 0) {
                return 0;
            } else {
                reset_replys(*res);// clear partial results
            }
        }

        thread_sleep(redis->retry_interval);// sleep then try again
    }

    WRITE_LOG(LOG_ERROR, "redis op try %d times but error", req->retry_count);
    __sync_add_and_fetch(err_count, 1);
    return -1;
}

/*! redis opertion on all cluster nodes
\param redis   redis cluster object
\param req   redis request
\param res   redis response
\param check   redis reply chech function
\return >0 for all ok count ; others for error count
*/
int redis_op_all_(redis_connection &redis, const redis_request &req, redis_response &res, redis_checkfn_t check) {
    if (redis.redis.empty()) {
        WRITE_LOG(LOG_ERROR, "empty redis");
        return -1;
    }

    int err_count = 0;
    const size_t SIZE_1 = redis.redis.size() - 1;
    boost::thread_group threads;
    std::vector<replyvec_t> thread_replys(SIZE_1);

    for (size_t i = 0; i != SIZE_1; ++i) {
        threads.add_thread(new boost::thread(redis_op_callback_, &redis, i, &req, &thread_replys[i], &err_count, check));
    }

    redis_op_callback_(&redis, SIZE_1, &req, &res.replys, &err_count, check);
    threads.join_all();

    for (size_t i = 0; i != SIZE_1; ++i) {
        replyvec_t &rs = thread_replys[i];
        std::copy(rs.begin(), rs.end(), std::back_inserter(res.replys));
    }

    return err_count == 0 ? static_cast<int>(SIZE_1 + 1) : -err_count;
}

void reset(rediscfgvec_t &redis) {
    for (rediscfgvec_t::iterator itr = redis.begin(), end = redis.end(); itr != end; ++itr) {
        if (itr->ctx) {
            redisFree(itr->ctx);
        }
    }

    WRITE_LOG(LOG_WARNING, "close redis all %zd", redis.size());
    redis.clear();
}

}

redis_connection::redis_connection(int sleep_time, int interval)
    : redis_idx(0), retry_interval(sleep_time), reconnect_interval(interval) {
}

redis_connection::~redis_connection() {
    reset(redis);
}

int redis_connection::connect(const std::string &cfg, const timeval *tv) {
    const size_t count = std::count(cfg.begin(), cfg.end(), ',') + 1;
    redis.reserve(count);

    std::string::size_type pos = 0;
    std::string::size_type comma = cfg.find_first_of(',');

    while (true) {
        redis.push_back(redis_cfg());
        redis_cfg &conn = redis.back();

        if (NULL != tv) {
            conn.timeout = *tv;
        }

        if (redis_cfg_parse_(&conn, &cfg[pos]) < 0 ||
            redis_connect_(&conn, tv) < 0) {
            reset(redis);
            return -1;
        }

        if (comma == std::string::npos) {
            break;
        } else {
            pos = comma + 1;
            comma = cfg.find_first_of(',', pos);
        }
    }

    WRITE_LOG(LOG_INFO, "connect %zd redis ok", redis.size());

    return 0;
}

int redis_connection::normal_op(const redis_request &req, redis_response &res, int idx) {
    if (idx < 0) {
        return redis_op_all_(*this, req, res, &empty_check);
    } else {
        if (idx < static_cast<int>(redis.size())) {
            return redis_op_(&redis[idx], req, res.replys, &empty_check);
        } else {
            WRITE_LOG(LOG_ERROR, "bad server index %d", idx);
            return -1;
        }
    }
}


int redis_connection::read_op(const redis_request &req, redis_response &res) {
    return redis_op_poll_(*this, redis_idx, req, res, &read_check);
}

int redis_connection::write_op(const redis_request &req, redis_response &res) {
    return redis_op_all_(*this, req, res, &write_check);
}
//////////////////////////////////////////////////////////////////////////
const char *redis_reply_type_string(int type) {
    switch (type) {
        case REDIS_REPLY_STRING:
            return "string";

        case REDIS_REPLY_ARRAY:
            return "array";

        case REDIS_REPLY_INTEGER:
            return "integer";

        case REDIS_REPLY_NIL:
            return "nil";

        case REDIS_REPLY_STATUS:
            return "status";

        case REDIS_REPLY_ERROR:
            return "error";

        default:
            return "unknown";
    }
}

