/*!  \file redis_wrapper.h
\brief redis op wrapper class for simple cluster model
\author zhaohongchao(zadezhao@tencent.com)
\date 2015/1/30 15:00:00
\version 1.0.0.0
\since 1.0.0.0
&copy; All rights reserved, Tencent
*/
#ifndef _TENCENT_TRAFFIC_REDIS_WRAPPER_H_
#define _TENCENT_TRAFFIC_REDIS_WRAPPER_H_

#include <hiredis.h>
#include <vector>
#include <string>
#include <assert.h>

/*! get redis reply string for type
\param type  redis reply type
\return reply string for type
*/
const char *redis_reply_type_string(int type);

//! typedef for redis request array,cmd&cmd_size
typedef std::vector<std::pair<char *, size_t> > redisreqvec_t;


/*! redis request class
*/
class redis_request {

    //! no copy
    redis_request(const redis_request &);

    //! no assign
    redis_request &operator=(const redis_request &);
  public:
    enum {
        MAX_RETRY_COUNT = 3,/*!< max retry count when redis error. */
    };
    //! retry count when write error
    int retry_count;

    //! redis request array
    redisreqvec_t reqs;

    //! ctor
    redis_request(int retry_count = MAX_RETRY_COUNT);

    //! dtor
    ~redis_request();

    /*! append redis command
    \param format   command format
    \param ...   command args
    \return >=0 for ok; others fail
    */
    int append_command(const char *format, ...);

    /*! append redis command
    \param format   command format
    \param ap   command args
    \return >=0 for ok; others fail
    */
    int append_vcommand(const char *format, va_list ap);
};

//! typedef for redis reply array
typedef std::vector<redisReply *> replyvec_t;

/*! redis response class
*/
class redis_response {

    //! no copy
    redis_response(const redis_response &);

    //! no assign
    redis_response &operator=(const redis_response &);

  public:
    //! reply array
    replyvec_t replys;

    //! ctor
    redis_response();

    //! dtor
    ~redis_response();

    /*! get front reply
    \return first reply object in array
    */
    redisReply *first_reply() const {
        assert(!replys.empty());
        return replys.front();
    }
};

/*! redis config struct
*/
struct redis_cfg {
    //! redis context
    redisContext *ctx;

    //! redis connection error time
    time_t err_time;

    //! redis port
    int port;

    //! redis host
    char host[20];

    //! redis connect&op timeout
    timeval timeout;

    //! ctor
    redis_cfg();
};

//! typedef for redis connection config array
typedef std::vector<redis_cfg> rediscfgvec_t;

/*! redis_connection class
*/
class redis_connection {
    //! no copy
    redis_connection(const redis_connection &);

    //! no assign
    redis_connection &operator=(const redis_connection &);

    //! redis poll index
    size_t redis_idx;
  public:

    enum {
        DEFAULT_SLEEP_INTERVAL = 10,/*!< sleep interval(milliseconds) for redis retry on error. */
        DEFAULT_RECONNECT_INTERVAL = 60,/*!<time interval(seconds) for redis reconnect. */
    };
    //! sleep time interval(milliseconds) for redis retry when op error
    int retry_interval;

    //! reconnect time interval(seconds) when redis connection error
    int reconnect_interval;

    //! redis cfg array
    rediscfgvec_t redis;

    //! ctor
    redis_connection(int sleep_time = DEFAULT_SLEEP_INTERVAL, int interval = DEFAULT_RECONNECT_INTERVAL);

    //! dtor
    ~redis_connection();

    /*! connect redis
    \param cfg   redis config string,such as "1.2.3.4:6379,5.6.7.8:6379"
    \param tv   redis op timeout , not use timeout when tv is NULL
    \return >=0 ok; others fail
    */
    int connect(const std::string &cfg, const timeval *tv);

    /*! redis normal op
    \param req   request object
    \param res   response object
    \param idx   server index;-1 for all servers
    \return >=0 ok; others fail
    */
    int normal_op(const redis_request &req, redis_response &res, int idx);

    /*! redis read op
    \param req   request object
    \param res   response object
    \return >=0 ok; others fail
    */
    int read_op(const redis_request &req, redis_response &res);

    /*! redis write op
    \param req   request object
    \param res   response object
    \return >0 for all ok count ; others for error count
    */
    int write_op(const redis_request &req, redis_response &res);
};


#endif // _TENCENT_TRAFFIC_REDIS_WRAPPER_H_
