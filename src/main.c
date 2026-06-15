#include <zephyr/kernel.h>
#include <zephyr/sys/printk.h>
#include <zephyr/net/socket.h>
#include <zephyr/net/mqtt.h>

#include <modem/nrf_modem_lib.h>
#include <modem/lte_lc.h>
#include <modem/modem_info.h>

#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>

#include <date_time.h>
#include <time.h>

int nrf_modem_at_cmd(void *buf, size_t len, const char *fmt, ...);

#define AWS_ENDPOINT "ac9ylqbi6jw1g-ats.iot.us-east-1.amazonaws.com"
#define AWS_PORT "8883"

#define CLIENT_ID "nrf-try-1"

#define MQTT_TELEMETRY_TOPIC "traps/trap_001/telemetry"
#define MQTT_COMMAND_TOPIC   "traps/trap_001/command"

#define TLS_SEC_TAG 42

#define TELEMETRY_INTERVAL_SECONDS 10
#define MQTT_PROCESS_INTERVAL_MS   100

#define MQTT_THREAD_STACK_SIZE 4096
#define MQTT_THREAD_PRIORITY   5

static struct mqtt_client client;
static struct sockaddr_storage broker;

static uint8_t rx_buffer[1024];
static uint8_t tx_buffer[1024];

static bool mqtt_connected = false;
static bool mqtt_ready_for_processing = false;

/*
 * Shared trap status.
 *
 * MQTT receive thread updates this.
 * Telemetry publishing loop reads this.
 */
static char trap_status[16] = "active";

/*
 * Mutex protects trap_status from being read and written at the same time.
 */
K_MUTEX_DEFINE(status_mutex);

/*
 * Mutex protects MQTT client operations because one thread calls mqtt_process()
 * while the main loop calls mqtt_publish().
 */
K_MUTEX_DEFINE(mqtt_mutex);

/*
 * MQTT service thread definition.
 */
K_THREAD_STACK_DEFINE(mqtt_thread_stack, MQTT_THREAD_STACK_SIZE);
static struct k_thread mqtt_thread_data;


static void set_trap_status(const char *new_status)
{
    k_mutex_lock(&status_mutex, K_FOREVER);

    strncpy(trap_status, new_status, sizeof(trap_status) - 1);
    trap_status[sizeof(trap_status) - 1] = '\0';

    k_mutex_unlock(&status_mutex);
}


static void get_trap_status(char *status_copy, size_t status_copy_size)
{
    k_mutex_lock(&status_mutex, K_FOREVER);

    strncpy(status_copy, trap_status, status_copy_size - 1);
    status_copy[status_copy_size - 1] = '\0';

    k_mutex_unlock(&status_mutex);
}


static void run_at(const char *cmd)
{
    char resp[512] = {0};

    int err = nrf_modem_at_cmd(resp, sizeof(resp), "%s", cmd);

    printk("\nCMD: %s\n", cmd);

    if (err) {
        printk("ERR: %d\n", err);
    } else {
        printk("RESP:\n%s\n", resp);
    }
}


static void lte_debug_checks(void)
{
    run_at("AT+CPIN?");
    run_at("AT+CEREG?");
    run_at("AT+CSQ");
    run_at("AT+CESQ");
    run_at("AT+CGATT?");
    run_at("AT+CGDCONT?");
    run_at("AT+CGPADDR");
    run_at("AT+COPS?");
}


static int subscribe_to_command_topic(void)
{
    static uint16_t sub_message_id = 1000;

    struct mqtt_topic topic = {
        .topic = {
            .utf8 = (uint8_t *)MQTT_COMMAND_TOPIC,
            .size = strlen(MQTT_COMMAND_TOPIC)
        },
        .qos = MQTT_QOS_1_AT_LEAST_ONCE
    };

    struct mqtt_subscription_list sub_list = {
        .list = &topic,
        .list_count = 1,
        .message_id = sub_message_id++
    };

    printk("Subscribing to command topic: %s\n", MQTT_COMMAND_TOPIC);

    return mqtt_subscribe(&client, &sub_list);
}


static void handle_command_message(const struct mqtt_publish_param *pub)
{
    uint8_t payload_buf[128];
    size_t payload_len = pub->message.payload.len;
    size_t received = 0;

    memset(payload_buf, 0, sizeof(payload_buf));

    while ((received < payload_len) && (received < sizeof(payload_buf) - 1)) {
        size_t space_left = (sizeof(payload_buf) - 1) - received;
        size_t remaining = payload_len - received;
        size_t read_len = remaining < space_left ? remaining : space_left;

        int bytes = mqtt_read_publish_payload(&client,
                                               payload_buf + received,
                                               read_len);

        if (bytes < 0) {
            printk("Failed to read command payload: %d\n", bytes);
            return;
        }

        if (bytes == 0) {
            break;
        }

        received += bytes;
    }

    payload_buf[received] = '\0';

    printk("Command received: %s\n", payload_buf);

    /*
     * Supported command payloads for now:
     *
     * active
     * deactive
     * deactivate
     *
     * This also works if Lambda later sends simple JSON containing
     * the word active/deactive/deactivate.
     */
    if ((strstr((char *)payload_buf, "deactive") != NULL) ||
        (strstr((char *)payload_buf, "deactivate") != NULL)) {

        set_trap_status("deactive");
        printk("Trap status updated to DEACTIVE\n");
    }
    else if (strstr((char *)payload_buf, "active") != NULL) {

        set_trap_status("active");
        printk("Trap status updated to ACTIVE\n");
    }
    else {
        char current_status[16];
        get_trap_status(current_status, sizeof(current_status));
        printk("Unknown command. Status unchanged: %s\n", current_status);
    }
}


static void mqtt_evt_handler(struct mqtt_client *client,
                             const struct mqtt_evt *evt)
{
    switch (evt->type) {

    case MQTT_EVT_CONNACK:
        printk("MQTT_EVT_CONNACK result: %d\n", evt->result);

        if (evt->result == 0) {
            mqtt_connected = true;
            printk("Connected to AWS IoT Core\n");
        }

        break;

    case MQTT_EVT_DISCONNECT:
        printk("MQTT disconnected, result: %d\n", evt->result);
        mqtt_connected = false;
        mqtt_ready_for_processing = false;
        break;

    case MQTT_EVT_PUBACK:
        printk("AWS received telemetry. PUBACK id: %u\n",
               evt->param.puback.message_id);
        break;

    case MQTT_EVT_SUBACK:
        printk("Subscription acknowledged. SUBACK id: %u\n",
               evt->param.suback.message_id);
        break;

    case MQTT_EVT_PUBLISH:
        printk("Incoming MQTT publish detected\n");

        handle_command_message(&evt->param.publish);

        if (evt->param.publish.message.topic.qos == MQTT_QOS_1_AT_LEAST_ONCE) {
            struct mqtt_puback_param ack = {
                .message_id = evt->param.publish.message_id
            };

            int err = mqtt_publish_qos1_ack(client, &ack);

            if (err) {
                printk("Failed to send PUBACK for command: %d\n", err);
            }
        }

        break;

    default:
        break;
    }
}


static int broker_init(void)
{
    struct addrinfo *res;

    struct addrinfo hints = {
        .ai_family = AF_INET,
        .ai_socktype = SOCK_STREAM
    };

    int err = getaddrinfo(AWS_ENDPOINT, AWS_PORT, &hints, &res);

    if (err) {
        printk("DNS lookup failed: %d\n", err);
        return err;
    }

    memcpy(&broker, res->ai_addr, res->ai_addrlen);

    freeaddrinfo(res);

    printk("AWS endpoint resolved\n");

    return 0;
}


static void mqtt_client_setup(void)
{
    static sec_tag_t sec_tags[] = { TLS_SEC_TAG };

    mqtt_client_init(&client);

    client.broker = &broker;
    client.evt_cb = mqtt_evt_handler;

    client.client_id.utf8 = (uint8_t *)CLIENT_ID;
    client.client_id.size = strlen(CLIENT_ID);

    client.password = NULL;
    client.user_name = NULL;

    client.protocol_version = MQTT_VERSION_3_1_1;

    client.rx_buf = rx_buffer;
    client.rx_buf_size = sizeof(rx_buffer);

    client.tx_buf = tx_buffer;
    client.tx_buf_size = sizeof(tx_buffer);

    client.keepalive = 60;

    client.transport.type = MQTT_TRANSPORT_SECURE;

    client.transport.tls.config.peer_verify = TLS_PEER_VERIFY_REQUIRED;
    client.transport.tls.config.sec_tag_list = sec_tags;
    client.transport.tls.config.sec_tag_count = 1;
    client.transport.tls.config.hostname = AWS_ENDPOINT;
    client.transport.tls.config.cipher_list = NULL;
}


static int mqtt_process(void)
{
    int err;

    err = mqtt_input(&client);

    if (err && err != -EAGAIN) {
        printk("mqtt_input error: %d\n", err);
        return err;
    }

    err = mqtt_live(&client);

    if (err && err != -EAGAIN) {
        printk("mqtt_live error: %d\n", err);
        return err;
    }

    return 0;
}


static int publish_to_aws(const char *payload)
{
    static uint16_t message_id = 1;

    struct mqtt_publish_param param;

    memset(&param, 0, sizeof(param));

    param.message.topic.qos = MQTT_QOS_1_AT_LEAST_ONCE;
    param.message.topic.topic.utf8 = (uint8_t *)MQTT_TELEMETRY_TOPIC;
    param.message.topic.topic.size = strlen(MQTT_TELEMETRY_TOPIC);

    param.message.payload.data = (uint8_t *)payload;
    param.message.payload.len = strlen(payload);

    param.message_id = message_id++;
    param.dup_flag = 0;
    param.retain_flag = 0;

    printk("Publishing telemetry: %s\n", payload);

    return mqtt_publish(&client, &param);
}


static void mqtt_service_thread(void *arg1, void *arg2, void *arg3)
{
    ARG_UNUSED(arg1);
    ARG_UNUSED(arg2);
    ARG_UNUSED(arg3);

    printk("MQTT service thread started\n");

    while (1) {
        if (mqtt_ready_for_processing) {
            k_mutex_lock(&mqtt_mutex, K_FOREVER);

            int err = mqtt_process();

            k_mutex_unlock(&mqtt_mutex);

            if (err) {
                printk("MQTT service thread mqtt_process failed: %d\n", err);
            }
        }

        k_sleep(K_MSEC(MQTT_PROCESS_INTERVAL_MS));
    }
}


int main(void)
{
    int err;

    printk("Starting AWS MQTT publish + subscribe firmware\n");

    err = nrf_modem_lib_init();

    if (err) {
        printk("nrf_modem_lib_init failed: %d\n", err);
        return 0;
    }

    printk("Modem initialized\n");

    printk("Configuring modem for Hologram...\n");

    run_at("AT%XSYSTEMMODE=1,0,0,0");
    run_at("AT+CGDCONT=1,\"IP\",\"hologram\"");
    run_at("AT+CFUN=0");

    k_sleep(K_SECONDS(2));

    run_at("AT+CFUN=1");

    k_sleep(K_SECONDS(5));

    printk("\nLTE checks before connect:\n");
    lte_debug_checks();

    printk("\nBefore LTE connect\n");

    err = lte_lc_connect();

    printk("After LTE connect: %d\n", err);

    printk("\nLTE checks after connect:\n");
    lte_debug_checks();

    if (err) {
        printk("LTE connection failed, stopping before AWS/MQTT.\n");
        return 0;
    }

    printk("LTE connected\n");

    err = broker_init();

    if (err) {
        printk("broker_init failed: %d\n", err);
        return 0;
    }

    mqtt_client_setup();

    err = mqtt_connect(&client);

    if (err) {
        printk("mqtt_connect failed: %d\n", err);
        return 0;
    }

    printk("mqtt_connect called\n");

    /*
     * Wait for AWS CONNACK.
     */
    while (!mqtt_connected) {
        err = mqtt_input(&client);

        if (err) {
            printk("mqtt_input before connected failed: %d\n", err);
            break;
        }

        err = mqtt_live(&client);

        if (err && err != -EAGAIN) {
            printk("mqtt_live before connected failed: %d\n", err);
            break;
        }

        k_sleep(K_MSEC(100));
    }

    if (!mqtt_connected) {
        printk("MQTT did not connect successfully. Stopping.\n");
        return 0;
    }

    /*
     * Subscribe once after the connection is confirmed.
     * Do not put this in the telemetry loop.
     */
    err = subscribe_to_command_topic();

    if (err) {
        printk("mqtt_subscribe failed: %d\n", err);
        return 0;
    }

    printk("Subscribe request sent\n");

    /*
     * Process MQTT briefly so SUBACK can be received before starting
     * the dedicated MQTT service thread.
     */
    for (int i = 0; i < 10; i++) {
        mqtt_process();
        k_sleep(K_MSEC(100));
    }

    /*
     * Now MQTT can be serviced continuously by the thread.
     */
    mqtt_ready_for_processing = true;

    k_thread_create(&mqtt_thread_data,
                    mqtt_thread_stack,
                    K_THREAD_STACK_SIZEOF(mqtt_thread_stack),
                    mqtt_service_thread,
                    NULL,
                    NULL,
                    NULL,
                    MQTT_THREAD_PRIORITY,
                    0,
                    K_NO_WAIT);

    printk("Main telemetry loop starting\n");

    while (1) {
        const char *id = "trap_001";

        float voltage = 5.0f + ((float)rand() / RAND_MAX) * (8.0f - 4.0f);

        int64_t unix_time_ms;
        char time_str[32] = "unknown";

        if (date_time_now(&unix_time_ms) == 0) {
            int64_t unix_time_s = unix_time_ms / 1000;
            time_t t = unix_time_s;

            struct tm *tm_info = gmtime(&t);

            strftime(time_str,
                     sizeof(time_str),
                     "%Y-%m-%d %H:%M:%S",
                     tm_info);

            printk("Time OK\n");
        } else {
            printk("Time NOT available\n");
        }

        char rsrp_buf[64];
        int sig_len;
        float rsrp_read;
        char payload[256];
        char current_status[16];

        get_trap_status(current_status, sizeof(current_status));

        sig_len = modem_info_string_get(MODEM_INFO_RSRP,
                                         rsrp_buf,
                                         sizeof(rsrp_buf));

        if (sig_len <= 0) {
            printk("Failed to get RSRP, err = %d\n", sig_len);

            snprintf(rsrp_buf, sizeof(rsrp_buf), "Unknown");

            rsrp_read = -999.0f;
        } else {
            printk("RSRP raw value: %s\n", rsrp_buf);

            int rsrp_raw = atoi(rsrp_buf);
            int rsrp_dbm = rsrp_raw - 140;

            rsrp_read = rsrp_dbm;
        }

        snprintf(payload,
                 sizeof(payload),
                 "{\"device_id\":\"%s\","
                 "\"status\":\"%s\","
                 "\"voltage\":%.2f,"
                 "\"rsrp\":%.0f,"
                 "\"timestamp\":\"%s\"}",
                 id,
                 current_status,
                 (double)voltage,
                 (double)rsrp_read,
                 time_str);

        k_mutex_lock(&mqtt_mutex, K_FOREVER);

        err = publish_to_aws(payload);

        k_mutex_unlock(&mqtt_mutex);

        if (err) {
            printk("mqtt_publish failed: %d\n", err);
        }

        k_sleep(K_SECONDS(TELEMETRY_INTERVAL_SECONDS));
    }

    return 0;
}