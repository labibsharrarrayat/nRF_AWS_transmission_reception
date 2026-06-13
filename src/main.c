#include <zephyr/kernel.h>          // Zephyr core kernel functions: sleep, timing macros, thread utilities
#include <zephyr/sys/printk.h>      // printk() for printing debug messages to the serial console
#include <zephyr/net/socket.h>      // BSD socket API used internally by MQTT and DNS lookup
#include <zephyr/net/mqtt.h>        // Zephyr MQTT client API

#include <modem/nrf_modem_lib.h>    // Initializes Nordic modem library
#include <modem/lte_lc.h>           // LTE link controller API for connecting to cellular network
#include <modem/modem_info.h>       // modem_info_string_get() for reading modem info like RSRP

#include <string.h>                 // strlen(), memcpy(), memset()
#include <errno.h>                  // Error codes such as -EAGAIN
#include <stdio.h>                  // snprintf()

#include <date_time.h>
#include <time.h>

/*
 * Manual declaration of Nordic modem AT command function.
 *
 * This function sends AT commands directly to the modem.
 * Example:
 *   AT+CPIN?
 *   AT+CEREG?
 *   AT+CSQ
 *
 * The modem response is written into the buffer passed as the first argument.
 */
int nrf_modem_at_cmd(void *buf, size_t len, const char *fmt, ...);


/*
 * AWS IoT Core MQTT endpoint.
 *
 * This is your device data endpoint from AWS IoT Core.
 * The nRF board connects to this address using MQTT over TLS.
 */
#define AWS_ENDPOINT "ac9ylqbi6jw1g-ats.iot.us-east-1.amazonaws.com"

/*
 * Secure MQTT port.
 *
 * Port 8883 is the standard port for MQTT over TLS.
 * AWS IoT Core requires TLS for secure device communication.
 */
#define AWS_PORT "8883"

/*
 * MQTT client ID.
 *
 * This identifies your device to AWS IoT Core.
 * For production, each physical device should have a unique client ID.
 */
#define CLIENT_ID "nrf-try-1"

/*
 * MQTT topic where the board publishes telemetry data.
 *
 * AWS IoT Core receives messages on this topic.
 * Later, an AWS IoT Rule can forward this data to Lambda, API Gateway,
 * DynamoDB, PostgreSQL, or another backend.
 */
#define MQTT_TOPIC "traps/trap_001/telemetry"

/*
 * TLS security tag.
 *
 * On Nordic cellular modems, certificates are stored inside the modem.
 * The TLS_SEC_TAG tells the modem which certificate set to use.
 *
 * In your case, security tag 42 should contain:
 *   - Amazon Root CA
 *   - Device certificate
 *   - Device private key
 */
#define TLS_SEC_TAG 42


/*
 * Main MQTT client structure.
 *
 * This stores all MQTT client configuration:
 *   - broker address
 *   - client ID
 *   - buffers
 *   - TLS settings
 *   - event callback
 */
static struct mqtt_client client;

/*
 * Broker address storage.
 *
 * getaddrinfo() resolves the AWS endpoint into an IP address.
 * The resolved IP address is copied into this variable.
 */
static struct sockaddr_storage broker;

/*
 * MQTT receive buffer.
 *
 * Incoming MQTT packets from AWS are stored here.
 * Examples:
 *   - CONNACK
 *   - PUBACK
 *   - PINGRESP
 */
static uint8_t rx_buffer[1024];

/*
 * MQTT transmit buffer.
 *
 * Outgoing MQTT packets are assembled here before being sent.
 * Examples:
 *   - CONNECT
 *   - PUBLISH
 *   - PINGREQ
 */
static uint8_t tx_buffer[1024];

/*
 * Connection status flag.
 *
 * false means MQTT connection is not fully established yet.
 * true means AWS accepted the MQTT CONNECT packet and returned CONNACK.
 */
static bool mqtt_connected = false;


/*
 * Helper function for sending AT commands to the modem.
 *
 * This is mainly for debugging.
 * It sends one AT command, prints the command, then prints the modem response.
 */
static void run_at(const char *cmd)
{
    /*
     * Buffer used to store the modem response.
     * Initialized to zero so that it is safely null-terminated.
     */
    char resp[512] = {0};

    /*
     * Send the AT command to the modem.
     *
     * "%s" means the command string is inserted directly.
     * The modem response is written into resp.
     */
    int err = nrf_modem_at_cmd(resp, sizeof(resp), "%s", cmd);

    printk("\nCMD: %s\n", cmd);

    /*
     * If err is nonzero, the AT command failed.
     * Otherwise, print the modem response.
     */
    if (err) {
        printk("ERR: %d\n", err);
    } else {
        printk("RESP:\n%s\n", resp);
    }
}


/*
 * LTE diagnostic checks.
 *
 * These AT commands help you understand the modem/network state.
 * This is useful before and after LTE connection.
 */
static void lte_debug_checks(void)
{
    /*
     * Checks SIM status.
     * Expected good response: +CPIN: READY
     */
    run_at("AT+CPIN?");

    /*
     * Checks LTE registration status.
     *
     * Important values:
     *   1 = registered on home network
     *   5 = registered while roaming
     */
    run_at("AT+CEREG?");

    /*
     * Basic signal quality.
     * This is older/less detailed than CESQ but still useful.
     */
    run_at("AT+CSQ");

    /*
     * Extended signal quality.
     * Gives more LTE-specific signal information.
     */
    run_at("AT+CESQ");

    /*
     * Checks whether the modem is attached to packet data service.
     * Expected good response: +CGATT: 1
     */
    run_at("AT+CGATT?");

    /*
     * Shows PDP context settings.
     * This confirms APN configuration.
     */
    run_at("AT+CGDCONT?");

    /*
     * Shows assigned IP address after cellular connection.
     */
    run_at("AT+CGPADDR");

    /*
     * Shows selected cellular operator.
     */
    run_at("AT+COPS?");
}


/*
 * MQTT event handler.
 *
 * Zephyr calls this function whenever an MQTT event happens.
 * Examples:
 *   - connection accepted
 *   - disconnected
 *   - publish acknowledged
 */
static void mqtt_evt_handler(struct mqtt_client *client,
                             const struct mqtt_evt *evt)
{
    switch (evt->type) {

    /*
     * MQTT_EVT_CONNACK means the broker responded to our CONNECT request.
     *
     * evt->result == 0 means the connection was accepted.
     * If nonzero, AWS rejected the connection.
     */
    case MQTT_EVT_CONNACK:
        printk("MQTT_EVT_CONNACK result: %d\n", evt->result);

        if (evt->result == 0) {
            mqtt_connected = true;
            printk("Connected to AWS IoT Core\n");
        }
        break;

    /*
     * MQTT_EVT_DISCONNECT means the MQTT connection was closed.
     *
     * This can happen because of:
     *   - network loss
     *   - TLS failure
     *   - AWS closing the connection
     *   - keepalive timeout
     */
    case MQTT_EVT_DISCONNECT:
        printk("MQTT disconnected, result: %d\n", evt->result);
        mqtt_connected = false;
        break;

    /*
     * MQTT_EVT_PUBACK means AWS acknowledged a QoS 1 publish.
     *
     * Since you publish using MQTT_QOS_1_AT_LEAST_ONCE,
     * AWS sends PUBACK after receiving the message.
     */
    case MQTT_EVT_PUBACK:
        printk("AWS received message. PUBACK id: %u\n",
               evt->param.puback.message_id);
        break;

    /*
     * Other MQTT events are ignored for now.
     */
    default:
        break;
    }
}


/*
 * Resolve AWS IoT endpoint into an IP address.
 *
 * MQTT needs a socket address, not just a hostname.
 * getaddrinfo() performs DNS lookup using the LTE network.
 */
static int broker_init(void)
{
    struct addrinfo *res;

    /*
     * DNS lookup hints.
     *
     * AF_INET means IPv4.
     * SOCK_STREAM means TCP.
     * MQTT over TLS uses TCP.
     */
    struct addrinfo hints = {
        .ai_family = AF_INET,
        .ai_socktype = SOCK_STREAM
    };

    /*
     * Resolve AWS_ENDPOINT and AWS_PORT.
     *
     * If successful, res points to the resolved address.
     */
    int err = getaddrinfo(AWS_ENDPOINT, AWS_PORT, &hints, &res);

    if (err) {
        printk("DNS lookup failed: %d\n", err);
        return err;
    }

    /*
     * Copy resolved AWS broker address into global broker variable.
     * The MQTT client will use this address during mqtt_connect().
     */
    memcpy(&broker, res->ai_addr, res->ai_addrlen);

    /*
     * Free memory allocated by getaddrinfo().
     */
    freeaddrinfo(res);

    printk("AWS endpoint resolved\n");
    return 0;
}


/*
 * Configure the Zephyr MQTT client.
 *
 * This function does not connect yet.
 * It only fills the MQTT client structure with the required settings.
 */
static void mqtt_client_setup(void)
{
    /*
     * sec_tags tells the modem which TLS credentials to use.
     *
     * TLS_SEC_TAG 42 must already be provisioned into the modem.
     */
    static sec_tag_t sec_tags[] = { TLS_SEC_TAG };

    /*
     * Initialize MQTT client structure to default values.
     */
    mqtt_client_init(&client);

    /*
     * Set the broker address.
     * broker was filled earlier by broker_init().
     */
    client.broker = &broker;

    /*
     * Register the MQTT event callback.
     * MQTT events will be passed to mqtt_evt_handler().
     */
    client.evt_cb = mqtt_evt_handler;

    /*
     * Set MQTT client ID.
     *
     * AWS uses this to identify the device.
     */
    client.client_id.utf8 = (uint8_t *)CLIENT_ID;
    client.client_id.size = strlen(CLIENT_ID);

    /*
     * AWS IoT Core does not use username/password here.
     * Authentication is done using TLS certificates.
     */
    client.password = NULL;
    client.user_name = NULL;

    /*
     * Use MQTT version 3.1.1.
     * AWS IoT Core supports this.
     */
    client.protocol_version = MQTT_VERSION_3_1_1;

    /*
     * Assign receive and transmit buffers.
     */
    client.rx_buf = rx_buffer;
    client.rx_buf_size = sizeof(rx_buffer);
    client.tx_buf = tx_buffer;
    client.tx_buf_size = sizeof(tx_buffer);

    /*
     * MQTT keepalive interval in seconds.
     *
     * If no data is sent for 60 seconds, the client sends a PINGREQ.
     * This helps keep the connection alive.
     */
    client.keepalive = 60;

    /*
     * Use secure MQTT transport.
     *
     * This means MQTT over TLS on port 8883.
     */
    client.transport.type = MQTT_TRANSPORT_SECURE;

    /*
     * Require server certificate verification.
     *
     * This verifies that the board is really talking to AWS IoT Core.
     */
    client.transport.tls.config.peer_verify = TLS_PEER_VERIFY_REQUIRED;

    /*
     * Tell the modem which stored certificates/private key to use.
     */
    client.transport.tls.config.sec_tag_list = sec_tags;
    client.transport.tls.config.sec_tag_count = 1;

    /*
     * Hostname used for TLS verification.
     *
     * This must match the AWS endpoint certificate.
     */
    client.transport.tls.config.hostname = AWS_ENDPOINT;

    /*
     * NULL means use modem/default supported cipher suites.
     */
    client.transport.tls.config.cipher_list = NULL;
}


/*
 * Process MQTT traffic.
 *
 * This must be called repeatedly.
 * MQTT is not fully automatic; the application must give the MQTT client
 * CPU time to process incoming packets and keepalive messages.
 */
static int mqtt_process(void)
{
    int err;

    /*
     * mqtt_input() reads incoming MQTT data from the socket.
     *
     * This is how CONNACK, PUBACK, DISCONNECT, etc. are processed.
     */
    err = mqtt_input(&client);

    /*
     * -EAGAIN means no data is available right now.
     * That is not a real failure.
     */
    if (err && err != -EAGAIN) {
        printk("mqtt_input error: %d\n", err);
        return err;
    }

    /*
     * mqtt_live() handles MQTT keepalive.
     *
     * It sends PINGREQ when needed.
     */
    err = mqtt_live(&client);

    if (err && err != -EAGAIN) {
        printk("mqtt_live error: %d\n", err);
        return err;
    }

    return 0;
}


/*
 * Publish one telemetry message to AWS IoT Core.
 *
 * Current payload is hardcoded for testing.
 * Later you can replace voltage and RSRP with real sensor/modem values.
 */
static int publish_to_aws(const char *payload)
{
    /*
     * MQTT message ID.
     *
     * QoS 1 messages need message IDs so PUBACK can be matched
     * to the original publish.
     */
    static uint16_t message_id = 1;

    /*
     * Payload buffer.
     *
     * This stores the JSON string that will be published.
     */
    //char payload[256];

    /*
     * Create JSON telemetry payload.
     *
     * This is the actual message AWS receives.
     */
    // snprintf(payload, sizeof(payload),"{\"device_id\":\"trap_001\",\"status\":\"active\",\"voltage\":5.72,\"rsrp\":-96}");

    /*
     * MQTT publish parameter structure.
     * Clear it first so unused fields are zero.
     */
    struct mqtt_publish_param param;
    memset(&param, 0, sizeof(param));

    /*
     * Set MQTT topic and QoS.
     *
     * QoS 1 means:
     *   - message is delivered at least once
     *   - AWS sends PUBACK after receiving it
     *   - duplicate delivery is possible if retry happens
     */
    param.message.topic.qos = MQTT_QOS_1_AT_LEAST_ONCE;
    param.message.topic.topic.utf8 = (uint8_t *)MQTT_TOPIC;
    param.message.topic.topic.size = strlen(MQTT_TOPIC);

    /*
     * Attach payload data.
     */
    param.message.payload.data = (uint8_t *)payload;
    param.message.payload.len = strlen(payload);

    /*
     * Assign unique message ID.
     */
    param.message_id = message_id++;

    /*
     * dup_flag = 0 means this is not a duplicate retransmission.
     */
    param.dup_flag = 0;

    /*
     * retain_flag = 0 means AWS should not retain this message
     * as the last known message on the topic.
     */
    param.retain_flag = 0;

    printk("Publishing to AWS: %s\n", payload);

    /*
     * Send the MQTT PUBLISH packet.
     */
    return mqtt_publish(&client, &param);
}


int main(void)
{
    int err;

    printk("Starting minimal AWS MQTT test with LTE diagnostics\n");

    /*
     * Initialize Nordic modem library.
     *
     * This must be done before using:
     *   - LTE functions
     *   - AT commands
     *   - sockets through the modem
     */
    err = nrf_modem_lib_init();

    if (err) {
        printk("nrf_modem_lib_init failed: %d\n", err);
        return 0;
    }

    printk("Modem initialized\n");

    printk("Configuring modem for Hologram...\n");

    /*
     * Set modem system mode.
     *
     * AT%XSYSTEMMODE=1,0,0,0 means:
     *   LTE-M enabled
     *   NB-IoT disabled
     *   GNSS disabled
     *   LTE preference disabled/default
     */
    run_at("AT%XSYSTEMMODE=1,0,0,0");

    /*
     * Set APN for Hologram SIM.
     *
     * PDP context 1 uses IP type and APN "hologram".
     */
    run_at("AT+CGDCONT=1,\"IP\",\"hologram\"");

    /*
     * Turn modem off.
     *
     * This forces the modem to reset radio/network state.
     */
    run_at("AT+CFUN=0");

    /*
     * Wait 2 seconds for modem state change.
     */
    k_sleep(K_SECONDS(2));

    /*
     * Turn modem back on in full functionality mode.
     */
    run_at("AT+CFUN=1");

    /*
     * Wait 5 seconds before checking LTE state.
     */
    k_sleep(K_SECONDS(5));

    printk("\nLTE checks before connect:\n");

    /*
     * Print modem/network state before LTE connection attempt.
     */
    lte_debug_checks();

    printk("\nBefore LTE connect\n");

    /*
     * Connect to LTE network.
     *
     * This blocks until connected or failure/timeout depending on config.
     */
    err = lte_lc_connect();

    printk("After LTE connect: %d\n", err);

    printk("\nLTE checks after connect:\n");

    /*
     * Print modem/network state after LTE connection attempt.
     */
    lte_debug_checks();

    /*
     * If LTE failed, stop here.
     * MQTT cannot work without cellular data connection.
     */
    if (err) {
        printk("LTE connection failed, stopping before AWS/MQTT.\n");
        return 0;
    }

    printk("LTE connected\n");

    /*
     * Resolve AWS endpoint through DNS.
     */
    err = broker_init();

    if (err) {
        printk("broker_init failed: %d\n", err);
        return 0;
    }

    /*
     * Fill MQTT client structure with AWS, TLS, buffer, and callback settings.
     */
    mqtt_client_setup();

    /*
     * Start MQTT connection to AWS.
     *
     * This sends the MQTT CONNECT packet over TLS.
     */
    err = mqtt_connect(&client);

    if (err) {
        printk("mqtt_connect failed: %d\n", err);
        return 0;
    }

    printk("mqtt_connect called\n");

    /*
     * Wait until AWS sends CONNACK.
     *
     * mqtt_connect() only starts the connection process.
     * The connection is not fully confirmed until MQTT_EVT_CONNACK is received.
     */
    while (!mqtt_connected) {

        /*
         * Process incoming MQTT packets.
         * This is needed to receive CONNACK.
         */
        err = mqtt_input(&client);

        if (err) {
            printk("mqtt_input before connected failed: %d\n", err);
            break;
        }

        /*
         * Process MQTT keepalive logic even during connection wait.
         */
        err = mqtt_live(&client);

        if (err && err != -EAGAIN) {
            printk("mqtt_live before connected failed: %d\n", err);
            break;
        }

        /*
         * Small delay to avoid busy-looping the CPU.
         */
        k_sleep(K_MSEC(100));
    }

    /*
     * Main application loop.
     *
     * Every loop:
     *   1. Publish one telemetry message
     *   2. Keep MQTT connection alive for 10 seconds
     *   3. Repeat forever
     */
    while (1) {

        // Attributes of board sent to server
        const char *id = "trap_001";
        const char *status = "active";

        /* =========================
        * SENSOR / DEVICE DATA
        * ========================= */

        // float voltage = 10.5f;   // example sensor value (can be updated dynamically)
        float voltage = 5.0f + ((float)rand() / RAND_MAX) * (8.0f - 4.0f);

        /* =========================
        * TIME DATA
        * ========================= */
       // Time variables
        int64_t unix_time_ms;
        char time_str[32];

        /* TIME CHECK */
        if (date_time_now(&unix_time_ms) == 0){
            int64_t unix_time_s = unix_time_ms / 1000;
            time_t t = unix_time_s;
            struct tm *tm_info = gmtime(&t);
            strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", tm_info);
            printk("Time OK\n");
        }
        else {
            printk("Time NOT available\n");
        }


        /* =========================
        * SIGNAL STRENGTH (RSRP)
        * ========================= */

        char rsrp_buf[64]; // buffer to hold raw RSRP value and signal quality string
        int sig_len; // length of raw RSRP string returned by modem_info_string_get
        float rsrp_read; 
        char payload[256]; // buffer for final JSON payload to be sent to AWS

        sig_len = modem_info_string_get(MODEM_INFO_RSRP, rsrp_buf, sizeof(rsrp_buf));

        if (sig_len <= 0) {
            printk("Failed to get RSRP, err = %d\n", sig_len);
            snprintf(rsrp_buf, sizeof(rsrp_buf), "Unknown");
            rsrp_read = -999.0f; // Use a sentinel value to indicate RSRP is unavailable
        } else {
            printk("RSRP raw value: %s\n", rsrp_buf);

            int rsrp_raw = atoi(rsrp_buf);
            int rsrp_dbm = rsrp_raw - 140;  // Convert raw RSRP to dBm (example conversion)
            rsrp_read = rsrp_dbm;

            //snprintf(rsrp_buf, sizeof(rsrp_buf), "%s (%d dBm)", signal, rsrp_dbm); // Update buffer to include signal quality and dBm value
        }

        /*==== PAYLOAD BEING SENT TO AWS ====*/
        snprintf(payload,
         sizeof(payload),
         "{\"device_id\":\"%s\","
         "\"status\":\"%s\","
         "\"voltage\":%.2f,"
         "\"rsrp\":%.0f,"
         "\"timestamp\":\"%s\"}",
         id,
         status,
         (double)voltage,
         (double)rsrp_read,
         time_str);



        /*
         * Send telemetry to AWS IoT Core.
         */
        err = publish_to_aws(payload);

        if (err) {
            printk("mqtt_publish failed: %d\n", err);
        }

        /*
         * For the next 10 seconds, process MQTT events once per second.
         *
         * This allows:
         *   - PUBACK messages to be received
         *   - keepalive ping logic to run
         *   - disconnect events to be detected
         */
        for (int i = 0; i < 10; i++) {
            mqtt_process();
            k_sleep(K_SECONDS(1));
        }
    }

    /*
     * This line is never reached because while(1) runs forever.
     */
    return 0;
}