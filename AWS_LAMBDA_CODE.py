import os
import json
import pg8000
import boto3
from datetime import datetime

DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]

IOT_DATA_ENDPOINT = os.environ["IOT_DATA_ENDPOINT"]

iot_client = boto3.client(
    "iot-data",
    endpoint_url=f"https://{IOT_DATA_ENDPOINT}"
)


def lambda_handler(event, context):

    print("Received event:", event)

    trap_id = event.get("device_id")
    status = event.get("status")
    voltage = event.get("voltage")
    rsrp = event.get("rsrp")
    timestamp_str = event.get("timestamp")

    #
    # Current UTC time from AWS Lambda.
    # Schedule lookup will use this time,
    # not the board timestamp.
    #
    utc_now = datetime.utcnow()
    current_time = utc_now.time()

    #
    # Still save the board timestamp in device_logs
    #
    if timestamp_str:
        device_time = datetime.strptime(
            timestamp_str,
            "%Y-%m-%d %H:%M:%S"
        )
    else:
        device_time = utc_now

    command_status = "deactive"

    conn = None

    try:

        conn = pg8000.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            ssl_context=True
        )

        cur = conn.cursor()

        cur.execute(
            """
            INSERT INTO device_logs
            (trap_id, status, voltage, rsrp, timestamp)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                trap_id,
                status,
                voltage,
                rsrp,
                device_time
            )
        )

        cur.execute(
            """
            SELECT status
            FROM trap_schedule
            WHERE trap_id = %s
              AND start_time <= %s
              AND end_time >= %s
            LIMIT 1
            """,
            (
                trap_id,
                current_time,
                current_time
            )
        )

        row = cur.fetchone()

        if row:

            

            if row[0] == "Activated":
                command_status = "active"

            elif row[0] == "De-activated":
                command_status = "deactive"

            else:
                command_status = "deactive"

        else:
            command_status = "deactive"

        conn.commit()
        cur.close()

        command_topic = f"traps/{trap_id}/command"

        iot_client.publish(
            topic=command_topic,
            qos=1,
            payload=command_status
        )

        print("UTC time used for schedule lookup:", current_time)
        print("Inserted telemetry into RDS")
        print(
            f"Published command '{command_status}' "
            f"to topic '{command_topic}'"
        )

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Telemetry inserted and command published",
                "trap_id": trap_id,
                "command": command_status,
                "utc_time_used": str(current_time)
            })
        }

    except Exception as e:

        print("ERROR:", str(e))

        return {
            "statusCode": 500,
            "body": str(e)
        }

    finally:

        if conn:
            conn.close()