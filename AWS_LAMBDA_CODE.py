import os
import pg8000
from datetime import datetime


DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]


def lambda_handler(event, context):
    print("Received event:", event)

    trap_id = event.get("device_id")
    status = event.get("status")
    voltage = event.get("voltage")
    rsrp = event.get("rsrp")
    timestamp_str = event.get("timestamp")

    device_time = None
    if timestamp_str:
        device_time = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")

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

        conn.commit()
        cur.close()

        print("Inserted telemetry into RDS")

        return {
            "statusCode": 200,
            "body": "Telemetry inserted into RDS"
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