import struct
import json
from threading import Thread
import serial
import socket
import logging
import modbus_tk
import modbus_tk.defines as cst
import modbus_tk.modbus_rtu as modbus_rtu
import time
import paho.mqtt.client as mqtt
from datetime import datetime
import time
logger = modbus_tk.utils.create_logger("console")


class MqttAdapter(Thread):
    is_connected = False
    broker = "10.1.1.2"
    port = 1883
    topic = "/home/egemeric/DELL_PC/ac_meter"
    client_id = socket.gethostname()

    def __init__(self):
        Thread.__init__(self)
        self.cli = mqtt.Client(self.client_id)
        self.cli.on_connect = self.on_connect
        self.cli.on_message = self.on_message

    def on_connect(self, client, data, flags, rc):
        if rc == 0:
            print("CONNECTED TO MQTT BROKER")
            self.is_connected = True
        else:
            print("FAILED TO COONECY MQTT BROKER")
            self.is_connected = False

    def on_message(self, client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
    
    def publish_msg(self, message):
        print(message)
        if self.is_connected:
            self.cli.publish(self.topic, message)
        else:
            print("mqtt connection is not ready.")

    def run(self):
        self.cli.connect(self.broker, self.port)
        self.cli.loop_forever()


class Pzem004PowerMeter(Thread):
    data = None
    last_update = None
    mqtt_adapter = None

    def __init__(self, serial_port="/dev/ttyUSB0", baudrate=9600, bytesize=8, parity='N', stopbits=1):
        Thread.__init__(self)
        self.master = modbus_rtu.RtuMaster(serial.Serial(
            port=serial_port, baudrate=baudrate, bytesize=bytesize, parity=parity, stopbits=stopbits))
        self.master.set_timeout(2.0)
        self. master.set_verbose(True)
        self.mqtt_adapter = MqttAdapter()
        self.mqtt_adapter.start()

    def run(self):
        while(1):
            self.read()
            if self.data:
                data_js = json.dumps(self.data)
            else:
                print("cannot publish data is none")
            self.mqtt_adapter.publish_msg(data_js)
            time.sleep(1)

    def read(self):
        self.data = self._read()
        logger.debug(self.data)

    def _read(self):
        raw_data = None
        try:
            raw_data = self.master.execute(
                0x01, 0x04, 0, 10,  data_format=(">20B"))
            raw_data=self._parse_data(raw_data)
            return self._serialize_data(raw_data)
        except Exception as e:
            logger.error(e)
            return None

    def _parse_data(self, data):
        data = struct.unpack(">HiiiHHH", bytearray([*data[0:2], *data[4:6], *data[2:4], *data[8:10],
                                                    *data[6:8], *data[12:14], *data[10:12], *data[14:16], *data[16:18], *data[18:20]]))
        return data

    def _serialize_data(self, data):
        return {'voltage': data[0] * 0.1, "current": data[1] * 0.001, "power": data[2] * 0.1, "energy_total": data[3] * 1, "grid_freq": data[4] * 0.1}


if __name__ == '__main__':
    power_meter = Pzem004PowerMeter()
    power_meter.start()
    power_meter.join()
