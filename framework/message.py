#!/usr/bin/env python3

from dataclasses import dataclass

START = "START"
SHUTDOWN = "SHUTDOWN"

# Control port is used to send the above commands to the controller
CONTROL_PORT = "60001"
STATUS_PORT = "60002"
