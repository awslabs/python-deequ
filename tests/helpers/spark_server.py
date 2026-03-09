# Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Spark Connect server management for tests.

This module provides utilities to start and manage a Spark Connect server
for running integration tests that require Spark.
"""

import os
import socket
import subprocess
import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class SparkServerConfig:
    """Configuration for Spark Connect server."""

    java_home: str = field(
        default_factory=lambda: os.environ.get(
            "JAVA_HOME",
            "/Library/Java/JavaVirtualMachines/amazon-corretto-17.jdk/Contents/Home",
        )
    )
    spark_home: str = field(
        default_factory=lambda: os.environ.get(
            "SPARK_HOME", "/Volumes/workplace/deequ_rewrite/spark-3.5.0-bin-hadoop3"
        )
    )
    port: int = 15002
    startup_timeout: int = 60
    poll_interval: float = 1.0
    driver_memory: str = "4g"
    executor_memory: str = "4g"
    deequ_jar: str = field(
        default_factory=lambda: os.environ.get(
            "DEEQU_JAR",
            "/Volumes/workplace/deequ_rewrite/deequ/target/deequ_2.12-2.1.0b-spark-3.5.jar"
        )
    )


class SparkConnectServer:
    """Manages Spark Connect server lifecycle for tests."""

    def __init__(self, config: Optional[SparkServerConfig] = None):
        """
        Initialize Spark Connect server manager.

        Args:
            config: Server configuration (uses defaults if not provided)
        """
        self.config = config or SparkServerConfig()
        self._process: Optional[subprocess.Popen] = None
        self._started_by_us = False

    def is_running(self) -> bool:
        """Check if Spark Connect server is running by attempting to connect."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex(("localhost", self.config.port))
            sock.close()
            return result == 0
        except (socket.error, OSError):
            return False

    def start(self) -> float:
        """
        Start Spark Connect server if not already running.

        Returns:
            Time taken to start the server (0 if already running)

        Raises:
            RuntimeError: If server fails to start within timeout
        """
        if self.is_running():
            print(f"Spark Connect server already running on port {self.config.port}")
            return 0.0

        start_time = time.time()

        # Build the startup command
        start_script = os.path.join(self.config.spark_home, "sbin", "start-connect-server.sh")

        if not os.path.exists(start_script):
            raise RuntimeError(f"Spark Connect start script not found: {start_script}")

        cmd = [
            start_script,
            "--conf", f"spark.driver.memory={self.config.driver_memory}",
            "--conf", f"spark.executor.memory={self.config.executor_memory}",
            "--packages", "org.apache.spark:spark-connect_2.12:3.5.0",
            "--jars", self.config.deequ_jar,
            "--conf", "spark.connect.extensions.relation.classes=com.amazon.deequ.connect.DeequRelationPlugin",
        ]

        # Set up environment
        env = os.environ.copy()
        env["JAVA_HOME"] = self.config.java_home
        env["SPARK_HOME"] = self.config.spark_home

        print(f"Starting Spark Connect server on port {self.config.port}...")
        print(f"  JAVA_HOME: {self.config.java_home}")
        print(f"  SPARK_HOME: {self.config.spark_home}")

        # Start the server
        self._process = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self._started_by_us = True

        # Wait for server to be ready
        deadline = time.time() + self.config.startup_timeout
        while time.time() < deadline:
            if self.is_running():
                elapsed = time.time() - start_time
                print(f"Spark Connect server started in {elapsed:.1f}s")
                return elapsed
            time.sleep(self.config.poll_interval)

        # Timeout - try to get error output
        if self._process:
            self._process.terminate()
            _, stderr = self._process.communicate(timeout=5)
            error_msg = stderr.decode() if stderr else "Unknown error"
            self._process = None
            self._started_by_us = False
            raise RuntimeError(
                f"Spark Connect server failed to start within {self.config.startup_timeout}s: {error_msg[:500]}"
            )

        raise RuntimeError(
            f"Spark Connect server failed to start within {self.config.startup_timeout}s"
        )

    def stop(self) -> None:
        """Stop Spark Connect server if we started it."""
        if not self._started_by_us:
            print("Spark Connect server was not started by us, skipping stop")
            return

        stop_script = os.path.join(self.config.spark_home, "sbin", "stop-connect-server.sh")

        if os.path.exists(stop_script):
            print("Stopping Spark Connect server...")
            env = os.environ.copy()
            env["JAVA_HOME"] = self.config.java_home
            env["SPARK_HOME"] = self.config.spark_home

            try:
                subprocess.run(
                    [stop_script],
                    env=env,
                    timeout=30,
                    capture_output=True,
                )
                print("Spark Connect server stopped")
            except subprocess.TimeoutExpired:
                print("Warning: stop script timed out")
            except Exception as e:
                print(f"Warning: Error stopping server: {e}")
        else:
            # Fall back to killing the process directly
            if self._process:
                print("Terminating Spark Connect server process...")
                self._process.terminate()
                try:
                    self._process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self._process.kill()
                print("Spark Connect server process terminated")

        self._started_by_us = False
        self._process = None
