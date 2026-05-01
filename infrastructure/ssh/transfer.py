"""
SFTP transfer client — only SSH/file-transfer logic lives here.
"""
import os
import socket
import time
from pathlib import Path
from typing import Any, Dict

import paramiko
import structlog

logger = structlog.get_logger(__name__)


class SSHTransferClient:
    def __init__(
        self,
        host: str,
        user: str,
        key_path: str = "",
        password: str = "",
        timeout: int = 30,) -> None:
        self.host = host
        self.user = user
        self.key_path = key_path
        self.password = password
        self.timeout = timeout

    def test_connectivity(self) -> Dict[str, Any]:
        """
        Run DNS + port + auth checks and return a structured report.
        """
        results: Dict[str, Any] = {"host": self.host, "username": self.user, "tests": []}

        # DNS
        try:
            socket.gethostbyname(self.host)
            results["tests"].append({"name": "DNS resolution", "status": "OK"})
        except socket.gaierror as exc:
            results["tests"].append({"name": "DNS resolution", "status": "FAILED", "error": str(exc)})
            return results

        # Port 22
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        try:
            rc = sock.connect_ex((self.host, 22))
            if rc == 0:
                results["tests"].append({"name": "SSH port 22", "status": "OK"})
            else:
                results["tests"].append({"name": "SSH port 22", "status": "FAILED", "error": "Port not reachable"})
                return results
        finally:
            sock.close()

        # Auth
        ssh = self._make_client()
        try:
            self._connect(ssh)
            results["tests"].append({"name": "SSH authentication", "status": "OK"})
            sftp = ssh.open_sftp()
            sftp.close()
            results["tests"].append({"name": "SFTP access", "status": "OK"})
        except Exception as exc:
            results["tests"].append({"name": "SSH authentication", "status": "FAILED", "error": str(exc)})
        finally:
            ssh.close()

        return results

    def upload_directory(self, local_dir: Path, remote_base: str) -> Dict[str, Any]:
        """
        Upload all files in local_dir to remote_base/<local_dir.name>/.
        """
        files = [f for f in local_dir.glob("*") if f.is_file()]
        if not files:
            return {"status": "skipped", "message": "No files to transfer"}

        remote_path = f"{remote_base}/{local_dir.name}"
        ssh = self._make_client()
        self._connect(ssh)
        try:
            ssh.exec_command(f"mkdir -p {remote_path}")[1].channel.recv_exit_status()
            sftp = ssh.open_sftp()
            transferred = 0
            failed = 0
            for file in files:
                dest = f"{remote_path}/{file.name}"
                try:
                    sftp.put(str(file), dest)
                    if file.stat().st_size == sftp.stat(dest).st_size:
                        transferred += 1
                        logger.info("sftp_uploaded", file=file.name, dest=dest)
                    else:
                        failed += 1
                        logger.error("sftp_integrity_failed", file=file.name)
                except Exception as exc:
                    failed += 1
                    logger.error("sftp_upload_failed", file=file.name, error=str(exc))
            sftp.close()
        finally:
            ssh.close()

        status = "success" if failed == 0 else ("partial" if transferred > 0 else "error")
        return {
            "status": status,
            "files_transferred": transferred,
            "files_failed": failed,
            "total_files": len(files),
            "remote_path": remote_path,
        }

    def _make_client(self) -> paramiko.SSHClient:
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        return ssh

    def _connect(self, ssh: paramiko.SSHClient, attempts: int = 3) -> None:
        for i in range(attempts):
            try:
                kwargs: Dict[str, Any] = {
                    "hostname": self.host,
                    "username": self.user,
                    "timeout": self.timeout,
                    "look_for_keys": False,
                    "allow_agent": False,
                }
                if self.key_path and os.path.exists(self.key_path):
                    kwargs["key_filename"] = self.key_path
                elif self.password:
                    kwargs["password"] = self.password
                ssh.connect(**kwargs)
                return
            except Exception as exc:
                logger.warning("ssh_connect_failed", attempt=i + 1, max_attempts=attempts, error=str(exc))
                if i < attempts - 1:
                    time.sleep(3)
        raise RuntimeError(f"SSH connection to {self.host} failed after {attempts} attempts")
