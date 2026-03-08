import yaml
from datetime import datetime, timedelta
from pathlib import Path
import logging
import socket



class SyncManager:
    """
    SyncManager class to handle synchronization logic
    """
    def __init__(self, config_file: str = "TSDB.yml"):
        # Setup logging
        self.logger = logging.getLogger(__name__)
        try:
            # Load yaml configuration file
            with open(config_file) as f:
                self.config = yaml.safe_load(f)
        except FileNotFoundError:
            self.logger.error(f"Error: Configuration file '{config_file}' not found.")
            self.config = {}
        except yaml.YAMLError as e:
            self.logger.error(f"Error parsing YAML file: {e}")
            self.config = {}
        self.QUESTDB_QUERY_URL = f"http://{self.config['questdb']['host']}:{self.config['questdb']['port']}/exec"  
        self.BATCH_SIZE = self.config.get('sissa', {}).get('batch_size', 50) 
        # logging.basicConfig(level=logging.INFO, 
        #             format='%(asctime)s - %(name)s - %(levelname)s - %(message)s') 
        
    def sync_to_remote_ssh(self, local_dir: Path, sync_id: str):
        """
        Sync files to remote computer via SSH/SFTP with password authentication
        """
        try:
            import paramiko
        except ImportError:
            self.logger.error("paramiko not installed")
            return {"status": "error", "error": "paramiko not installed"}
        
        # Get SSH config
        ssh_config = self.config.get('remote_sync', {}).get('ssh', {})
        remote_host = ssh_config.get('host', '')
        remote_user = ssh_config.get('username', '')
        remote_path = ssh_config.get('path', '/tmp/synced_data')
        password = ssh_config.get('password', '')
        
        if not remote_host or not remote_user or not password:
            missing = []
            if not remote_host: missing.append("host")
            if not remote_user: missing.append("username") 
            if not password: missing.append("password")
            return {
                "status": "error", 
                "error": f"SSH configuration incomplete - missing: {', '.join(missing)}"
            }
        
        self.logger.info(f"Starting SSH sync to {remote_user}@{remote_host}:{remote_path}/{sync_id}")
        
        ssh = None
        sftp = None
        
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            # Connect with password only
            self.logger.info(f"Connecting to {remote_host} with password authentication...")
            ssh.connect(
                hostname=remote_host,
                username=remote_user,
                password=password,
                timeout=30,
                allow_agent=False,
                look_for_keys=False,
                auth_timeout=30
            )
            self.logger.info(f"SSH connection established to {remote_host}")
            
            # Open SFTP session
            sftp = ssh.open_sftp()
            
            # First, ensure the base directory exists
            try:
                sftp.stat(remote_path)
                self.logger.info(f"Base directory exists: {remote_path}")
            except FileNotFoundError:
                # Create base directory if it doesn't exist
                try:
                    sftp.mkdir(remote_path)
                    self.logger.info(f"Created base directory: {remote_path}")
                except Exception as e:
                    self.logger.error(f"Failed to create base directory: {e}")
                    return {
                        "status": "error",
                        "error": f"Failed to create base directory {remote_path}: {str(e)}"
                    }
            
            # Create sync-specific subdirectory
            remote_sync_path = f"{remote_path}/{sync_id}"
            
            # Try to create the sync directory
            try:
                sftp.mkdir(remote_sync_path)
                self.logger.info(f"Created sync directory: {remote_sync_path}")
            except IOError as e:
                # Directory might already exist (from a previous sync with same ID - unlikely)
                if "File exists" in str(e):
                    self.logger.info(f"Sync directory already exists (will use it): {remote_sync_path}")
                else:
                    self.logger.error(f"Failed to create sync directory: {e}")
                    return {
                        "status": "error",
                        "error": f"Failed to create sync directory: {str(e)}"
                    }
            
            # Transfer files
            transferred_files = []
            failed_files = []
            
            for file_path in local_dir.rglob("*"):
                if file_path.is_file():
                    relative_path = file_path.relative_to(local_dir)
                    remote_file = f"{remote_sync_path}/{relative_path}"
                    
                    # Create subdirectories if needed
                    remote_dir = str(Path(remote_file).parent)
                    if remote_dir != remote_sync_path:
                        try:
                            sftp.stat(remote_dir)
                        except FileNotFoundError:
                            # Create subdirectory
                            try:
                                sftp.mkdir(remote_dir)
                                self.logger.debug(f"Created subdirectory: {remote_dir}")
                            except Exception as e:
                                self.logger.error(f"Failed to create subdirectory {remote_dir}: {e}")
                                failed_files.append(str(relative_path))
                                continue
                    
                    try:
                        # Transfer file
                        self.logger.debug(f"Transferring: {relative_path} -> {remote_file}")
                        sftp.put(str(file_path), remote_file)
                        transferred_files.append(str(relative_path))
                        self.logger.debug(f"Transferred: {relative_path}")
                    except Exception as e:
                        self.logger.error(f"Failed to transfer {relative_path}: {e}")
                        failed_files.append(str(relative_path))
            
            self.logger.info(f"SFTP sync completed: {len(transferred_files)} files transferred")
            
            # Also create a manifest file with sync info
            manifest = {
                "sync_id": sync_id,
                "timestamp": datetime.now().isoformat(),
                "source_host": socket.gethostname(),
                "files": transferred_files,
                "total_files": len(transferred_files)
            }
            
            manifest_file = f"{remote_sync_path}/sync_manifest.json"
            try:
                with sftp.open(manifest_file, 'w') as f:
                    import json
                    f.write(json.dumps(manifest, indent=2))
                self.logger.info(f"Created manifest: {manifest_file}")
            except Exception as e:
                self.logger.warning(f"Could not create manifest: {e}")
            
            return {
                "status": "success" if not failed_files else "partial",
                "method": "ssh",
                "host": remote_host,
                "path": remote_sync_path,
                "files_transferred": len(transferred_files),
                "files_failed": len(failed_files),
                "transferred_files": transferred_files[:5]
            }
            
        except paramiko.AuthenticationException as e:
            self.logger.error(f"SSH authentication failed: {e}")
            return {
                "status": "error",
                "method": "ssh",
                "error": f"Authentication failed: {str(e)}. Check your password."
            }
        except paramiko.SSHException as e:
            self.logger.error(f"SSH connection failed: {e}")
            return {
                "status": "error",
                "method": "ssh",
                "error": f"SSH connection failed: {str(e)}"
            }
        except Exception as e:
            self.logger.error(f"SSH sync error: {e}", exc_info=True)
            return {
                "status": "error",
                "method": "ssh",
                "error": str(e)
            }
        finally:
            if sftp:
                sftp.close()
            if ssh:
                ssh.close()
                self.logger.info("SSH connection closed")  
    
    def load_remote_config(self):
        """
        Load remote sync configuration
        """
        #global REMOTE_SYNC_ENABLED, REMOTE_API_ENDPOINT, REMOTE_API_TOKEN, REMOTE_SYNC_METHOD
        
        try:
            if 'remote_sync' in self.config:
                remote_config = self.config['remote_sync']
                self.REMOTE_SYNC_ENABLED = remote_config.get('enabled', False)
                self.REMOTE_API_ENDPOINT = remote_config.get('api_endpoint', '')
                self.REMOTE_API_TOKEN = remote_config.get('api_token', '')
                self.REMOTE_SYNC_METHOD = remote_config.get('method', 'api')

                if self.REMOTE_SYNC_ENABLED:
                    self.logger.info(f"Remote sync enabled via {self.REMOTE_SYNC_METHOD} to {self.REMOTE_API_ENDPOINT}")
        except Exception as e:
            self.logger.error(f"Error loading remote config: {e}")                             





# async def sync_to_remote_api(records: List[Dict], sync_id: str):
#     """
#     Sync data to remote computer via REST API
#     """
#     if not REMOTE_SYNC_ENABLED or not REMOTE_API_ENDPOINT:
#         logger.warning("Remote API sync not configured")
#         return {"status": "disabled", "method": "api"}
    
#     payload = {
#         "sync_id": sync_id,
#         "timestamp": datetime.now().isoformat(),
#         "source": "hydrogen_data_sync",
#         "record_count": len(records),
#         "records": records
#     }
    
#     headers = {
#         "Content-Type": "application/json",
#         "User-Agent": "HydrogenDataSync/1.0"
#     }
    
#     if REMOTE_API_TOKEN:
#         headers["Authorization"] = f"Bearer {REMOTE_API_TOKEN}"
    
#     try:
#         timeout = httpx.Timeout(30.0, connect=10.0)
#         async with httpx.AsyncClient(timeout=timeout) as client:
#             response = await client.post(
#                 REMOTE_API_ENDPOINT,
#                 json=payload,
#                 headers=headers
#             )
            
#             if response.status_code in (200, 201):
#                 logger.info(f"Remote API sync successful: {response.json()}")
#                 return {
#                     "status": "success",
#                     "method": "api",
#                     "endpoint": REMOTE_API_ENDPOINT,
#                     "response": response.json()
#                 }
#             else:
#                 logger.error(f"Remote API sync failed: {response.status_code} - {response.text}")
#                 return {
#                     "status": "error",
#                     "method": "api",
#                     "endpoint": REMOTE_API_ENDPOINT,
#                     "error": f"{response.status_code}: {response.text}"
#                 }
                
#     except httpx.ConnectError:
#         logger.error(f"Cannot connect to remote API: {REMOTE_API_ENDPOINT}")
#         return {
#             "status": "error",
#             "method": "api",
#             "endpoint": REMOTE_API_ENDPOINT,
#             "error": "Connection failed"
#         }
#     except Exception as e:
#         logger.error(f"Remote API sync error: {e}")
#         return {
#             "status": "error",
#             "method": "api",
#             "endpoint": REMOTE_API_ENDPOINT,
#             "error": str(e)
#         }
        


# def sync_to_remote_ssh(local_dir: Path, sync_id: str):
#     """Sync files to remote computer via SSH/SFTP with password authentication"""
#     try:
#         import paramiko
#     except ImportError:
#         logger.error("paramiko not installed")
#         return {"status": "error", "error": "paramiko not installed"}
    
#     # Get SSH config
#     ssh_config = config.get('remote_sync', {}).get('ssh', {})
#     remote_host = ssh_config.get('host', '')
#     remote_user = ssh_config.get('username', '')
#     remote_path = ssh_config.get('path', '/tmp/synced_data')
#     password = ssh_config.get('password', '')
    
#     if not remote_host or not remote_user or not password:
#         missing = []
#         if not remote_host: missing.append("host")
#         if not remote_user: missing.append("username") 
#         if not password: missing.append("password")
#         return {
#             "status": "error", 
#             "error": f"SSH configuration incomplete - missing: {', '.join(missing)}"
#         }
    
#     logger.info(f"Starting SSH sync to {remote_user}@{remote_host}:{remote_path}/{sync_id}")
    
#     ssh = None
#     sftp = None
    
#     try:
#         ssh = paramiko.SSHClient()
#         ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
#         # Connect with password only - disable all other auth methods
#         logger.info(f"Connecting to {remote_host} with password authentication...")
#         ssh.connect(
#             hostname=remote_host,
#             username=remote_user,
#             password=password,
#             timeout=30,
#             allow_agent=False,  # Don't use SSH agent
#             look_for_keys=False,  # Don't look for keys
#             auth_timeout=30
#         )
#         logger.info(f"SSH connection established to {remote_host}")
        
#         # Open SFTP session
#         sftp = ssh.open_sftp()
        
#         # Create remote directory structure
#         remote_sync_path = f"{remote_path}/{sync_id}"
        
#         # Create directories recursively
#         path_parts = remote_sync_path.split('/')
#         current_path = ''
#         for part in path_parts:
#             if part:
#                 current_path += '/' + part
#                 try:
#                     sftp.stat(current_path)
#                     logger.debug(f"Directory exists: {current_path}")
#                 except FileNotFoundError:
#                     sftp.mkdir(current_path)
#                     logger.debug(f"Created directory: {current_path}")
        
#         logger.info(f"Created/verified remote directory: {remote_sync_path}")
        
#         # Transfer files
#         transferred_files = []
#         failed_files = []
        
#         for file_path in local_dir.rglob("*"):
#             if file_path.is_file():
#                 relative_path = file_path.relative_to(local_dir)
#                 remote_file = f"{remote_sync_path}/{relative_path}"
                
#                 # Create subdirectories if needed
#                 remote_dir = str(Path(remote_file).parent)
#                 if remote_dir != remote_sync_path:
#                     try:
#                         sftp.stat(remote_dir)
#                     except FileNotFoundError:
#                         # Create subdirectory
#                         sub_parts = Path(relative_path).parent.parts
#                         current_sub = remote_sync_path
#                         for part in sub_parts:
#                             current_sub += '/' + part
#                             try:
#                                 sftp.stat(current_sub)
#                             except FileNotFoundError:
#                                 sftp.mkdir(current_sub)
#                                 logger.debug(f"Created subdirectory: {current_sub}")
                
#                 try:
#                     # Transfer file
#                     logger.debug(f"Transferring: {relative_path} -> {remote_file}")
#                     sftp.put(str(file_path), remote_file)
#                     transferred_files.append(str(relative_path))
#                     logger.debug(f"Transferred: {relative_path}")
#                 except Exception as e:
#                     logger.error(f"Failed to transfer {relative_path}: {e}")
#                     failed_files.append(str(relative_path))
        
#         logger.info(f"SFTP sync completed: {len(transferred_files)} files transferred")
        
#         return {
#             "status": "success" if not failed_files else "partial",
#             "method": "ssh",
#             "host": remote_host,
#             "path": remote_sync_path,
#             "files_transferred": len(transferred_files),
#             "files_failed": len(failed_files),
#             "transferred_files": transferred_files[:5]
#         }
        
#     except paramiko.AuthenticationException as e:
#         logger.error(f"SSH authentication failed: {e}")
#         return {
#             "status": "error",
#             "method": "ssh",
#             "error": f"Authentication failed: {str(e)}. Check your password."
#         }
#     except paramiko.SSHException as e:
#         logger.error(f"SSH connection failed: {e}")
#         return {
#             "status": "error",
#             "method": "ssh",
#             "error": f"SSH connection failed: {str(e)}"
#         }
#     except Exception as e:
#         logger.error(f"SSH sync error: {e}", exc_info=True)
#         return {
#             "status": "error",
#             "method": "ssh",
#             "error": str(e)
#         }
#     finally:
#         if sftp:
#             sftp.close()
#         if ssh:
#             ssh.close()
#             logger.info("SSH connection closed")
            
            
            


# @app.get("/api/remote-sync/status")
# async def get_remote_sync_status():
#     """
#     Get remote sync configuration and status
#     """
#     return {
#         "enabled": REMOTE_SYNC_ENABLED,
#         "method": REMOTE_SYNC_METHOD,
#         "api_endpoint": REMOTE_API_ENDPOINT,
#         "configured": bool(REMOTE_API_ENDPOINT)
#     }




# @app.post("/api/debug/test-ssh")
# async def test_ssh_connection():
#     """Test SSH connection to remote host with password authentication"""
    
#     ssh_config = config.get('remote_sync', {}).get('ssh', {})
#     remote_host = ssh_config.get('host', '')
#     remote_user = ssh_config.get('username', '')
#     remote_path = ssh_config.get('path', '')
#     password = ssh_config.get('password', '')
    
#     if not all([remote_host, remote_user, password]):
#         return {
#             "status": "error",
#             "message": "SSH configuration incomplete",
#             "required_fields": ["host", "username", "password"],
#             "current_config": {
#                 "host": remote_host or "MISSING",
#                 "username": remote_user or "MISSING",
#                 "has_password": bool(password)
#             }
#         }
    
#     # Test results
#     results = {
#         "host": remote_host,
#         "username": remote_user,
#         "path": remote_path,
#         "authentication_method": "password",
#         "tests": []
#     }
    
#     import socket
    
#     # Test 1: DNS resolution
#     try:
#         socket.gethostbyname(remote_host)
#         results["tests"].append({"name": "DNS resolution", "status": "OK"})
#     except socket.gaierror:
#         results["tests"].append({
#             "name": "DNS resolution", 
#             "status": "FAILED",
#             "error": f"Cannot resolve hostname: {remote_host}"
#         })
#         return results
    
#     # Test 2: Port 22 connectivity
#     sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     sock.settimeout(5)
#     try:
#         result = sock.connect_ex((remote_host, 22))
#         if result == 0:
#             results["tests"].append({"name": "SSH port (22) connectivity", "status": "OK"})
#         else:
#             results["tests"].append({
#                 "name": "SSH port (22) connectivity",
#                 "status": "FAILED",
#                 "error": f"Port 22 is not reachable (error code: {result})"
#             })
#             sock.close()
#             return results
#         sock.close()
#     except Exception as e:
#         results["tests"].append({
#             "name": "SSH port (22) connectivity",
#             "status": "FAILED",
#             "error": str(e)
#         })
#         return results
    
#     # Test 3: SSH authentication with password
#     ssh = paramiko.SSHClient()
#     ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
#     try:
#         # Connect with password only - disable all other auth methods
#         ssh.connect(
#             hostname=remote_host,
#             username=remote_user,
#             password=password,
#             timeout=10,
#             allow_agent=False,  # Don't use SSH agent
#             look_for_keys=False,  # Don't look for keys
#             auth_timeout=10
#         )
        
#         results["tests"].append({
#             "name": "SSH authentication", 
#             "status": "OK",
#             "message": "Successfully connected with password"
#         })
        
#         # Test 4: Directory creation
#         try:
#             sftp = ssh.open_sftp()
            
#             # Try to create directory
#             try:
#                 sftp.mkdir(remote_path)
#                 results["tests"].append({
#                     "name": "Directory creation", 
#                     "status": "OK",
#                     "message": f"Created directory: {remote_path}"
#                 })
#             except IOError as e:
#                 if "File exists" in str(e):
#                     results["tests"].append({
#                         "name": "Directory creation", 
#                         "status": "OK",
#                         "message": f"Directory already exists: {remote_path}"
#                     })
#                 else:
#                     results["tests"].append({
#                         "name": "Directory creation",
#                         "status": "FAILED",
#                         "error": str(e)
#                     })
            
#             # Test 5: Write permissions
#             test_file = f"{remote_path}/test_write_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
#             try:
#                 with sftp.open(test_file, 'w') as f:
#                     f.write("Test write permission\n")
                
#                 # Verify file exists
#                 sftp.stat(test_file)
                
#                 # Clean up
#                 sftp.remove(test_file)
                
#                 results["tests"].append({
#                     "name": "Write permissions", 
#                     "status": "OK",
#                     "message": f"Successfully wrote and removed test file"
#                 })
#             except Exception as e:
#                 results["tests"].append({
#                     "name": "Write permissions",
#                     "status": "FAILED",
#                     "error": f"Cannot write to directory: {str(e)}"
#                 })
            
#             sftp.close()
            
#         except Exception as e:
#             results["tests"].append({
#                 "name": "SFTP operations",
#                 "status": "FAILED",
#                 "error": str(e)
#             })
        
#         ssh.close()
        
#     except paramiko.AuthenticationException as e:
#         results["tests"].append({
#             "name": "SSH authentication",
#             "status": "FAILED",
#             "error": f"Authentication failed: {str(e)}. Check your password."
#         })
#     except paramiko.SSHException as e:
#         results["tests"].append({
#             "name": "SSH connection",
#             "status": "FAILED",
#             "error": f"SSH protocol error: {str(e)}"
#         })
#     except Exception as e:
#         results["tests"].append({
#             "name": "SSH connection",
#             "status": "FAILED",
#             "error": str(e)
#         })
    
#     return results


# if __name__ == "__main__":
#     import uvicorn
    
#     print("\n" + "="*60)
#     print("HYDROGEN DATA SYNC SERVICE - WITH REMOTE SYNC")
#     print("="*60)
#     print(f"Local data directory: {SYNC_DATA_DIR.absolute()}")
#     print(f"QuestDB URL: {QUESTDB_QUERY_URL}")
    
#     if SyncConfig.REMOTE_SYNC_ENABLED:
#         print(f"Remote sync: ENABLED via {SyncConfig.REMOTE_SYNC_METHOD}")
#         if SyncConfig.REMOTE_API_ENDPOINT:
#             print(f"Remote endpoint: {SyncConfig.REMOTE_API_ENDPOINT}")
#     else:
#         print("Remote sync: DISABLED")
    
#     print(f"\nServer starting on: http://localhost:8080")
#     print("="*60 + "\n")
    
#     uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")