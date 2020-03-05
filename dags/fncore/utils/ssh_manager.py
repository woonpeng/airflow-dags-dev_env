# coding=utf-8
"""
This module defines the helper functions to interact with a ssh server
"""
# pylint: disable=unused-argument
from contextlib import contextmanager

import paramiko
import scp

from fncore.utils.connstr import decode_url_connection_string


class TempDirException(IOError):
    """
    Exception in creating temporary folder
    """
    pass


@contextmanager
def get_ssh_session(server_uri, port=22, timeout=60*30, username=None):
    """
    Returns a ssh session and closes the session when out of context

    :param server_uri: Connection string specifying the server to ssh into
    :type server_uri: str
    :param port: Port number to use to connect to the ssh server. If specified,
                 override that specified in the connection string [Optional]
    :type port: int
    :param timeout: Timeout (in seconds) to use in connecting to the ssh server
    :type timeout: float
    :param username: Username to use to connect to the ssh server. If specified,
                     override that specified in the connection string. It is
                     assumed that the necessary private-public keys have been
                     set up properly between the ssh and host server [Optional]
    :type username: str
    """
    # Decode server uri
    uri = decode_url_connection_string(server_uri)

    # Start a new session
    session = paramiko.SSHClient()

    # Set auto add policy for host
    session.load_system_host_keys()
    session.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # Set default port and username
    if username is None:
        username = uri['username']
    if port is None:
        port = uri['port']

    # Connect to the Neo4j server
    session.connect(uri['hostname'], username=username, port=port,
                    auth_timeout=timeout)

    # Returns the session
    yield session

    # Close the session
    session.close()


@contextmanager
def use_temporary_file(session, srcpath, timeout=60*30, **kwargs):
    """
    Upload a file to a ssh server for temporary use and delete the file when
    out of context

    :param session: SSH session returned by get_ssh_session
    :type session: paramiko.client.SSHClient
    :param srcpath: Full path to the file on the host server to transfer to
                    the ssh server
    :type srcpath: str
    :param timeout: Timeout (in seconds) to use in connecting to the ssh server
    :type timeout: float
    :param kwargs: Additional arguments to pass to SCP for uploading the file
    """

    # Create a temporary folder in destination
    retcode, out, _ = run_script(session, 'mktemp -d', timeout=timeout)

    if retcode != 0 or len(out) < 1:
        raise TempDirException("Unable to create temporary files")
    tempdir = out[-1].strip()

    # Upload file to temporary folder
    upload_file(session, srcpath, tempdir, timeout=timeout, **kwargs)

    yield tempdir

    # Remove the temporary folder (when out of context)
    retcode, out, _ = run_script(session,
                                 'rm -r {}'.format(tempdir),
                                 timeout=timeout)

    if retcode != 0:
        raise TempDirException("Unable to remove temporary files")


# pylint: disable=too-many-arguments
def upload_file(session, srcpath, destpath, timeout=60*30, **kwargs):
    """
    Upload a file to ssh server

    :param session: SSH session returned by get_ssh_session
    :type session: paramiko.client.SSHClient
    :param srcpath: Full path to the file on the host server to transfer to
                    the ssh server
    :type srcpath: str
    :param destpath: Full path on the ssh server to transfer the file into
    :type destpath: str
    :param timeout: Timeout (in seconds) to use in connecting to the ssh server
    :type timeout: float
    :param kwargs: Additional arguments to pass to SCP
    """

    # Upload the file
    with scp.SCPClient(session.get_transport(), socket_timeout=timeout) as scps:
        scps.put(srcpath, destpath, **kwargs)


def run_script(session, script_command, timeout=30*60):
    """
    Execute scripts in the ssh server

    :param session: SSH session returned by get_ssh_session
    :type session: paramiko.client.SSHClient
    :param script_command: Command to run in the ssh server
    :type script_command: str
    :param timeout: Timeout (in seconds) to use in connecting to the ssh server
    :type timeout: float
    """

    # Execute the command
    stdin, stdout, stderr = session.exec_command(script_command,
                                                 timeout=timeout)

    # Get the exit code
    retcode = stdout.channel.recv_exit_status()
    stdout = [k for k in stdout.readlines()]
    stderr = [k for k in stderr.readlines()]

    # Close the stdin
    stdin.close()

    return retcode, stdout, stderr
