import os
import logging
from subprocess import Popen

pidfile = os.path.join(os.sep, 'var', 'run', 'insights-client.pid')
logger = logging.getLogger(__name__)


def read_pidfile():
    '''
    Read the pidfile we wrote at launch
    '''
    pid = None
    try:
        with open(pidfile) as file_:
            pid = file_.read()
    except IOError:
        logger.debug('Could not open pidfile for reading.')
    return pid


def systemd_notify(pid):
    '''
    Ping the systemd watchdog with the main PID so that
    the watchdog doesn't kill the process
    '''
    if not os.getenv('NOTIFY_SOCKET'):
        # running standalone, not via systemd job
        return
    if not pid:
        logger.debug('No PID specified.')
        return
    if not os.path.exists('/usr/bin/systemd-notify'):
        # RHEL 6, no systemd
        return
    try:
        proc = Popen(['/usr/bin/systemd-notify', '--pid=' + str(pid), 'WATCHDOG=1'])
    except OSError:
        logger.debug('Could not launch systemd-notify.')
        return
    stdout, stderr = proc.communicate()
    if proc.returncode != 0:
        logger.debug('systemd-notify returned %s', proc.returncode)
