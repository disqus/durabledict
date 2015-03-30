import os
import sys
import glob


def get_zookeeper_paths():
    zk_path = None

    if sys.platform == 'darwin':
        homebrew_path = glob.glob('/usr/local/Cellar/zookeeper/*/libexec/')
        if homebrew_path:
            zk_path = homebrew_path[0]
    elif sys.platform == 'linux2':
        linux_default = '/usr/share/java/'
        if os.path.exists(linux_default):
            zk_path = linux_default

    if zk_path:
        return zk_path

    raise Exception('ZOOKEEPER_PATH must be in environment for tests to run')


if 'ZOOKEEPER_PATH' not in os.environ:
    os.environ['ZOOKEEPER_PATH'] = get_zookeeper_paths()
